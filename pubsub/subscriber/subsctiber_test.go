package subscriber_test

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/golang/mock/gomock"
	"github.com/kiddikn/gocloud-pubsub/pubsub/subscriber"
	"github.com/kiddikn/gocloud-pubsub/pubsub/subscriber/mock_subscriber"
	"github.com/stretchr/testify/assert"
	_ "gocloud.dev/pubsub/gcppubsub"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
)

const (
	emulatorProjectID      = "project-testid"
	emulatorTopicID        = "topic-testid"
	emulatorSubscriptionID = "subscription-testid"
)

func Test_Subscriber(t *testing.T) {
	type args struct {
		projectID       string
		subscriptionID  string
		publishMessages []string
	}
	tests := []struct {
		description string
		args        args
		setup       func(*mock_subscriber.MockDispatcher)
	}{
		{
			description: "Subscribeに成功",
			args: args{
				projectID:      emulatorProjectID,
				subscriptionID: emulatorSubscriptionID,
				publishMessages: []string{
					"test1",
					"test2",
				},
			},
			setup: func(d *mock_subscriber.MockDispatcher) {
				d.EXPECT().HandleMessage(gomock.Any(), []byte("test1")).Return(nil)
				d.EXPECT().HandleMessage(gomock.Any(), []byte("test2")).Return(nil)
			},
		},
		{
			description: "project idの初期設定が不正な場合はメッセージを受け取らず終了",
			args: args{
				projectID:      "not-fosssund-project-test",
				subscriptionID: emulatorSubscriptionID,
			},
			setup: func(d *mock_subscriber.MockDispatcher) {},
		},
		{
			description: "subscription idの初期設定が不正な場合はメッセージを受け取らず終了",
			args: args{
				projectID:      emulatorProjectID,
				subscriptionID: "not-fosssund-subscription-test",
			},
			setup: func(d *mock_subscriber.MockDispatcher) {},
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			ctx := context.Background()
			srv := pstest.NewServer()
			defer srv.Close()
			t.Setenv("PUBSUB_EMULATOR_HOST", srv.Addr)

			conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
			if err != nil {
				t.Fatal()
			}
			defer conn.Close()

			client, err := pubsub.NewClient(ctx, emulatorProjectID, option.WithGRPCConn(conn))
			if err != nil {
				t.Fatal()
			}
			defer client.Close()

			topic, err := client.CreateTopic(ctx, emulatorTopicID)
			if err != nil {
				t.Fatal()
			}
			defer topic.Delete(context.Background())

			sub, err := client.CreateSubscription(ctx, emulatorSubscriptionID, pubsub.SubscriptionConfig{Topic: topic})
			if err != nil {
				t.Fatal()
			}
			defer sub.Delete(context.Background())

			ctrl := gomock.NewController(t)
			mockDispacher := mock_subscriber.NewMockDispatcher(ctrl)
			tt.setup(mockDispacher)

			cctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			subscriber, err := subscriber.NewSubscriber(cctx, tt.args.projectID, tt.args.subscriptionID, 2, mockDispacher)
			assert.NoError(t, err)

			go func() {
				for _, m := range tt.args.publishMessages {
					topic.Publish(context.Background(), &pubsub.Message{Data: []byte(m)})
				}
				topic.Flush()
				// Flushで全てのメッセージをpublishした後にsubscriberを終了させる
				time.Sleep(1 * time.Second)
				cancel()
			}()

			err = subscriber.Run(cctx)
			assert.NoError(t, err)
		})
	}
}
