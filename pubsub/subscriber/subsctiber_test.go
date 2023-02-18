package subscriber_test

import (
	"context"
	"testing"

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
	emulatorProjectID      = "project-test"
	emulatorTopicID        = "topic-test"
	emulatorSubscriptionID = "subscription-test"
)

func setupEmulator(ctx context.Context) (string, *pubsub.Topic, error) {
	srv := pstest.NewServer()
	conn, err := grpc.Dial(srv.Addr, grpc.WithInsecure())
	if err != nil {
		return "", nil, err
	}

	client, err := pubsub.NewClient(ctx, emulatorProjectID, option.WithGRPCConn(conn))
	if err != nil {
		return "", nil, err
	}
	topic, err := client.CreateTopic(ctx, emulatorTopicID)
	if err != nil {
		return "", nil, err
	}
	if _, err := client.CreateSubscription(ctx, emulatorSubscriptionID, pubsub.SubscriptionConfig{Topic: topic}); err != nil {
		return "", nil, err
	}

	return srv.Addr, topic, nil
}

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
		wantErr     bool
	}{
		{
			description: "Subscribeに成功",
			args: args{
				projectID:      "project-test",
				subscriptionID: "subscription-test",
				publishMessages: []string{
					"test1",
					"test2",
				},
			},
			setup: func(d *mock_subscriber.MockDispatcher) {
				d.EXPECT().HandleMessage(gomock.Any(), "test1").Return(nil)
				d.EXPECT().HandleMessage(gomock.Any(), "test2").Return(nil)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			addr, topic, err := setupEmulator(ctx)
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("PUBSUB_EMULATOR_HOST", addr)

			ctrl := gomock.NewController(t)
			mockDispacher := mock_subscriber.NewMockDispatcher(ctrl)
			tt.setup(mockDispacher)

			sub, err := subscriber.NewSubscriber(ctx, tt.args.projectID, tt.args.subscriptionID, 2, mockDispacher)
			assert.NoError(t, err)

			go func() {
				// テスト対象のtをchannelで渡し終えた後に閉じることでRunのループを正常終了させる
				defer cancel()
				for _, m := range tt.args.publishMessages {
					topic.Publish(ctx, &pubsub.Message{Data: []byte(m)})
				}
			}()

			err = sub.Run(ctx)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

		})
	}
}
