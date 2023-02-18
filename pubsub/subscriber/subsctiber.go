//go:generate mockgen -source $GOFILE -destination ./mock_subscriber/mock_subscriber.go
package subscriber

import (
	"context"
	"errors"
	"fmt"
	"log"

	"gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/gcppubsub"
)

type Dispatcher interface {
	HandleMessage(ctx context.Context, body []byte) error
}

type Subscriber struct {
	projectID, subscriptionID string
	maxHandlers               int
	dispatcher                Dispatcher
}

func NewSubscriber(ctx context.Context, projectID, subscriptionID string, maxHandlers int, dispatcher Dispatcher) (*Subscriber, error) {
	return &Subscriber{
		projectID:      projectID,
		subscriptionID: subscriptionID,
		maxHandlers:    maxHandlers,
		dispatcher:     dispatcher,
	}, nil
}

func (s *Subscriber) Run(ctx context.Context) error {
	subscription, err := pubsub.OpenSubscription(ctx,
		fmt.Sprintf("gcppubsub://projects/%s/subscriptions/%s", s.projectID, s.subscriptionID))
	if err != nil {
		return err
	}
	defer subscription.Shutdown(ctx)

	sem := make(chan struct{}, s.maxHandlers)
recvLoop:
	for {
		msg, err := subscription.Receive(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			// Errors from Receive indicate that Receive will no longer succeed.
			log.Printf("Receiving error: %v", err)
			break
		}
		log.Printf("Receiving message: %v", msg)

		// Wait if there are too many active handle goroutines and acquire the
		// semaphore. If the context is canceled, stop waiting and start shutting
		// down.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break recvLoop
		}

		// Handle the message in a new goroutine.
		go func() {
			defer func() { <-sem }() // Release the semaphore.

			if err := s.dispatcher.HandleMessage(ctx, msg.Body); err != nil {
				if msg.Nackable() {
					msg.Nack()
				}
				return
			}

			msg.Ack()
		}()
	}

	// We're no longer receiving messages. Wait to finish handling any
	// unacknowledged messages by totally acquiring the semaphore.
	for n := 0; n < s.maxHandlers; n++ {
		sem <- struct{}{}
	}
	return nil
}
