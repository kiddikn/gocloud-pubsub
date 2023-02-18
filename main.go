package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/kiddikn/gocloud-pubsub/pubsub/subscriber"
	"github.com/kiddikn/gocloud-pubsub/pubsub/subscriber/dispatcher"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const maxHandlers = 10
	disp := dispatcher.NewUsecase()
	sub, err := subscriber.NewSubscriber(ctx, "my-project", "my-subscription", maxHandlers, disp)
	if err != nil {
		return err
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-signalChan
		cancel()
	}()

	return sub.Run(ctx)
}
