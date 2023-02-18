package dispatcher

import (
	"context"
	"fmt"
)

type Usecase struct{}

func NewUsecase() *Usecase {
	return &Usecase{}
}

func (u *Usecase) HandleMessage(ctx context.Context, body []byte) error {
	fmt.Printf("Got message: %q\n", body)
	return nil
}
