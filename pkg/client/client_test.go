package client

import (
	"context"
	"testing"

	"github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
)

func TestNewRedisClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	mockClient := mock.NewClient(ctrl)

	c := ClientArgs{
		Instance: mockClient,
	}

	if err := c.InitClient(ctx); err != nil {
		t.Fatalf("InitClient failed: %v", err)
	}
}
