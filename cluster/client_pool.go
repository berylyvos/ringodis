package cluster

import (
	"context"
	"errors"
	pool "github.com/jolestar/go-commons-pool/v2"
	"ringodis/resp/client"
)

type connFactory struct {
	Peer string
}

func (cf *connFactory) MakeObject(ctx context.Context) (*pool.PooledObject, error) {
	c, err := client.MakeClient(cf.Peer)
	if err != nil {
		return nil, err
	}
	c.Start()
	return pool.NewPooledObject(c), nil
}

func (cf *connFactory) DestroyObject(ctx context.Context, object *pool.PooledObject) error {
	c, ok := object.Object.(*client.Client)
	if !ok {
		return errors.New("type mismatch")
	}
	c.Close()
	return nil
}

func (cf *connFactory) ValidateObject(ctx context.Context, object *pool.PooledObject) bool {
	return true
}

func (cf *connFactory) ActivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}

func (cf *connFactory) PassivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}
