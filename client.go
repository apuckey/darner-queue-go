package darner

import (
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"time"
)

type Client struct {
	Timeout time.Duration
	client  *memcache.Client
}

func NewClient(host string, port, timeout int) *Client {
	mc := memcache.New(fmt.Sprintf("%s:%d", host, port))
	mc.Timeout = time.Duration(timeout) * time.Second * 2

	return &Client{
		client: mc,
		Timeout: time.Duration(timeout) * time.Second,
	}
}

func (c *Client) Get(queueName string, maxItems int32, autoAbort time.Duration) (*QueueItem, error) {
	item, err := c.client.Get(fmt.Sprintf("%s/t=%d", queueName, int32(c.Timeout/time.Millisecond)))
	if item != nil {
		return &QueueItem{
			Message: string(item.Value),
		}, err
	}
	return nil, err
}

func (c *Client) Set(queueName, message string) error {
	item := &memcache.Item{
		Key: queueName,
		Value: []byte(message),
	}
	err := c.client.Set(item)
	return err
}
