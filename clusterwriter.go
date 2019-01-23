package darner

import (
	"fmt"
	"sync/atomic"
	"time"
)

type ClusterWriter struct {
	SetTimeout   time.Duration
	AbortTimeout time.Duration
	Backoff      *Backoff

	clients []*Client
	offset uint64

}

func NewClusterWriter(clients []*Client) *ClusterWriter {
	return &ClusterWriter{
		clients: clients,
	}
}

func (w *ClusterWriter) Write(queueName, item string) (err error) {
	c := w.getClient()
	if c != nil {
		err = c.Set(queueName, item)
	} else {
		return fmt.Errorf("[DarnerQueue]: Unable to get a client from the pool.")
	}
	return
}

func (w *ClusterWriter) getClient() *Client {
	n := len(w.clients)
	if n == 0 {
		return nil
	}
	i := int(atomic.AddUint64(&w.offset, 1) % uint64(n))
	return w.clients[i]
}