package darner

import (
	"errors"
	"fmt"
	"github.com/apuckey/darner-queue-go/memcache"
	"github.com/apuckey/scribe-logger-go"
	"sync"
	"time"
)

type ClusterReader struct {
	GetTimeout   time.Duration
	AbortTimeout time.Duration
	Backoff      *Backoff

	clients []*Client

	closed chan struct{}
	wg     sync.WaitGroup
}

func NewClusterReader(clients []*Client) *ClusterReader {
	return &ClusterReader{
		clients: clients,

		// defaults
		GetTimeout:   5 * time.Second,
		AbortTimeout: 1 * time.Minute,
		Backoff: &Backoff{
			//These are the defaults
			Min:    100 * time.Millisecond,
			Max:    5 * time.Minute,
			Factor: 2,
		},
	}
}

func (r *ClusterReader) ReadIntoChannel(queueName string, ch chan<- *QueueItem) {
	r.closed = make(chan struct{})

	for _, client := range r.clients {
		r.wg.Add(1)

		go func(client *Client, queueName string, ch chan<- *QueueItem, closed chan struct{}) {
			defer r.wg.Done()

			hasFailed := false

			for {
				item, err := client.Get(queueName, 1, r.AbortTimeout)
				if err != nil && !errors.Is(err, memcache.ErrCacheMiss) {
					// probably decide what to do here. lets just wait for timeout before trying to get new messages for now.
					// most likely a transient issue ie: restarting darner
					hasFailed = true
					logger.Error(fmt.Sprintf("[DarnerQueue]: Error getting message from queue: %s", err.Error()))
					<-time.After(r.Backoff.Duration())
				} else {
					// normal operation. reset backoff timer
					r.Backoff.Reset()
					if hasFailed {
						logger.Info(fmt.Sprintf("[DarnerQueue]: resuming normal operation"))
						hasFailed = false
					}
					if item != nil {
						ch <- item
					}
				}

				select {
				case <-closed:
					return
				default:
					continue
				}
			}
		}(client, queueName, ch, r.closed)
	}
	r.wg.Wait()
}

func (r *ClusterReader) Close() error {
	if r.closed != nil {
		close(r.closed)
	}

	return nil
}
