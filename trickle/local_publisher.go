package trickle

import (
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/livepeer/node/internal/observability"
)

// local (in-memory) publisher for trickle protocol

type TrickleLocalPublisher struct {
	channelName string
	mimeType    string
	server      *Server

	mu  *sync.Mutex
	seq int
}

func NewLocalPublisher(sm *Server, channelName string, mimeType string) *TrickleLocalPublisher {
	return &TrickleLocalPublisher{
		channelName: channelName,
		server:      sm,
		mu:          &sync.Mutex{},
		mimeType:    mimeType,
	}
}

func (c *TrickleLocalPublisher) CreateChannel() {
	c.server.getOrCreateStream(c.channelName, c.mimeType, true)
}

func (c *TrickleLocalPublisher) Write(data io.Reader) error {
	stream := c.server.getOrCreateStream(c.channelName, c.mimeType, false)
	if stream == nil {
		return StreamNotFoundErr
	}
	c.mu.Lock()
	seq := c.seq
	segment, exists := stream.getForWrite(seq)
	if exists {
		c.mu.Unlock()
		return errors.New("entry already exists for this sequence")
	}

	// before we begin - let's pre-create the next segment
	nextSeq := c.seq + 1
	if _, exists = stream.getForWrite(nextSeq); exists {
		c.mu.Unlock()
		return errors.New("next entry already exists in this sequence")
	}
	c.seq = nextSeq
	c.mu.Unlock()

	// now continue with the show
	buf := make([]byte, 1024*32) // 32kb to begin with
	totalRead := 0
	for {
		n, err := data.Read(buf)
		if n > 0 {
			if totalRead == 0 {
				stream.mutex.Lock()
				stream.nextWrite = seq + 1
				stream.writeTime = time.Now()
				stream.mutex.Unlock()
			}
			if err := segment.writeData(buf[:n]); err != nil {
				segment.close()
				return err
			}
			totalRead += n
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			slog.With("component", "trickle", "event", "stream", "operation", "stream").Debug("Error reading published data", "channel", c.channelName, "seq", seq, "bytes written", totalRead, "outcome", "error", "error_kind", observability.ErrorKind(err))
		}
	}
	segment.close()
	return nil
}

func (c *TrickleLocalPublisher) Close() error {
	return c.server.closeStream(c.channelName)
}
