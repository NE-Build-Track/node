package orchestrator

import (
	"bytes"
	"context"
	"net/http"
	"time"

	"github.com/livepeer/node/internal/observability"
	"github.com/livepeer/node/trickle"
)

const o2rKeepaliveMessage = `{"keep":"alive"}`

// channel tracks ownership; trickle.Server owns the stream and its segments.
type channel struct {
	name, mime, runnerID, sessionID string
	identity                        observability.Identity
	publisher                       *trickle.TrickleLocalPublisher
}

// newChannel requires s.mu.
func (s *Server) newChannel(id, name, mime, runnerID, sessionID string) *channel {
	publisher := trickle.NewLocalPublisher(s.trickleServer, id, mime)
	publisher.CreateChannel()
	ch := &channel{name: name, mime: mime, runnerID: runnerID, sessionID: sessionID, publisher: publisher}
	s.channels[id] = ch
	return ch
}

// closeChannel requires s.mu.
func (s *Server) closeChannel(id string) {
	ch := s.channels[id]
	if ch == nil {
		return
	}
	delete(s.channels, id)
	_ = ch.publisher.Close()
}

// A direct HTTP delete also removes the orchestrator's ownership record.
func (s *Server) beforeDeleteChannel(_ *http.Request, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ch := s.channels[id]; ch != nil {
		delete(s.channels, id)
		if s.o2r[ch.runnerID] == id {
			delete(s.o2r, ch.runnerID)
		}
	}
	return nil
}

func (s *Server) publishSessionEvent(id string, data []byte) {
	if ch := s.channels[id]; ch != nil {
		_ = ch.publisher.Write(bytes.NewReader(data))
	}
}

func (s *Server) runO2RKeepalives(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			for _, id := range s.o2r {
				s.publishSessionEvent(id, []byte(o2rKeepaliveMessage))
			}
			s.mu.Unlock()
		}
	}
}
