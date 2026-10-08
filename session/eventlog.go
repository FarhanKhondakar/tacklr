package session

import (
	"context"
	"sync"

	"github.com/ryanaldo34/tacklr"
)

type logEntry struct {
	seq   Seq
	topic string
	ev    tacklr.StreamEvent
}

type sessionLog struct {
	mu      sync.Mutex
	entries []logEntry
	subs    map[chan tacklr.StreamEvent]Seq
	closed  bool
	// kick is closed to end live tails. Subscribe owns each subscriber channel.
	kick chan struct{}
}

// MemoryEventLog is a channel EventLog with topics events/retry/close.
type MemoryEventLog struct {
	mu       sync.RWMutex
	sessions map[SessionID]*sessionLog
}

// NewMemoryEventLog returns an empty EventLog.
func NewMemoryEventLog() *MemoryEventLog {
	return &MemoryEventLog{sessions: make(map[SessionID]*sessionLog)}
}

func (l *MemoryEventLog) lookup(id SessionID) *sessionLog {
	l.mu.RLock()
	s := l.sessions[id]
	l.mu.RUnlock()
	return s
}

func (l *MemoryEventLog) session(id SessionID) *sessionLog {
	if s := l.lookup(id); s != nil {
		return s
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if s, ok := l.sessions[id]; ok {
		return s
	}
	s := &sessionLog{
		subs: make(map[chan tacklr.StreamEvent]Seq),
		kick: make(chan struct{}),
	}
	l.sessions[id] = s
	return s
}

// Append implements EventLog.
func (l *MemoryEventLog) Append(_ context.Context, sessionID SessionID, topic string, ev tacklr.StreamEvent) error {
	s := l.session(sessionID)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrSessionNotFound
	}
	seq := Seq(len(s.entries) + 1)
	s.entries = append(s.entries, logEntry{seq: seq, topic: topic, ev: ev})
	if topic == TopicRetry {
		return nil
	}
	for ch, after := range s.subs {
		if seq <= after {
			continue
		}
		select {
		case ch <- ev:
		default:
			// Drop if the subscriber is slow; the log still has the entry for replay.
		}
	}
	return nil
}

func visible(e logEntry) bool {
	return e.topic != TopicRetry
}

// seq is 1-based and equal to index in entries plus one. Caller holds s.mu.
func (s *sessionLog) visibleAfter(after Seq) ([]tacklr.StreamEvent, Seq) {
	start := int(after) //nolint:gosec // G115: EventLog seq is well below MaxInt
	if start > len(s.entries) {
		start = len(s.entries)
	}
	n := len(s.entries) - start
	if n == 0 {
		return nil, after
	}
	out := make([]tacklr.StreamEvent, 0, n)
	for _, e := range s.entries[start:] {
		if !visible(e) {
			continue
		}
		out = append(out, e.ev)
		after = e.seq
	}
	return out, after
}

func (s *sessionLog) unregister(ch chan tacklr.StreamEvent) {
	s.mu.Lock()
	delete(s.subs, ch)
	s.mu.Unlock()
}

// kickSubs ends live tails. Caller holds s.mu. Subscriber goroutines close their channels.
func (s *sessionLog) kickSubs() {
	close(s.kick)
	s.kick = make(chan struct{})
	clear(s.subs)
}

// Subscribe implements EventLog. It replays seq > after then tails.
// Replay is sent after unlocking so a large log cannot deadlock Append.
func (l *MemoryEventLog) Subscribe(ctx context.Context, sessionID SessionID, after Seq) (<-chan tacklr.StreamEvent, error) {
	s := l.session(sessionID)
	ch := make(chan tacklr.StreamEvent, 64)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		close(ch)
		return ch, nil
	}
	kick := s.kick
	replay, after := s.visibleAfter(after)
	s.mu.Unlock()

	go func() {
		defer close(ch)
		for _, ev := range replay {
			select {
			case ch <- ev:
			case <-ctx.Done():
				return
			case <-kick:
				return
			}
		}
		s.mu.Lock()
		if s.closed || s.kick != kick {
			s.mu.Unlock()
			return
		}
		extra, after := s.visibleAfter(after)
		s.subs[ch] = after
		s.mu.Unlock()
		for _, ev := range extra {
			select {
			case ch <- ev:
			case <-ctx.Done():
				s.unregister(ch)
				return
			case <-kick:
				s.unregister(ch)
				return
			}
		}
		select {
		case <-ctx.Done():
		case <-kick:
		}
		s.unregister(ch)
	}()
	return ch, nil
}

// Head implements EventLog. Unknown sessions report seq 0 without allocating a log.
func (l *MemoryEventLog) Head(_ context.Context, sessionID SessionID) (Seq, error) {
	s := l.lookup(sessionID)
	if s == nil {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return Seq(len(s.entries)), nil
}

// CloseSession implements EventLog. It drops the log so a later session
// can reuse the id and so closed history does not stay in process memory.
func (l *MemoryEventLog) CloseSession(_ context.Context, sessionID SessionID) error {
	l.mu.Lock()
	s, ok := l.sessions[sessionID]
	if ok {
		delete(l.sessions, sessionID)
	}
	l.mu.Unlock()
	if !ok {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.entries = nil
	s.kickSubs()
	return nil
}

var _ EventLog = (*MemoryEventLog)(nil)
