package serve

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"
)

const defaultLogBufferSize = 500

type LogEvent struct {
	ID        int64  `json:"id"`
	Timestamp string `json:"timestamp"`
	Message   string `json:"message"`
}

type LogBroker struct {
	mu        sync.Mutex
	maxEvents int
	nextID    int64
	events    []LogEvent
	subs      map[chan LogEvent]struct{}
	partial   bytes.Buffer
}

func NewLogBroker(maxEvents int) *LogBroker {
	if maxEvents <= 0 {
		maxEvents = defaultLogBufferSize
	}
	return &LogBroker{
		maxEvents: maxEvents,
		subs:      make(map[chan LogEvent]struct{}),
	}
}

func (b *LogBroker) Writer() io.Writer {
	return brokerWriter{broker: b}
}

func (b *LogBroker) Recent(limit int) []LogEvent {
	b.mu.Lock()
	defer b.mu.Unlock()

	if limit <= 0 || limit > len(b.events) {
		limit = len(b.events)
	}
	start := len(b.events) - limit
	result := make([]LogEvent, limit)
	copy(result, b.events[start:])
	return result
}

func (b *LogBroker) Subscribe() (<-chan LogEvent, func()) {
	ch := make(chan LogEvent, 64)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()

	return ch, func() {
		b.mu.Lock()
		if _, ok := b.subs[ch]; ok {
			delete(b.subs, ch)
			close(ch)
		}
		b.mu.Unlock()
	}
}

func (b *LogBroker) publishLine(line string) {
	line = trimLogLine(line)
	if line == "" {
		return
	}

	event := LogEvent{
		ID:        b.nextEventID(),
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Message:   line,
	}

	b.mu.Lock()
	b.events = append(b.events, event)
	if len(b.events) > b.maxEvents {
		b.events = append([]LogEvent(nil), b.events[len(b.events)-b.maxEvents:]...)
	}

	subs := make([]chan LogEvent, 0, len(b.subs))
	for ch := range b.subs {
		subs = append(subs, ch)
	}
	b.mu.Unlock()

	for _, ch := range subs {
		select {
		case ch <- event:
		default:
		}
	}
}

func (b *LogBroker) nextEventID() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextID++
	return b.nextID
}

func (b *LogBroker) write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	total := len(p)
	for len(p) > 0 {
		idx := bytes.IndexByte(p, '\n')
		if idx < 0 {
			_, _ = b.partial.Write(p)
			break
		}

		_, _ = b.partial.Write(p[:idx])
		line := b.partial.String()
		b.partial.Reset()
		go b.publishLine(line)
		p = p[idx+1:]
	}
	return total, nil
}

func trimLogLine(line string) string {
	return string(bytes.TrimSpace([]byte(line)))
}

type brokerWriter struct {
	broker *LogBroker
}

func (w brokerWriter) Write(p []byte) (int, error) {
	if w.broker == nil {
		return len(p), nil
	}
	return w.broker.write(p)
}

func encodeSSEEvent(event LogEvent) ([]byte, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	return []byte(fmt.Sprintf("data: %s\n\n", payload)), nil
}
