// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

import (
	"fmt"
)

// Guarded is a sink wrapper that enforces the three limits a source owns
// (max_depth, max_key_bytes, max_scalar_bytes) on the events as they are
// produced, polls the abort flag per event so a walk stops as promptly
// as a parse does, and counts the source metrics (events, keys,
// scalars) locally, flushing them in one step at End, so the hot path
// carries no atomic per event. Every source emits through one.
type Guarded struct {
	inner          Sink
	maxDepth       int
	maxKeyBytes    int
	maxScalarBytes int
	abort          *AbortFlag
	metrics        *Metrics
	depth          int
	events         uint64
	keys           uint64
	scalars        uint64
}

// NewGuarded wraps inner under limits; a nil abort or metrics gets a
// fresh one.
func NewGuarded(inner Sink, limits Limits, abort *AbortFlag, metrics *Metrics) *Guarded {
	if abort == nil {
		abort = NewAbortFlag()
	}
	if metrics == nil {
		metrics = NewMetrics()
	}
	return &Guarded{
		inner:          inner,
		maxDepth:       limits.MaxDepth,
		maxKeyBytes:    limits.MaxKeyBytes,
		maxScalarBytes: limits.MaxScalarBytes,
		abort:          abort,
		metrics:        metrics,
	}
}

// Flush adds the counts so far to the shared metrics and starts again.
func (g *Guarded) Flush() {
	g.metrics.Events.Add(g.events)
	g.metrics.Keys.Add(g.keys)
	g.metrics.Scalars.Add(g.scalars)
	g.events, g.keys, g.scalars = 0, 0, 0
}

// Depth is the number of open containers right now.
func (g *Guarded) Depth() int { return g.depth }

// Inner is the wrapped sink.
func (g *Guarded) Inner() Sink { return g.inner }

func (g *Guarded) scalar(bytes int) *Fail {
	g.scalars++
	if bytes > g.maxScalarBytes {
		return LimitFail("max_scalar_bytes", uint64(g.maxScalarBytes),
			fmt.Sprintf("a scalar of %d bytes is larger than %d", bytes, g.maxScalarBytes))
	}
	return nil
}

// Event checks ev, counts it, and passes it on.
func (g *Guarded) Event(ev Event) (Flow, *Fail) {
	if g.abort.IsAborted() {
		return Continue, AbortedFail()
	}
	g.events++
	var f *Fail
	switch ev.Kind {
	case ObjectStart, ArrayStart:
		g.depth++
		if g.depth > g.maxDepth {
			f = LimitFail("max_depth", uint64(g.maxDepth),
				fmt.Sprintf("a container is nested deeper than %d", g.maxDepth))
		}
	case ObjectEnd, ArrayEnd:
		if g.depth > 0 {
			g.depth--
		}
	case Key:
		g.keys++
		if len(ev.Text) > g.maxKeyBytes {
			f = LimitFail("max_key_bytes", uint64(g.maxKeyBytes),
				fmt.Sprintf("a key of %d bytes is longer than %d", len(ev.Text), g.maxKeyBytes))
		}
	case String:
		f = g.scalar(len(ev.Text))
	case Number:
		f = g.scalar(len(ev.Lexeme))
	case Null, Bool:
		f = g.scalar(0)
	case End:
		g.Flush()
	}
	if f != nil {
		return Continue, f
	}
	return g.inner.Event(ev)
}
