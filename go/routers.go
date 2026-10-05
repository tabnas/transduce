// Copyright (c) 2026 tabnas, MIT License

package tabnastransduce

import (
	"github.com/tabnas/alchemy/go/shared"
)

// Routers is this package's implementation of alchemy's shared.Routers:
// the stages alchemy's runtime builds on the transducer side, each made by
// this package's constructor of the same name. A host hands it to
// alchemy's Compile, with render's Renderers.
func Routers() shared.Routers { return routers{} }

type routers struct{}

// Router is NewRouter.
func (routers) Router(specs []CaptureSpec, limits Limits, duplicates Duplicates, metrics *Metrics, downstream RouteSink) (Sink, *Fail) {
	router, f := NewRouter(specs, limits, duplicates, metrics, downstream)
	if f != nil {
		return nil, f
	}
	return router, nil
}

// TableFromJSON is NewTableFromJSON.
func (routers) TableFromJSON(binding TableBinding, limits Limits, duplicates Duplicates, metrics *Metrics, sink TableSink) (Sink, *Fail) {
	table, f := NewTableFromJSON(binding, limits, duplicates, metrics, sink)
	if f != nil {
		return nil, f
	}
	return table, nil
}

// ScanEmit is NewScanEmit, over values of any type.
func (routers) ScanEmit(initial any, step func(any, any) (Transition[any, any], *Fail), finish func(any) ([]any, *Fail), out func(any) (Flow, *Fail)) shared.ScanEmitter {
	return NewScanEmit[any, any, any](initial, step, finish, out)
}

// Guarded is NewGuarded.
func (routers) Guarded(inner Sink, limits Limits, abort *AbortFlag, metrics *Metrics) Sink {
	return NewGuarded(inner, limits, abort, metrics)
}
