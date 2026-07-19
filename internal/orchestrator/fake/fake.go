// Package fake provides in-memory implementations of Orchestrator and
// events.Source so the engine can be tested with no cluster and no sleeps.
package fake

import (
	"context"
	"fmt"

	"github.com/applegreengrape/recoverygate/internal/events"
	"github.com/applegreengrape/recoverygate/internal/orchestrator"
)

// Orchestrator is an in-memory orchestrator.Orchestrator.
type Orchestrator struct {
	Workers   []orchestrator.Worker
	GroupCh   chan orchestrator.GroupEvent
	Killed    []orchestrator.Worker
	CleanedUp bool

	// OnKill runs synchronously inside Kill. Tests use it to script the
	// restart sequence (group events + post-restart training events).
	OnKill func()

	DiscoverErr error
	KillErr     error
}

// NewOrchestrator builds a fake with `ranks` healthy workers.
func NewOrchestrator(ranks int) *Orchestrator {
	ws := make([]orchestrator.Worker, 0, ranks)
	for i := 0; i < ranks; i++ {
		ws = append(ws, orchestrator.Worker{
			Rank:  i,
			ID:    fmt.Sprintf("worker-%d", i),
			Node:  fmt.Sprintf("node-%d", i),
			Alive: true,
		})
	}
	return &Orchestrator{
		Workers: ws,
		GroupCh: make(chan orchestrator.GroupEvent, 32),
	}
}

func (f *Orchestrator) Discover(ctx context.Context, sel orchestrator.Selector) ([]orchestrator.Worker, error) {
	if f.DiscoverErr != nil {
		return nil, f.DiscoverErr
	}
	out := make([]orchestrator.Worker, len(f.Workers))
	copy(out, f.Workers)
	return out, nil
}

func (f *Orchestrator) Kill(ctx context.Context, w orchestrator.Worker) error {
	if f.KillErr != nil {
		return f.KillErr
	}
	f.Killed = append(f.Killed, w)
	if f.OnKill != nil {
		f.OnKill()
	}
	return nil
}

func (f *Orchestrator) WatchGroup(ctx context.Context, sel orchestrator.Selector) (<-chan orchestrator.GroupEvent, error) {
	return f.GroupCh, nil
}

func (f *Orchestrator) Cleanup(ctx context.Context, sel orchestrator.Selector) error {
	f.CleanedUp = true
	return nil
}

// Source is an in-memory events.Source.
type Source struct {
	Ch chan events.TrainingEvent
}

// NewSource builds a buffered fake event source.
func NewSource() *Source {
	return &Source{Ch: make(chan events.TrainingEvent, 64)}
}

func (s *Source) Events(ctx context.Context) (<-chan events.TrainingEvent, error) {
	return s.Ch, nil
}

// Emit is a convenience for scripting events in tests.
func (s *Source) Emit(kind events.Kind, step, rank int) {
	s.Ch <- events.TrainingEvent{Kind: kind, GlobalStep: step, Rank: rank}
}
