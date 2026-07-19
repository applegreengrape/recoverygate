// Package events carries training progress out of the job and into the engine.
//
// Kubernetes cannot see training progress: a pod can be Running while training
// silently restarted from step 0. So the training process emits events (via the
// tiny Python reporter) and the engine consumes them through Source.
//
// v0.1 transport is pod stdout/logs streamed via pods/log. Shared-volume JSONL
// is the fallback and the natural SLURM implementation. The engine does not
// care which — it just wants an ordered stream.
package events

import (
	"context"
	"time"
)

// Kind is the type of training event the reporter emits.
type Kind string

const (
	// KindStart is emitted by every rank when it (re)enters the training loop.
	// GlobalStep carries the step it resumed FROM. This is the authoritative
	// signal for "where did we resume" — stronger than pod readiness, because a
	// pod can be Ready while the process is hung and never re-enters training.
	KindStart Kind = "start"

	// KindProgress is emitted as training advances.
	KindProgress Kind = "progress"

	// KindCheckpoint is emitted after a checkpoint is durably written.
	KindCheckpoint Kind = "checkpoint_completed"
)

// TrainingEvent is one line from the reporter.
type TrainingEvent struct {
	Kind       Kind
	GlobalStep int
	Rank       int
	// Time is the emitting process's clock. Use it for ordering hints and
	// buffering detection ONLY — never subtract two of these to measure a
	// duration, because ranks live on different nodes with clock skew.
	Time time.Time
}

// Source is an ordered stream of training events.
type Source interface {
	Events(ctx context.Context) (<-chan TrainingEvent, error)
}
