// Package orchestrator is the ONLY thing the engine knows about the cluster.
// One implementation per backend (kube first, slurm later). The engine must
// never import client-go or shell out to squeue — all of that lives behind
// this interface. Litmus test: if a SLURM adapter can implement these four
// methods, the boundary is right.
package orchestrator

import (
	"context"
	"time"
)

// Selector identifies which workload's workers we're targeting.
type Selector struct {
	// Labels is a label selector for k8s ("training-run=llama-finetune"),
	// or a job name/pattern for SLURM.
	Labels string
}

// Worker is one rank of a distributed training job.
type Worker struct {
	Rank  int
	ID    string // pod name (k8s) | nodelist+taskid (slurm)
	Node  string
	Alive bool
}

// GroupEventType describes a worker-group lifecycle transition.
type GroupEventType int

const (
	// WorkerDied means one worker terminated abnormally.
	WorkerDied GroupEventType = iota
	// GroupRestarting means the orchestrator tore the group down to relaunch it.
	GroupRestarting
	// GroupReady means all expected workers are back up and running.
	GroupReady
)

func (t GroupEventType) String() string {
	switch t {
	case WorkerDied:
		return "WorkerDied"
	case GroupRestarting:
		return "GroupRestarting"
	case GroupReady:
		return "GroupReady"
	default:
		return "Unknown"
	}
}

// GroupEvent is emitted by WatchGroup as the worker group changes state.
type GroupEvent struct {
	Type  GroupEventType
	Time  time.Time
	Ranks int // number of workers up, meaningful on GroupReady
}

// Orchestrator is the cluster-facing contract.
type Orchestrator interface {
	// Discover returns the current worker set for a workload.
	Discover(ctx context.Context, sel Selector) ([]Worker, error)

	// Kill terminates exactly one worker. Implementations MUST NOT kill more
	// than the single worker they are handed.
	Kill(ctx context.Context, w Worker) error

	// WatchGroup streams lifecycle events until ctx is cancelled.
	WatchGroup(ctx context.Context, sel Selector) (<-chan GroupEvent, error)

	// Cleanup removes anything the drill created or changed.
	Cleanup(ctx context.Context, sel Selector) error
}
