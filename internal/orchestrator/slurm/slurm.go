// Package slurm is a PLACEHOLDER for the SLURM implementation of
// orchestrator.Orchestrator.
//
// Nothing here works yet. It exists for two reasons:
//
//  1. The compile-time assertion at the bottom proves the Orchestrator
//     interface is actually satisfiable by a non-Kubernetes backend. If a
//     future change to the interface leaks Kubernetes concepts into it, THIS
//     FILE STOPS COMPILING — which is exactly the early warning we want.
//
//  2. It records how each method would work on SLURM, so the design decisions
//     survive until someone builds it.
//
// Why SLURM matters: most large-scale training runs on SLURM, not Kubernetes.
// Jobs run under a wall-clock limit, so multi-day training is necessarily a
// chain of requeued jobs, each loading the last checkpoint. Recovery there is
// the normal operating mode, not an edge case.
//
// The pitch is different, though. SLURM teams exercise their resume path daily
// via time-limit requeue, so those bugs surface fast. What stays untested is the
// ABRUPT case: a time-limit stop is graceful (SIGTERM first, the script can
// save), while a rank dying mid-step is not. Different code paths. The drill
// targets the second one.
package slurm

import (
	"context"
	"errors"

	"github.com/applegreengrape/recoverygate/internal/orchestrator"
)

// ErrNotImplemented is returned by every method until this adapter is built.
var ErrNotImplemented = errors.New("slurm adapter: not implemented yet")

// Orchestrator will drive a training job on a SLURM cluster.
//
// Unlike Kubernetes, there is no API server to talk to — everything is shelling
// out to the SLURM CLI (squeue/scancel/sacct/srun) on a login node, or over SSH.
type Orchestrator struct {
	// JobID is the SLURM job to drill (e.g. "1234567").
	JobID string
	// LoginHost is the host to run SLURM commands on. Empty = local.
	LoginHost string
}

// New returns an unimplemented SLURM orchestrator.
func New(jobID, loginHost string) *Orchestrator {
	return &Orchestrator{JobID: jobID, LoginHost: loginHost}
}

// Discover would map SLURM tasks to workers.
//
// Plan: `scontrol show job <id>` or `squeue -j <id> -o "%N"` for the node list,
// then map rank↔node. Ranks come from SLURM_PROCID inside the job, so the
// reporter should include it — on SLURM there is no equivalent of the Kubeflow
// replica-index label to read from the outside.
func (o *Orchestrator) Discover(ctx context.Context, sel orchestrator.Selector) ([]orchestrator.Worker, error) {
	return nil, ErrNotImplemented
}

// Kill would terminate exactly one rank.
//
// This is the hard part, and the main reason the SLURM adapter is more duct tape
// than the Kubernetes one. There is no clean "kill rank 3" API:
//   - `scancel <jobid>` kills the WHOLE job — too blunt, that's not worker loss.
//   - `scancel --signal=KILL <jobid>.<stepid>` targets a job step, not a task.
//   - Realistically: SSH to the node holding that rank and kill the PID.
//
// Whatever we choose must still look like an abrupt loss (SIGKILL), not a
// graceful shutdown — a graceful stop lets the script checkpoint on the way out,
// which is precisely the code path we are NOT trying to test.
func (o *Orchestrator) Kill(ctx context.Context, w orchestrator.Worker) error {
	return ErrNotImplemented
}

// WatchGroup would detect the requeue and the group coming back.
//
// No watch stream exists, so this is polling: `squeue -j <id>` for state
// transitions (RUNNING -> PENDING/REQUEUED -> RUNNING), or `sacct -j <id>` for
// completed steps. Emit WorkerDied on the state leaving RUNNING, GroupReady when
// it returns. Poll interval wants to be well under the recovery SLO.
func (o *Orchestrator) WatchGroup(ctx context.Context, sel orchestrator.Selector) (<-chan orchestrator.GroupEvent, error) {
	return nil, ErrNotImplemented
}

// Cleanup is expected to stay a no-op, as on Kubernetes: the drill mutates
// nothing it needs to unwind.
func (o *Orchestrator) Cleanup(ctx context.Context, sel orchestrator.Selector) error {
	return nil
}

// Compile-time proof that the interface is backend-neutral.
//
// This line is the entire point of the placeholder. Keep it.
var _ orchestrator.Orchestrator = (*Orchestrator)(nil)

// Also still to build for SLURM, in a sibling package under internal/events:
//
//	events/slurmlog — reads the job's stdout file (slurm-<jobid>.out) from the
//	shared parallel filesystem (Lustre/GPFS) instead of streaming pod logs. Same
//	RECOVERYGATE/v1 sentinel, same parser; only the transport differs. Tail the
//	file rather than streaming an HTTP response.
//
// And the failure to look for is the SLURM-flavoured version of the same bug:
// checkpoints written to node-local scratch (/tmp, or $SLURM_TMPDIR which is
// purged at job end) instead of the shared filesystem. On requeue the job lands
// on different nodes, the checkpoint is gone, and training restarts from zero.
