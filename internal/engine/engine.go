package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/applegreengrape/recoverygate/internal/events"
	"github.com/applegreengrape/recoverygate/internal/orchestrator"
)

// ErrChannelClosed means a source stopped before the drill could finish.
var ErrChannelClosed = errors.New("event stream closed before the drill completed")

// Engine runs one recovery drill. It knows nothing about Kubernetes or SLURM —
// only the Orchestrator and Source interfaces.
type Engine struct {
	orch  orchestrator.Orchestrator
	src   events.Source
	cfg   Config
	now   func() time.Time
	phase Phase
}

// Option configures an Engine.
type Option func(*Engine)

// WithClock injects a clock. Tests use this to make SLO breaches deterministic.
// Production uses time.Now.
func WithClock(now func() time.Time) Option {
	return func(e *Engine) { e.now = now }
}

// New builds an Engine.
func New(orch orchestrator.Orchestrator, src events.Source, cfg Config, opts ...Option) *Engine {
	e := &Engine{
		orch:  orch,
		src:   src,
		cfg:   cfg,
		now:   time.Now,
		phase: PhasePending,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// Phase reports the current state. Useful for progress output and debugging.
func (e *Engine) Phase() Phase { return e.phase }

func (e *Engine) Run(ctx context.Context) (*Result, error) {
	res := &Result{}

	// 1. Subscribe BEFORE acting, or we race the restart and miss the first
	//    post-restart event — the one that decides the verdict.
	evCh, err := e.src.Events(ctx)
	if err != nil {
		return res, fmt.Errorf("subscribe to training events: %w", err)
	}
	grpCh, err := e.orch.WatchGroup(ctx, e.cfg.Selector)
	if err != nil {
		return res, fmt.Errorf("watch worker group: %w", err)
	}

	// 2. Cleanup on every exit path, including cancellation.
	defer func() {
		_ = e.orch.Cleanup(context.WithoutCancel(ctx), e.cfg.Selector)
	}()

	// 3. WaitingForCheckpoint — block until a known good state exists.
	e.phase = PhaseWaitingForCheckpoint
	for {
		ev, err := recv(ctx, evCh)
		if err != nil {
			return res, fmt.Errorf("waiting for checkpoint: %w", err)
		}
		if ev.GlobalStep > res.BaselineStep {
			res.BaselineStep = ev.GlobalStep
		}
		if ev.Kind == events.KindCheckpoint && ev.GlobalStep >= e.cfg.AfterCheckpoint {
			res.CheckpointStep = ev.GlobalStep
			break
		}
	}

	// 4. RecordingBaseline — snapshot the workers, pick exactly one victim.
	e.phase = PhaseRecordingBaseline
	workers, err := e.orch.Discover(ctx, e.cfg.Selector)
	if err != nil {
		return res, fmt.Errorf("discover workers: %w", err)
	}
	if len(workers) == 0 {
		return res, errors.New("no workers matched the selector")
	}
	victim := workers[len(workers)-1]
	res.KilledRank = victim.Rank

	// 5. InjectingFailure — one worker, and the clock starts on OUR side.
	e.phase = PhaseInjectingFailure
	killedAt := e.now()
	if err := e.orch.Kill(ctx, victim); err != nil {
		return res, fmt.Errorf("kill worker rank %d: %w", victim.Rank, err)
	}

	// 6. WaitingForRestart — the orchestrator tears the group down and back up.
	e.phase = PhaseWaitingForRestart
	for {
		ge, err := recv(ctx, grpCh)
		if err != nil {
			return res, fmt.Errorf("waiting for worker group to restart: %w", err)
		}
		if ge.Type == orchestrator.GroupReady {
			res.RanksRejoined = ge.Ranks
			break
		}
	}

	// 7. VerifyingProgress — the first `start` after the restart carries the
	//    step training actually resumed FROM. Pod readiness is not enough: a pod
	//    can be Ready while the process is hung and never re-enters the loop.
	e.phase = PhaseVerifyingProgress
	for {
		ev, err := recv(ctx, evCh)
		if err != nil {
			return res, fmt.Errorf("waiting for training to resume: %w", err)
		}
		if ev.Kind == events.KindStart {
			res.ResumedFromStep = ev.GlobalStep
			break
		}
	}
	// Measured on our own clock only — never across nodes.
	res.RecoveryTime = e.now().Sub(killedAt)
	res.StepsLost = res.BaselineStep - res.ResumedFromStep

	// 8. Verdict. Most specific failure first — the Reason is the product.
	switch {
	case res.ResumedFromStep < res.CheckpointStep:
		e.fail(res, fmt.Sprintf(
			"resumed from step %d instead of checkpoint step %d — the checkpoint was "+
				"unavailable to the restarted worker (written to node-local disk?)",
			res.ResumedFromStep, res.CheckpointStep))
	case res.RanksRejoined != e.cfg.ExpectedRanks:
		e.fail(res, fmt.Sprintf(
			"only %d of %d ranks rejoined the worker group",
			res.RanksRejoined, e.cfg.ExpectedRanks))
	case res.RecoveryTime > e.cfg.MaxRecoveryTime:
		e.fail(res, fmt.Sprintf(
			"recovery took %s, exceeding the %s SLO",
			res.RecoveryTime.Round(time.Second), e.cfg.MaxRecoveryTime))
	case res.StepsLost > e.cfg.MaxLostSteps:
		e.fail(res, fmt.Sprintf(
			"lost %d steps, more than the %d permitted",
			res.StepsLost, e.cfg.MaxLostSteps))
	default:
		res.Verdict = VerdictPassed
		res.Reason = fmt.Sprintf(
			"resumed from checkpoint step %d in %s, %d steps lost, %d/%d ranks rejoined",
			res.ResumedFromStep, res.RecoveryTime.Round(time.Millisecond),
			res.StepsLost, res.RanksRejoined, e.cfg.ExpectedRanks)
		e.phase = PhasePassed
	}
	return res, nil
}

func (e *Engine) fail(res *Result, reason string) {
	res.Verdict = VerdictFailed
	res.Reason = reason
	e.phase = PhaseFailed
}

// recv reads one value, respecting cancellation. Generic so both the training
// event stream and the group event stream share it.
func recv[T any](ctx context.Context, ch <-chan T) (T, error) {
	var zero T
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case v, ok := <-ch:
		if !ok {
			return zero, ErrChannelClosed
		}
		return v, nil
	}
}
