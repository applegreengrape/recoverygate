package engine

import (
	"context"
	"testing"
	"time"

	"github.com/applegreengrape/recoverygate/internal/events"
	"github.com/applegreengrape/recoverygate/internal/orchestrator"
	"github.com/applegreengrape/recoverygate/internal/orchestrator/fake"
)

// baseCfg is a healthy drill definition: kill after checkpoint 1000, expect
// both ranks back within 5 minutes, tolerate 100 lost steps.
func baseCfg() Config {
	return Config{
		Selector:        orchestrator.Selector{Labels: "training-run=test"},
		AfterCheckpoint: 1000,
		ExpectedRanks:   2,
		MaxRecoveryTime: 5 * time.Minute,
		MaxLostSteps:    100,
	}
}

// preKill loads the events that happen before the fault is injected.
func preKill(src *fake.Source) {
	src.Emit(events.KindStart, 0, 0)
	src.Emit(events.KindProgress, 500, 0)
	src.Emit(events.KindCheckpoint, 1000, 0)
}

// restartWith returns an OnKill hook that scripts the restart: the group comes
// back with `ranks` workers, and each rank re-enters training at `resumeStep`.
func restartWith(orch *fake.Orchestrator, src *fake.Source, ranks, resumeStep int) func() {
	return func() {
		orch.GroupCh <- orchestrator.GroupEvent{Type: orchestrator.WorkerDied}
		orch.GroupCh <- orchestrator.GroupEvent{Type: orchestrator.GroupRestarting}
		orch.GroupCh <- orchestrator.GroupEvent{Type: orchestrator.GroupReady, Ranks: ranks}
		for r := 0; r < ranks; r++ {
			src.Emit(events.KindStart, resumeStep, r)
		}
		src.Emit(events.KindProgress, resumeStep+1, 0)
	}
}

func TestRun_Passes_WhenResumedFromCheckpoint(t *testing.T) {
	orch := fake.NewOrchestrator(2)
	src := fake.NewSource()
	preKill(src)
	orch.OnKill = restartWith(orch, src, 2, 1000)

	res, err := New(orch, src, baseCfg()).Run(context.Background())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if res.Verdict != VerdictPassed {
		t.Errorf("verdict = %q (%s), want PASSED", res.Verdict, res.Reason)
	}
	if res.ResumedFromStep != 1000 {
		t.Errorf("ResumedFromStep = %d, want 1000", res.ResumedFromStep)
	}
	if res.RanksRejoined != 2 {
		t.Errorf("RanksRejoined = %d, want 2", res.RanksRejoined)
	}
	if res.StepsLost != 0 {
		t.Errorf("StepsLost = %d, want 0", res.StepsLost)
	}
	if len(orch.Killed) != 1 {
		t.Fatalf("killed %d workers, want exactly 1", len(orch.Killed))
	}
	if res.KilledRank != orch.Killed[0].Rank {
		t.Errorf("KilledRank = %d, want %d", res.KilledRank, orch.Killed[0].Rank)
	}
	if !orch.CleanedUp {
		t.Error("Cleanup was not called")
	}
}

// The money test: the job restarted fine but training silently began again from
// zero, because the checkpoint was written to node-local disk.
func TestRun_Fails_WhenResumedFromZero(t *testing.T) {
	orch := fake.NewOrchestrator(2)
	src := fake.NewSource()
	preKill(src)
	orch.OnKill = restartWith(orch, src, 2, 0)

	res, err := New(orch, src, baseCfg()).Run(context.Background())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if res.Verdict != VerdictFailed {
		t.Errorf("verdict = %q, want FAILED", res.Verdict)
	}
	if res.ResumedFromStep != 0 {
		t.Errorf("ResumedFromStep = %d, want 0", res.ResumedFromStep)
	}
	if res.StepsLost != 1000 {
		t.Errorf("StepsLost = %d, want 1000", res.StepsLost)
	}
	if res.Reason == "" {
		t.Error("Reason must explain WHY it failed — the reason string is the product")
	}
}

func TestRun_Fails_WhenRecoveryExceedsSLO(t *testing.T) {
	orch := fake.NewOrchestrator(2)
	src := fake.NewSource()
	preKill(src)
	orch.OnKill = restartWith(orch, src, 2, 1000)

	// Clock jumps 10 minutes on every call, so any measured recovery blows the
	// 1-minute SLO regardless of how many times Run consults it.
	base := time.Now()
	var calls int
	clk := func() time.Time {
		calls++
		return base.Add(time.Duration(calls) * 10 * time.Minute)
	}

	cfg := baseCfg()
	cfg.MaxRecoveryTime = time.Minute

	res, err := New(orch, src, cfg, WithClock(clk)).Run(context.Background())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if res.Verdict != VerdictFailed {
		t.Errorf("verdict = %q, want FAILED (recovery exceeded SLO)", res.Verdict)
	}
	if res.RecoveryTime < cfg.MaxRecoveryTime {
		t.Errorf("RecoveryTime = %v, want > %v", res.RecoveryTime, cfg.MaxRecoveryTime)
	}
}

func TestRun_Fails_WhenNotAllRanksRejoin(t *testing.T) {
	orch := fake.NewOrchestrator(2)
	src := fake.NewSource()
	preKill(src)
	orch.OnKill = restartWith(orch, src, 1, 1000) // only 1 of 2 comes back

	res, err := New(orch, src, baseCfg()).Run(context.Background())
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if res.Verdict != VerdictFailed {
		t.Errorf("verdict = %q, want FAILED (only 1 rank rejoined)", res.Verdict)
	}
	if res.RanksRejoined != 1 {
		t.Errorf("RanksRejoined = %d, want 1", res.RanksRejoined)
	}
}

// Stretch goal — unskip once the four above are green.
func TestRun_Fails_WhenNoRestartObserved(t *testing.T) {
	t.Skip("stretch: add a timeout so a job that never restarts fails fast instead of hanging")
}
