// Package cli holds the command-line entry point, shared by both binaries:
//
//	recoverygate            — primary, orchestrator-neutral (go install)
//	kubectl-recoverygate    — same code, krew-discoverable as `kubectl recoverygate`
//
// Keeping the logic here (rather than in a main package) is what lets us ship
// both without duplicating anything.
package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/applegreengrape/recoverygate/internal/config"
	"github.com/applegreengrape/recoverygate/internal/engine"
	"github.com/applegreengrape/recoverygate/internal/events/logs"
	"github.com/applegreengrape/recoverygate/internal/orchestrator/kube"
)

// version is set at build time via ldflags:
//
//	-X github.com/applegreengrape/recoverygate/internal/cli.version=v0.1.0
var version = "dev"

// Version returns the build version.
func Version() string { return version }

// Exit codes: 0 = PASS, 1 = FAIL (so CI gates work), 2 = usage/setup error.
const (
	exitPass  = 0
	exitFail  = 1
	exitError = 2
)

// Execute parses args and runs the CLI, returning the process exit code.
func Execute(args []string) int {
	fs := flag.NewFlagSet("recoverygate", flag.ContinueOnError)
	var (
		file        = fs.String("f", "", "path to recovery-test.yaml")
		kubeconfig  = fs.String("kubeconfig", "", "path to kubeconfig (default: standard loading rules)")
		resultPath  = fs.String("result", "result.json", "where to write the machine-readable result")
		dryRun      = fs.Bool("dry-run", false, "run every phase except the kill")
		yes         = fs.Bool("y", false, "skip the confirmation prompt")
		timeout     = fs.Duration("timeout", 30*time.Minute, "overall drill timeout")
		showVersion = fs.Bool("version", false, "print version and exit")
	)
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "recoverygate — prove a training job recovers from worker loss\n\n")
		fmt.Fprintf(os.Stderr, "usage:\n  recoverygate run -f recovery-test.yaml\n\nflags:\n")
		fs.PrintDefaults()
	}

	// Accept an optional leading "run" verb so both `recoverygate -f x.yaml`
	// and `recoverygate run -f x.yaml` work.
	if len(args) > 0 && args[0] == "run" {
		args = args[1:]
	}
	if err := fs.Parse(args); err != nil {
		return exitError
	}
	if *showVersion {
		fmt.Println("recoverygate", version)
		return exitPass
	}
	if *file == "" {
		fs.Usage()
		return exitError
	}

	if err := run(*file, *kubeconfig, *resultPath, *dryRun, *yes, *timeout); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		if err == errDrillFailed {
			return exitFail
		}
		return exitError
	}
	return exitPass
}

var errDrillFailed = fmt.Errorf("recovery drill failed")

func run(file, kubeconfig, resultPath string, dryRun, yes bool, timeout time.Duration) error {
	cf, err := config.Load(file)
	if err != nil {
		return err
	}
	ecfg, err := cf.EngineConfig()
	if err != nil {
		return err
	}

	// Ctrl-C cancels the drill; the engine's deferred cleanup still runs.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	orch, err := kube.New(kubeconfig, cf.Workload.Namespace, ecfg.ExpectedRanks)
	if err != nil {
		return err
	}
	src := logs.New(orch.Clientset(), orch.Namespace(), cf.Workload.Selector)

	// Preflight: fail fast and legibly rather than mid-drill.
	workers, err := orch.Discover(ctx, ecfg.Selector)
	if err != nil {
		return fmt.Errorf("preflight: %w", err)
	}
	if len(workers) == 0 {
		return fmt.Errorf("preflight: no pods match selector %q in namespace %q",
			cf.Workload.Selector, cf.Workload.Namespace)
	}
	fmt.Printf("target:   %d workers matching %q\n", len(workers), cf.Workload.Selector)
	for _, w := range workers {
		fmt.Printf("          rank %d  %s  (node %s)\n", w.Rank, w.ID, w.Node)
	}
	fmt.Printf("drill:    kill 1 worker after checkpoint step %d; expect resume within %s\n",
		ecfg.AfterCheckpoint, ecfg.MaxRecoveryTime)

	if dryRun || cf.Safety.DryRun {
		fmt.Println("\ndry-run: everything above is real; the kill would happen now. Exiting.")
		return nil
	}
	if cf.Safety.RequireConfirm && !yes {
		if !confirm() {
			return fmt.Errorf("aborted")
		}
	}

	res, runErr := engine.New(orch, src, ecfg).Run(ctx)
	if res != nil {
		printCertificate(res, ecfg)
		if err := writeResult(resultPath, res); err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not write %s: %v\n", resultPath, err)
		}
	}
	if n := src.Unparsed(); n > 0 {
		fmt.Fprintf(os.Stderr,
			"warning: %d reporter lines failed to parse — is the reporter version mismatched?\n", n)
	}
	if runErr != nil {
		return runErr
	}
	if res.Verdict == engine.VerdictFailed {
		return errDrillFailed
	}
	return nil
}

func confirm() bool {
	fmt.Print("\nThis will KILL one worker pod. Continue? [y/N] ")
	sc := bufio.NewScanner(os.Stdin)
	if !sc.Scan() {
		return false
	}
	answer := strings.ToLower(strings.TrimSpace(sc.Text()))
	return answer == "y" || answer == "yes"
}

// printCertificate is the artifact the customer keeps. The Reason line is the
// product — be specific.
func printCertificate(r *engine.Result, cfg engine.Config) {
	line := strings.Repeat("─", 64)
	fmt.Printf("\n%s\nRECOVERY DRILL: %s\n", line, r.Verdict)
	fmt.Printf("  %s\n\n", r.Reason)
	fmt.Printf("  Fault injected:   killed rank %d near step %d\n", r.KilledRank, r.BaselineStep)
	fmt.Printf("  Expected:         resume from step %d within %s, ≤%d steps lost\n",
		r.CheckpointStep, cfg.MaxRecoveryTime, cfg.MaxLostSteps)
	fmt.Printf("  Resumed from:     step %d\n", r.ResumedFromStep)
	fmt.Printf("  Recovery time:    %s\n", r.RecoveryTime.Round(time.Millisecond))
	fmt.Printf("  Steps lost:       %d\n", r.StepsLost)
	fmt.Printf("  Ranks rejoined:   %d/%d\n", r.RanksRejoined, cfg.ExpectedRanks)
	fmt.Println(line)
}

func writeResult(path string, r *engine.Result) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}
