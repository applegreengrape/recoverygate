// Package config loads recovery-test.yaml — the declarative drill definition.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/applegreengrape/recoverygate/internal/engine"
	"github.com/applegreengrape/recoverygate/internal/orchestrator"
)

// File mirrors recovery-test.yaml on disk.
type File struct {
	Workload struct {
		Orchestrator string `yaml:"orchestrator"`
		Selector     string `yaml:"selector"`
		Namespace    string `yaml:"namespace"`
	} `yaml:"workload"`

	InjectFailure struct {
		Type            string `yaml:"type"`
		AfterCheckpoint string `yaml:"afterCheckpoint"` // "step-1000" or "1000"
	} `yaml:"injectFailure"`

	Expectations struct {
		ResumeFromCheckpoint string `yaml:"resumeFromCheckpoint"`
		MaxRecoveryTime      string `yaml:"maxRecoveryTime"` // "5m"
		MaxLostSteps         int    `yaml:"maxLostSteps"`
		ExpectedRanks        int    `yaml:"expectedRanks"`
	} `yaml:"expectations"`

	Safety struct {
		RequireConfirm bool `yaml:"requireConfirm"`
		DryRun         bool `yaml:"dryRun"`
	} `yaml:"safety"`
}

// Load reads and validates a drill definition.
func Load(path string) (*File, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var f File
	if err := yaml.Unmarshal(raw, &f); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if f.Workload.Selector == "" {
		return nil, fmt.Errorf("%s: workload.selector is required", path)
	}
	switch f.Workload.Orchestrator {
	case "", "kubernetes":
		// supported
	case "slurm":
		return nil, fmt.Errorf(
			"%s: the slurm adapter is a placeholder and not implemented yet "+
				"(see internal/orchestrator/slurm)", path)
	default:
		return nil, fmt.Errorf(
			"%s: unknown orchestrator %q — supported: kubernetes (slurm planned)",
			path, f.Workload.Orchestrator)
	}
	if f.Workload.Namespace == "" {
		f.Workload.Namespace = "default"
	}
	if f.InjectFailure.Type != "" && f.InjectFailure.Type != "KillWorker" {
		return nil, fmt.Errorf("%s: only injectFailure.type 'KillWorker' is supported in v0.1", path)
	}
	if f.Expectations.ExpectedRanks <= 0 {
		return nil, fmt.Errorf("%s: expectations.expectedRanks must be set (how many workers must rejoin)", path)
	}
	return &f, nil
}

// EngineConfig converts the file into what the engine needs.
func (f *File) EngineConfig() (engine.Config, error) {
	after, err := parseStep(f.InjectFailure.AfterCheckpoint)
	if err != nil {
		return engine.Config{}, fmt.Errorf("injectFailure.afterCheckpoint: %w", err)
	}

	slo := 5 * time.Minute
	if s := f.Expectations.MaxRecoveryTime; s != "" {
		if slo, err = time.ParseDuration(s); err != nil {
			return engine.Config{}, fmt.Errorf("expectations.maxRecoveryTime: %w", err)
		}
	}

	lost := f.Expectations.MaxLostSteps
	if lost == 0 {
		lost = 1 << 30 // unset = don't gate on steps lost
	}

	return engine.Config{
		Selector:        orchestrator.Selector{Labels: f.Workload.Selector},
		AfterCheckpoint: after,
		ExpectedRanks:   f.Expectations.ExpectedRanks,
		MaxRecoveryTime: slo,
		MaxLostSteps:    lost,
	}, nil
}

// parseStep accepts "step-1000", "1000", or "" (meaning "any checkpoint").
func parseStep(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	s = strings.TrimPrefix(s, "step-")
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("expected a step like 'step-1000', got %q", s)
	}
	return n, nil
}
