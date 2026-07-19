package engine

import (
	"encoding/json"
	"time"

	"github.com/applegreengrape/recoverygate/internal/orchestrator"
)

// Config is the drill definition — the Go form of recovery-test.yaml.
type Config struct {
	Selector orchestrator.Selector

	// AfterCheckpoint: wait for a checkpoint at or beyond this step before
	// injecting the fault. Makes the drill deterministic.
	AfterCheckpoint int

	// ExpectedRanks is how many workers must be back up to count as rejoined.
	ExpectedRanks int

	// MaxRecoveryTime is the SLO: kill -> resumed training.
	MaxRecoveryTime time.Duration

	// MaxLostSteps is how much work we tolerate losing.
	MaxLostSteps int
}

// Verdict is the drill outcome.
type Verdict string

const (
	VerdictPassed Verdict = "PASSED"
	VerdictFailed Verdict = "FAILED"
)

// Result is the artifact the customer keeps (and CI gates on).
type Result struct {
	Verdict Verdict `json:"verdict"`
	Reason  string  `json:"reason"`

	KilledRank      int           `json:"killed_rank"`
	CheckpointStep  int           `json:"checkpoint_step"`   // expected resume point
	BaselineStep    int           `json:"baseline_step"`     // step reached before the kill
	ResumedFromStep int           `json:"resumed_from_step"` // step actually resumed from
	StepsLost       int           `json:"steps_lost"`
	RecoveryTime    time.Duration `json:"recovery_time_ns"` // measured on OUR clock
	RanksRejoined   int           `json:"ranks_rejoined"`
}

// MarshalJSON adds a human-readable recovery time alongside the raw duration,
// so result.json is useful to both jq and a person reading a CI log.
func (r Result) MarshalJSON() ([]byte, error) {
	type alias Result
	return json.Marshal(struct {
		alias
		RecoveryTimeSeconds float64 `json:"recovery_time_seconds"`
		RecoveryTimeHuman   string  `json:"recovery_time"`
	}{
		alias:               alias(r),
		RecoveryTimeSeconds: r.RecoveryTime.Seconds(),
		RecoveryTimeHuman:   r.RecoveryTime.Round(time.Millisecond).String(),
	})
}
