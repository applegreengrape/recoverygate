package engine

// Phase is the drill state machine's current state.
//
//	Pending
//	  -> WaitingForCheckpoint   block until checkpoint >= AfterCheckpoint
//	  -> RecordingBaseline      snapshot workers + current step
//	  -> InjectingFailure       kill exactly one worker
//	  -> WaitingForRestart      watch the group tear down and come back
//	  -> VerifyingProgress      where did it resume? how long? how much lost?
//	  -> Passed | Failed
//
// Cleanup always runs, on every exit path.
type Phase string

const (
	PhasePending              Phase = "Pending"
	PhaseWaitingForCheckpoint Phase = "WaitingForCheckpoint"
	PhaseRecordingBaseline    Phase = "RecordingBaseline"
	PhaseInjectingFailure     Phase = "InjectingFailure"
	PhaseWaitingForRestart    Phase = "WaitingForRestart"
	PhaseVerifyingProgress    Phase = "VerifyingProgress"
	PhasePassed               Phase = "Passed"
	PhaseFailed               Phase = "Failed"
)
