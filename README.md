# RecoveryGate

**Chaos testing purpose-built for distributed AI training.**

RecoveryGate deliberately kills a worker in a distributed PyTorch job and proves the
job resumes from its latest checkpoint within an agreed recovery SLO — before you
trust it with an expensive run.

`torchrun` restarts a failed worker group and the Kubeflow Training Operator
recreates a dead pod, but neither verifies that *your* code, *your* checkpoint
storage and *your* cluster config actually work together under real failure. A
generic chaos tool sees a pod die and a pod come back, and calls that success — it
has no idea training just silently restarted from step 0.

## How it works

```text
  Client                          Kubernetes cluster
┌──────────────────┐   k8s API   ┌──────────────────────────────────────────────────────┐
│ recoverygate CLI │────────────▶│  control pool (no GPU)        gpu pool               │
│ kubeconfig       │  list       │  ┌─────────────────────┐      ┌────────────────────┐ │
│                  │  delete     │  │ Training Operator   │─────▶│ node-1             │ │
│                  │  watch      │  │ NFS server          │      │ pod: Master rank 0 │ │
│                  │  pods/log   │  │ system pods         │      │ torchrun · reporter│ │
└──────────────────┘             │  └─────────────────────┘      └─────────┬──────────┘ │
                                 │     creates & restarts pods             │ NCCL       │
                                 │                               ┌─────────┴──────────┐ │
                                 │  ┌─────────────────────┐      │ node-2             │ │
                                 │  │ PVC · ReadWriteMany │◀─────│ pod: Worker rank 1 │ │
                                 │  │ /shared/ckpts       │      │ ← the drill kills  │ │
                                 │  │ survives node change│      │   this one         │ │
                                 │  └─────────────────────┘      └────────────────────┘ │
                                 └──────────────────────────────────────────────────────┘

The drill
  1 · wait for a checkpoint              4 · ranks re-rendezvous over NCCL
  2 · delete the rank-1 pod              5 · read the resume step from pod logs
  3 · operator recreates it                  → PASS if it matches the checkpoint
      (maybe on another node)                → FAIL if it resumed from 0
```

The **engine** owns the state machine and the verdict, and knows nothing about any
cluster. It reaches the world through two small interfaces — `Orchestrator`
(discover / kill / watch / cleanup) and `EventSource` (training progress).
Kubernetes is simply the first `Orchestrator` implementation; SLURM is additive,
not a rewrite.

Event transport is **pod stdout**, streamed via `pods/log` — no volume mounts, no
sidecar, no pod-spec changes. The only RBAC needed is `pods` (list/watch/delete)
and `pods/log` (get) in one namespace.

## Orchestrator support

| Orchestrator | Status |
| --- | --- |
| **Kubernetes** (Kubeflow `PyTorchJob` / Trainer `TrainJob`) | supported |
| **SLURM** | planned — placeholder in `internal/orchestrator/slurm` |

Kubernetes is where v0.1 runs, but it isn't where most large-scale training
lives. **SLURM is the next adapter**, and the architecture is built so it's
additive rather than a rewrite: the engine depends only on the `Orchestrator`
interface, so a second backend is a new package implementing four methods —
`Discover`, `Kill`, `WatchGroup`, `Cleanup` — plus an event source. **The engine
changes by zero lines.**

That isn't just an intention, it's compiler-enforced. The placeholder carries:

```go
var _ orchestrator.Orchestrator = (*Orchestrator)(nil)
```

If a future change leaks a Kubernetes concept into the interface, that line stops
compiling — you find out at `go build`, not months later.

**Why SLURM matters.** HPC jobs run under a wall-clock limit, so multi-day
training is necessarily a chain of requeued jobs, each loading the last
checkpoint. Recovery there is the normal operating mode, not an edge case. And
the same bug exists in local dialect: checkpoints written to node-local scratch
(`/tmp`, or `$SLURM_TMPDIR` which is purged at job end) instead of the shared
parallel filesystem — the job requeues onto different nodes and restarts from
zero.

**Why the drill still adds value there.** SLURM teams exercise their resume path
daily via time-limit requeue, so the obvious bugs surface fast. But a time-limit
stop is *graceful* — SIGTERM arrives first and the script can save. A rank dying
mid-step is *abrupt*. Those are different code paths, and only the first one gets
tested by accident.

What changes under the hood: `squeue`/`scancel`/`sacct` instead of the Kubernetes
API, polling instead of a watch stream, and tailing `slurm-<jobid>.out` on
Lustre/GPFS instead of streaming pod logs. Same sentinel, same parser, same
verdict logic.

## Usage

**1. Report progress** — two lines in your training loop, so the tool can see
*training* progress rather than just pod status:

```python
from recoverygate import reporter
reporter.checkpoint_completed(global_step)   # after each checkpoint save
reporter.progress(global_step)               # each step
```

**2. Define the drill** — a unit test for fault tolerance:

```yaml
# recovery-test.yaml
workload:
  orchestrator: kubernetes
  selector: "training-run=llama-finetune"
injectFailure:
  type: KillWorker
  afterCheckpoint: step-1000     # deterministic: wait for a known good state
expectations:
  resumeFromCheckpoint: step-1000
  maxRecoveryTime: 5m
  maxLostSteps: 100
safety:
  requireConfirm: true
  dryRun: false
```

**3. Run it:**

```bash
recoverygate run -f recovery-test.yaml
# or, as a kubectl plugin:
kubectl recoverygate run -f recovery-test.yaml
```

It exits non-zero on FAIL, so it drops straight into CI as a gate on every
framework, storage or cluster change.

### Install

```bash
go install github.com/applegreengrape/recoverygate/cmd/recoverygate@latest

# kubectl users, straight from the manifest
kubectl krew install --manifest=.krew/recoverygate.yaml
```

## Results

Real drill results — from a 2-node `PyTorchJob` on rented GPUs, showing both a
correct PASS and a correct FAIL — will be published here once testing is complete.

## Safety

RecoveryGate deliberately destroys things. It kills exactly one worker, defaults to
`requireConfirm`, supports `--dry-run` (the full flow minus the kill), and cleans up
on every exit path. **Run it against a staging job before pointing it at anything
you care about.**

## Development

```bash
go test ./...
```

```text
cmd/recoverygate/            primary binary (orchestrator-neutral)
cmd/kubectl-recoverygate/    krew alias — runs as `kubectl recoverygate`
internal/cli/                shared CLI entry
internal/config/             recovery-test.yaml loading
internal/engine/             state machine + verdict (no cluster knowledge)
internal/orchestrator/       Orchestrator interface
  kube/                      Kubernetes adapter — the only client-go importer
  slurm/                     placeholder + design notes for the next adapter
  fake/                      in-memory adapter for tests
internal/events/             EventSource interface + TrainingEvent
  logs/                      pod-log transport (RECOVERYGATE/v1 sentinel)
```

## License

[Apache 2.0](LICENSE)
