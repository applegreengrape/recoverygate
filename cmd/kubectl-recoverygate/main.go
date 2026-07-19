// Command kubectl-recoverygate is the krew-distributed alias of recoverygate.
//
// kubectl discovers any executable on PATH named `kubectl-<name>` and exposes
// it as `kubectl <name>`, so this binary runs as:
//
//	kubectl recoverygate -select training-run=llama-finetune
//
// It shares all logic with the primary binary — see internal/cli.
package main

import (
	"os"

	"github.com/applegreengrape/recoverygate/internal/cli"
)

func main() {
	os.Exit(cli.Execute(os.Args[1:]))
}
