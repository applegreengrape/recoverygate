// Command recoverygate runs a recovery drill against a distributed training job.
//
// This is the primary, orchestrator-neutral binary. Install with:
//
//	go install github.com/applegreengrape/recoverygate/cmd/recoverygate@latest
package main

import (
	"os"

	"github.com/applegreengrape/recoverygate/internal/cli"
)

func main() {
	os.Exit(cli.Execute(os.Args[1:]))
}
