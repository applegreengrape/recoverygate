# RecoveryGate — build, verify, ship.
#
#   make            list targets
#   make check      what CI runs (fmt + vet + test)
#   make build      both binaries into bin/
#   make drill      run a drill against the current kube context

SHELL      := /bin/bash
GO         ?= go
BIN        := bin
PKG        := github.com/applegreengrape/recoverygate
VERSION    ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS    := -s -w -X $(PKG)/internal/cli.version=$(VERSION)

# Cluster helpers (override: make deploy-healthy ZONE=europe-west2-a)
ZONE       ?= us-central1-a
CLUSTER    ?= rg-test
NS         ?= default
SELECTOR   ?= training-run=recoverygate-demo
CONFIG     ?= examples/recovery-test.yaml

.DEFAULT_GOAL := help

## ---------------------------------------------------------------- build ----

.PHONY: build
build: ## Build both binaries into bin/
	@mkdir -p $(BIN)
	$(GO) build -ldflags '$(LDFLAGS)' -o $(BIN)/recoverygate ./cmd/recoverygate
	$(GO) build -ldflags '$(LDFLAGS)' -o $(BIN)/kubectl-recoverygate ./cmd/kubectl-recoverygate
	@echo "built $(VERSION) -> $(BIN)/"

.PHONY: install
install: ## go install the primary binary
	$(GO) install -ldflags '$(LDFLAGS)' ./cmd/recoverygate

.PHONY: tidy
tidy: ## go mod tidy
	$(GO) mod tidy

.PHONY: clean
clean: ## Remove build output and drill artifacts
	rm -rf $(BIN) dist result.json

## --------------------------------------------------------------- verify ----

.PHONY: check
check: fmt-check vet test ## Everything CI runs — run this before pushing

.PHONY: test
test: ## Unit tests (engine, via fakes — no cluster needed)
	$(GO) test ./...

.PHONY: test-race
test-race: ## Unit tests with the race detector
	$(GO) test -race ./...

.PHONY: cover
cover: ## Coverage report -> coverage.html
	$(GO) test -coverprofile=coverage.out ./...
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "open coverage.html"

.PHONY: vet
vet: ## go vet
	$(GO) vet ./...

.PHONY: fmt
fmt: ## Format the tree
	$(GO) fmt ./...

.PHONY: fmt-check
fmt-check: ## Fail if anything is unformatted
	@out=$$(gofmt -l . | grep -v '^vendor/' || true); \
	if [ -n "$$out" ]; then echo "unformatted:"; echo "$$out"; exit 1; fi

## ------------------------------------------------------------- the drill ----

.PHONY: drill
drill: build ## Run a drill against the current kube context
	$(BIN)/recoverygate run -f $(CONFIG)

.PHONY: drill-dry
drill-dry: build ## Preflight only — every phase except the kill
	$(BIN)/recoverygate run -f $(CONFIG) --dry-run

## ------------------------------------------------------------ test rig ----
## Requires testrig/ locally (it is gitignored).

.PHONY: operator
operator: ## Install the Kubeflow training operator
	kubectl apply -k "github.com/kubeflow/training-operator/manifests/overlays/standalone?ref=v1.8.1"
	kubectl -n kubeflow rollout status deploy/training-operator

.PHONY: nfs
nfs: ## Install an in-cluster NFS provisioner (ReadWriteMany)
	helm repo add nfs-ganesha https://kubernetes-sigs.github.io/nfs-ganesha-server-and-external-provisioner/ || true
	helm repo update
	helm upgrade --install nfs nfs-ganesha/nfs-server-provisioner \
		--set storageClass.name=nfs --set persistence.enabled=true --set persistence.size=10Gi

.PHONY: configmap
configmap: ## (Re)create the ConfigMap holding the training scripts
	kubectl create configmap recoverygate-scripts \
		--from-file=testrig/reporter.py \
		--from-file=testrig/train_ddp.py \
		--dry-run=client -o yaml | kubectl apply -f -

.PHONY: deploy-healthy
deploy-healthy: configmap ## Deploy the healthy pipeline (expect PASS)
	kubectl apply -f testrig/cpu/pytorchjob-healthy.yaml

.PHONY: deploy-broken
deploy-broken: configmap ## Deploy the broken pipeline (expect FAIL)
	kubectl apply -f testrig/cpu/pytorchjob-broken.yaml

.PHONY: pods
pods: ## Watch the training pods (confirm they land on DIFFERENT nodes)
	kubectl get pods -l $(SELECTOR) -o wide -w

.PHONY: events
events: ## Tail reporter events from all ranks
	kubectl logs -f -l $(SELECTOR) --prefix --tail=-1 | grep RECOVERYGATE

.PHONY: kill-worker
kill-worker: ## Manually kill one worker (the drill does this for you)
	kubectl delete pod -l $(SELECTOR),training.kubeflow.org/replica-type=worker \
		--grace-period=0 --force

.PHONY: clean-cluster
clean-cluster: ## Remove jobs + PVC (ALWAYS do this between healthy/broken runs)
	-kubectl delete -f testrig/cpu/pytorchjob-healthy.yaml --ignore-not-found
	-kubectl delete -f testrig/cpu/pytorchjob-broken.yaml --ignore-not-found
	-kubectl delete pvc recoverygate-shared --ignore-not-found

.PHONY: teardown
teardown: ## DELETE the GKE cluster — do not forget this
	gcloud container clusters delete $(CLUSTER) --zone=$(ZONE) --quiet

## ---------------------------------------------------------------- ship ----

.PHONY: snapshot
snapshot: ## Local goreleaser build, no publish
	goreleaser release --snapshot --clean

.PHONY: release
release: check ## Tag-driven release (run: git tag vX.Y.Z && git push --tags)
	goreleaser release --clean

.PHONY: checksums
checksums: ## Show sha256 sums to paste into .krew/recoverygate.yaml
	@cat dist/checksums.txt 2>/dev/null || echo "run 'make snapshot' or 'make release' first"

## ---------------------------------------------------------------- help ----

.PHONY: help
help: ## Show this help
	@echo "RecoveryGate $(VERSION)"
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'
