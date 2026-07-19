// Package kube is the Kubernetes implementation of orchestrator.Orchestrator.
//
// This is the ONLY package in the tool that imports client-go. If an import of
// it ever appears in internal/engine, the SLURM adapter becomes a rewrite.
package kube

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/applegreengrape/recoverygate/internal/orchestrator"
)

// replicaIndexLabel is what the Kubeflow Training Operator stamps on each pod.
const replicaIndexLabel = "training.kubeflow.org/replica-index"

// Orchestrator drives a training job on Kubernetes.
type Orchestrator struct {
	cs        kubernetes.Interface
	namespace string
	expected  int // how many ranks constitute a healthy group
}

// New builds a kube orchestrator. Empty kubeconfig uses the default loading
// rules (KUBECONFIG, then ~/.kube/config, then in-cluster).
func New(kubeconfig, namespace string, expectedRanks int) (*Orchestrator, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		rules.ExplicitPath = kubeconfig
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		rules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig: %w", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("build kubernetes client: %w", err)
	}
	return &Orchestrator{cs: cs, namespace: namespace, expected: expectedRanks}, nil
}

// Clientset exposes the client so the log event source can share it.
func (o *Orchestrator) Clientset() kubernetes.Interface { return o.cs }

// Namespace returns the target namespace.
func (o *Orchestrator) Namespace() string { return o.namespace }

func (o *Orchestrator) Discover(ctx context.Context, sel orchestrator.Selector) ([]orchestrator.Worker, error) {
	pods, err := o.cs.CoreV1().Pods(o.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: sel.Labels,
	})
	if err != nil {
		return nil, fmt.Errorf("list pods (%s): %w", sel.Labels, err)
	}

	out := make([]orchestrator.Worker, 0, len(pods.Items))
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.DeletionTimestamp != nil {
			continue // already on its way out
		}
		out = append(out, orchestrator.Worker{
			Rank:  rankOf(p),
			ID:    p.Name,
			Node:  p.Spec.NodeName,
			Alive: p.Status.Phase == corev1.PodRunning,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Rank < out[j].Rank })
	return out, nil
}

// Kill deletes exactly one pod, with grace period 0 so it looks like an abrupt
// loss (a preemption or a node failure) rather than a graceful shutdown.
func (o *Orchestrator) Kill(ctx context.Context, w orchestrator.Worker) error {
	grace := int64(0)
	err := o.cs.CoreV1().Pods(o.namespace).Delete(ctx, w.ID, metav1.DeleteOptions{
		GracePeriodSeconds: &grace,
	})
	if err != nil {
		return fmt.Errorf("delete pod %s: %w", w.ID, err)
	}
	return nil
}

// WatchGroup translates pod churn into worker-group lifecycle events.
//
// It only reports GroupReady once it has seen a death, so a group that was
// already healthy at subscribe time doesn't produce a spurious "recovered".
func (o *Orchestrator) WatchGroup(ctx context.Context, sel orchestrator.Selector) (<-chan orchestrator.GroupEvent, error) {
	w, err := o.cs.CoreV1().Pods(o.namespace).Watch(ctx, metav1.ListOptions{
		LabelSelector: sel.Labels,
	})
	if err != nil {
		return nil, fmt.Errorf("watch pods (%s): %w", sel.Labels, err)
	}

	out := make(chan orchestrator.GroupEvent, 32)
	go func() {
		defer close(out)
		defer w.Stop()

		ready := map[string]bool{}
		var sawDeath bool

		emit := func(t orchestrator.GroupEventType, ranks int) {
			select {
			case out <- orchestrator.GroupEvent{Type: t, Time: time.Now(), Ranks: ranks}:
			case <-ctx.Done():
			}
		}

		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-w.ResultChan():
				if !ok {
					return // watch expired; caller's ctx governs retry
				}
				pod, ok := ev.Object.(*corev1.Pod)
				if !ok {
					continue
				}
				switch ev.Type {
				case watch.Deleted:
					delete(ready, pod.Name)
					sawDeath = true
					emit(orchestrator.WorkerDied, countReady(ready))
					emit(orchestrator.GroupRestarting, countReady(ready))
				case watch.Added, watch.Modified:
					wasReady := ready[pod.Name]
					isReady := podReady(pod)
					ready[pod.Name] = isReady
					// A running pod that flipped to not-ready is also a death
					// signal (crash-loop, OOM) even without a Deleted event.
					if wasReady && !isReady {
						sawDeath = true
						emit(orchestrator.WorkerDied, countReady(ready))
					}
				}
				if sawDeath && countReady(ready) >= o.expected {
					emit(orchestrator.GroupReady, countReady(ready))
					sawDeath = false
				}
			}
		}
	}()
	return out, nil
}

// Cleanup is a no-op: the drill mutates nothing. It deletes one pod, which the
// Training Operator recreates — there is no state of ours to unwind.
func (o *Orchestrator) Cleanup(ctx context.Context, sel orchestrator.Selector) error {
	return nil
}

// rankOf prefers the Kubeflow replica-index label and falls back to the numeric
// suffix of the pod name (…-worker-2).
func rankOf(p *corev1.Pod) int {
	if v, ok := p.Labels[replicaIndexLabel]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	if i := strings.LastIndex(p.Name, "-"); i >= 0 {
		if n, err := strconv.Atoi(p.Name[i+1:]); err == nil {
			return n
		}
	}
	return 0
}

func podReady(p *corev1.Pod) bool {
	if p.Status.Phase != corev1.PodRunning || p.DeletionTimestamp != nil {
		return false
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func countReady(m map[string]bool) int {
	n := 0
	for _, ok := range m {
		if ok {
			n++
		}
	}
	return n
}
