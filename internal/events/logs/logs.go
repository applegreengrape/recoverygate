// Package logs implements events.Source by streaming pod stdout.
package logs

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"

	"github.com/applegreengrape/recoverygate/internal/events"
)

// Sentinel prefixes every reporter line so PyTorch/NCCL spam is trivially
// ignorable. Versioned so we can change the payload without silent breakage.
const Sentinel = "RECOVERYGATE/v1 "

// Source streams reporter events out of the pods matching a selector.
type Source struct {
	cs        kubernetes.Interface
	namespace string
	selector  string

	// unparsed counts lines that looked like ours but failed to parse. Surface
	// it in diagnostics — never swallow it, or a changed sentinel looks
	// identical to "no events".
	unparsed atomic.Int64
}

// New builds a pod-log event source. Share the clientset with the orchestrator.
func New(cs kubernetes.Interface, namespace, selector string) *Source {
	return &Source{cs: cs, namespace: namespace, selector: selector}
}

// Unparsed reports how many sentinel-looking lines failed to parse.
func (s *Source) Unparsed() int64 { return s.unparsed.Load() }

// Events watches for pods matching the selector and streams each one's logs.
// New pods (i.e. restart replacements) are picked up automatically.
func (s *Source) Events(ctx context.Context) (<-chan events.TrainingEvent, error) {
	w, err := s.cs.CoreV1().Pods(s.namespace).Watch(ctx, metav1.ListOptions{
		LabelSelector: s.selector,
	})
	if err != nil {
		return nil, err
	}

	out := make(chan events.TrainingEvent, 256)
	var (
		mu       sync.Mutex
		attached = map[string]bool{} // pod UID -> streaming
		wg       sync.WaitGroup
	)

	go func() {
		defer close(out)
		defer w.Stop()

		for {
			select {
			case <-ctx.Done():
				wg.Wait()
				return
			case ev, ok := <-w.ResultChan():
				if !ok {
					wg.Wait()
					return
				}
				pod, ok := ev.Object.(*corev1.Pod)
				if !ok || ev.Type == watch.Deleted {
					continue
				}
				if pod.Status.Phase == corev1.PodPending {
					continue // no logs yet
				}

				// Key on UID, not name: a replacement pod frequently reuses the
				// name, and we must attach to it as a genuinely new stream.
				key := string(pod.UID)
				mu.Lock()
				already := attached[key]
				if !already {
					attached[key] = true
				}
				mu.Unlock()
				if already {
					continue
				}

				wg.Add(1)
				go func(name string) {
					defer wg.Done()
					s.stream(ctx, name, out)
				}(pod.Name)
			}
		}
	}()

	return out, nil
}

// stream follows one pod's logs from the BEGINNING of the container log.
//
// Deliberately not "follow from now": the first `start` line is emitted within
// milliseconds of the container starting and it is the line that decides the
// verdict. Tailing from now races the pod and loses it.
func (s *Source) stream(ctx context.Context, podName string, out chan<- events.TrainingEvent) {
	req := s.cs.CoreV1().Pods(s.namespace).GetLogs(podName, &corev1.PodLogOptions{
		Follow: true,
	})
	rc, err := req.Stream(ctx)
	if err != nil {
		return // pod may already be gone; the watch will bring us its replacement
	}
	defer rc.Close()

	sc := bufio.NewScanner(rc)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024) // training logs get long
	for sc.Scan() {
		ev, ok := parse(sc.Text())
		if !ok {
			continue
		}
		if ev == nil {
			s.unparsed.Add(1)
			continue
		}
		select {
		case out <- *ev:
		case <-ctx.Done():
			return
		}
	}
	_ = io.EOF // stream end is expected: the pod died or the job finished
}

// parse returns (event, true) for a good reporter line, (nil, true) for a line
// that carried our sentinel but failed to decode, and (nil, false) for
// everything else (PyTorch/NCCL noise).
func parse(line string) (*events.TrainingEvent, bool) {
	i := strings.Index(line, Sentinel)
	if i < 0 {
		return nil, false
	}
	payload := line[i+len(Sentinel):]

	var m struct {
		Kind       string  `json:"kind"`
		GlobalStep int     `json:"global_step"`
		Rank       int     `json:"rank"`
		TS         float64 `json:"ts"`
	}
	if err := json.Unmarshal([]byte(payload), &m); err != nil {
		return nil, true
	}
	return &events.TrainingEvent{
		Kind:       events.Kind(m.Kind),
		GlobalStep: m.GlobalStep,
		Rank:       m.Rank,
		Time:       floatToTime(m.TS),
	}, true
}

// floatToTime converts the reporter's Unix float timestamp.
//
// Reminder: this is the EMITTING process's clock, on a different node. Use it
// for ordering hints and buffering detection only — never subtract two of these
// to measure a duration.
func floatToTime(ts float64) time.Time {
	if ts == 0 {
		return time.Time{}
	}
	sec := int64(ts)
	nsec := int64((ts - float64(sec)) * 1e9)
	return time.Unix(sec, nsec)
}
