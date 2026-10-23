/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package vllmmetrics reads a vLLM pod's in-flight request count directly from
// its Prometheus /metrics endpoint. The controller scrapes each target pod by
// IP rather than going through a Prometheus server, so quiescing doesn't
// depend on a monitoring stack being installed or on a scrape interval making
// the reading stale by more than one HTTP round trip.
package vllmmetrics

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Activity is the in-flight request count vLLM reports for one pod.
type Activity struct {
	// Running is vllm:num_requests_running: requests currently generating
	// tokens, including active streaming responses.
	Running float64
	// Waiting is vllm:num_requests_waiting: requests admitted but not yet
	// scheduled. A pod pulled from routing can still have these drain down
	// on their own; they count as in-flight until they do.
	Waiting float64
}

// Idle reports whether neither running nor waiting requests remain.
func (a Activity) Idle() bool {
	return a.Running == 0 && a.Waiting == 0
}

// Client scrapes a single vLLM instance's /metrics endpoint over HTTP.
type Client struct {
	HTTPClient *http.Client
	Port       int
	Path       string
}

// NewClient returns a Client with a bounded per-request timeout. Scrapes are
// short-lived and run once per reconcile, so a generous shared client timeout
// would otherwise let one unreachable pod stall the whole reconcile loop.
func NewClient(port int, path string, timeout time.Duration) *Client {
	return &Client{
		HTTPClient: &http.Client{Timeout: timeout},
		Port:       port,
		Path:       path,
	}
}

// Fetch scrapes podIP's /metrics endpoint and sums the running/waiting gauges
// across every label combination vLLM exposes (e.g. per served model), since
// a pod serving more than one model would otherwise only report one series.
//
// This fails closed: an unreachable pod, a non-200 response, a body missing
// either expected metric name, or a sample whose value is negative, NaN, or
// infinite all return an error rather than a zero or partial Activity. The
// caller must not treat a failed or rejected scrape as "idle" — see
// drainrequest_controller.go's gracefulDrain, which treats any Fetch error as
// "still active," never as evidence of zero in-flight requests. Requiring
// both metrics (rather than either one) specifically closes the gap where a
// vLLM version renames or drops just one of them: that used to silently read
// as that signal being zero, and now fails the scrape instead.
func (c *Client) Fetch(ctx context.Context, podIP string) (Activity, error) {
	url := fmt.Sprintf("http://%s:%d%s", podIP, c.Port, c.Path)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Activity{}, fmt.Errorf("build metrics request: %w", err)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return Activity{}, fmt.Errorf("scrape %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return Activity{}, fmt.Errorf("scrape %s: unexpected status %d", url, resp.StatusCode)
	}

	return parse(resp.Body)
}

// vLLM renamed its metrics across versions (e.g. a "vllm:" prefix was added,
// then versions also shipped without it), so each signal accepts either exact
// name — exact equality against the metric name with any label block
// stripped, not a substring match, so an unrelated metric that merely
// contains "num_requests_running" as part of a longer name is never mistaken
// for it.
var (
	runningMetricNames = map[string]bool{"vllm:num_requests_running": true, "num_requests_running": true}
	waitingMetricNames = map[string]bool{"vllm:num_requests_waiting": true, "num_requests_waiting": true}
)

func parse(body io.Reader) (Activity, error) {
	var activity Activity

	var foundRunning, foundWaiting bool

	scanner := bufio.NewScanner(body)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		name, valueText, ok := splitSample(line)
		if !ok {
			continue
		}

		base := baseMetricName(name)

		var target *float64

		switch {
		case runningMetricNames[base]:
			target, foundRunning = &activity.Running, true
		case waitingMetricNames[base]:
			target, foundWaiting = &activity.Waiting, true
		default:
			continue // not a metric this package reads
		}

		// Only a line that claims to BE one of our two metrics is validated
		// this strictly — a malformed value here is treated as a hard failure
		// rather than silently skipped, since skipping it would otherwise
		// silently undercount exactly the signal the caller depends on.
		value, err := strconv.ParseFloat(valueText, 64)
		if err != nil {
			return Activity{}, fmt.Errorf("%s: value %q is not a number: %w", base, valueText, err)
		}

		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return Activity{}, fmt.Errorf("%s reported %v: must be a finite, non-negative number", base, value)
		}

		*target += value
	}

	if err := scanner.Err(); err != nil {
		return Activity{}, fmt.Errorf("parse metrics: %w", err)
	}

	switch {
	case !foundRunning && !foundWaiting:
		return Activity{}, fmt.Errorf(
			"neither num_requests_running nor num_requests_waiting found in response: "+
				"malformed body, wrong endpoint, or an unrecognized vLLM metrics format (checked names: %v, %v)",
			keys(runningMetricNames), keys(waitingMetricNames))
	case !foundRunning:
		return Activity{}, fmt.Errorf(
			"num_requests_running not found in response though num_requests_waiting was: "+
				"refusing to treat the missing signal as zero (checked names: %v)", keys(runningMetricNames))
	case !foundWaiting:
		return Activity{}, fmt.Errorf(
			"num_requests_waiting not found in response though num_requests_running was: "+
				"refusing to treat the missing signal as zero (checked names: %v)", keys(waitingMetricNames))
	}

	return activity, nil
}

// splitSample splits a single exposition-format line into its metric name
// (including any label block) and the raw text of its trailing value,
// without parsing that value yet — callers decide how strictly to validate it
// depending on whether the name matched something they care about.
func splitSample(line string) (name string, valueText string, ok bool) {
	lastSpace := strings.LastIndexByte(line, ' ')
	if lastSpace < 0 {
		return "", "", false
	}

	return line[:lastSpace], line[lastSpace+1:], true
}

// baseMetricName strips a Prometheus label block ("{...}") off a metric name,
// so "vllm:num_requests_running{model_name=\"a\"}" compares equal to
// "vllm:num_requests_running".
func baseMetricName(name string) string {
	if i := strings.IndexByte(name, '{'); i >= 0 {
		return name[:i]
	}

	return name
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	return out
}
