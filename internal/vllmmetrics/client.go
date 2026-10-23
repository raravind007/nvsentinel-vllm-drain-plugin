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
// An unreachable pod, a non-200 response, or a body with neither recognized
// metric name in it (empty, truncated, an HTML error page, or a vLLM version
// that renamed both) all return an error rather than a zero Activity — the
// caller must not treat a failed or malformed scrape as "idle": see
// drainrequest_controller.go's gracefulDrain, which treats any Fetch error as
// "still active," never as evidence of zero in-flight requests. This cannot
// catch every malformed-data case: if only one of the two metric names is
// recognized in the response (e.g. a future vLLM version renames just one),
// Fetch still succeeds and silently reports that one signal as zero.
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

// Prometheus exposition format: "metric_name{label=\"value\",...} 1.0" or
// "metric_name 1.0", one sample per line; comment lines start with "#".
// vLLM renamed its metrics across versions (e.g. a "vllm:" prefix was added,
// then versions also shipped without it) so both are matched.
const (
	runningMetric = "num_requests_running"
	waitingMetric = "num_requests_waiting"
)

func parse(body io.Reader) (Activity, error) {
	var activity Activity

	foundAnySample := false

	scanner := bufio.NewScanner(body)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		name, value, ok := splitSample(line)
		if !ok {
			continue
		}

		switch {
		case strings.Contains(name, runningMetric):
			activity.Running += value
			foundAnySample = true
		case strings.Contains(name, waitingMetric):
			activity.Waiting += value
			foundAnySample = true
		}
	}

	if err := scanner.Err(); err != nil {
		return Activity{}, fmt.Errorf("parse metrics: %w", err)
	}

	// A response with neither metric name in it is far more likely malformed,
	// truncated, or from the wrong endpoint than a vLLM instance that is
	// genuinely and simultaneously idle on both signals forever — callers must
	// not read a zero-value Activity here as "confirmed idle."
	if !foundAnySample {
		return Activity{}, fmt.Errorf("no %s or %s samples found in response: "+
			"malformed body, wrong endpoint, or an unrecognized vLLM metrics format", runningMetric, waitingMetric)
	}

	return activity, nil
}

// splitSample splits a single exposition-format line into its metric name
// (including any label block, so callers can substring-match on the base
// name) and its trailing float value.
func splitSample(line string) (name string, value float64, ok bool) {
	lastSpace := strings.LastIndexByte(line, ' ')
	if lastSpace < 0 {
		return "", 0, false
	}

	v, err := strconv.ParseFloat(line[lastSpace+1:], 64)
	if err != nil {
		return "", 0, false
	}

	return line[:lastSpace], v, true
}
