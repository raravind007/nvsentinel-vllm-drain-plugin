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

package controller

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds the plugin's runtime tuning. All of it is a reference
// heuristic: a real deployment is expected to adjust these for its own vLLM
// version, scrape cost and fault taxonomy rather than take the defaults as
// correct for every cluster.
type Config struct {
	// MetricsPort/MetricsPath locate each target pod's vLLM /metrics endpoint.
	// Scraped directly over the pod IP, not through a Prometheus server, so
	// quiescing never depends on a monitoring stack being installed.
	MetricsPort int
	MetricsPath string
	// MetricsTimeout bounds a single scrape so one unreachable pod can't stall
	// the reconcile loop.
	MetricsTimeout time.Duration

	// QuiesceDeadline is how long the plugin waits for in-flight requests to
	// reach zero before forcing eviction regardless of activity. Anchored to
	// DrainRequest.Status.DrainStartTime, not wall-clock-since-process-start,
	// so a controller restart neither resets nor extends it.
	QuiesceDeadline time.Duration
	// PollInterval is both the reconcile requeue cadence while waiting and the
	// minimum spacing between the two consecutive zero-activity readings
	// ConfirmWindow requires.
	PollInterval time.Duration
	// ConfirmWindow is how long a pod's activity must have continuously read
	// zero before it is evicted. A single zero reading can race with a
	// request that was already admitted just before routing was pulled, so
	// this debounces that instead of trusting one sample.
	ConfirmWindow time.Duration

	// ForceDeleteGracePeriodSeconds bounds how long a forced deletion (deadline
	// exceeded, or a fatal fault) itself takes, overriding whatever
	// terminationGracePeriodSeconds the pod spec declares. Zero means an
	// immediate SIGKILL — appropriate once the plugin has already decided not
	// to wait any further, but configurable since always skipping the target's
	// own shutdown hooks is not free (e.g. an in-progress checkpoint write).
	ForceDeleteGracePeriodSeconds int64

	// StuckTerminatingTimeout bounds how long a pod may sit with a
	// DeletionTimestamp set (from either an eviction or a forced delete) before
	// the plugin considers it stuck. A deletion grace period alone does not
	// guarantee a pod disappears within it — a finalizer holds the object in
	// etcd until whatever set it removes it, which may never happen if that
	// controller also needed the node being drained. Only acted on when
	// ClearStuckFinalizers is also true; otherwise this only governs when a
	// "stuck" warning is logged.
	StuckTerminatingTimeout time.Duration

	// ClearStuckFinalizers, when true, removes every finalizer from a pod stuck
	// Terminating past StuckTerminatingTimeout, to unblock a deletion that
	// would otherwise wait forever.
	//
	// Off by default, deliberately: a finalizer exists to guarantee some
	// cleanup completes first — a volume detaching, a load balancer
	// deregistering the pod, a custom controller's own bookkeeping — and
	// clearing it skips that guarantee without running it. Enabling this
	// trades "the drain might never finish" for "cleanup this finalizer was
	// protecting might not happen." Only turn it on after checking what
	// finalizers your vLLM pods actually carry and confirming that trade is
	// acceptable for your environment; it is not a safe universal default.
	ClearStuckFinalizers bool

	// FatalErrorCodes is a secondary override on top of DrainRequest.Spec.IsFatal
	// (NVSentinel's own classification, the primary signal): error codes listed
	// here also skip graceful quiescing even when isFatal was false upstream.
	// Useful for codes an operator has learned are effectively unrecoverable for
	// a specific vLLM deployment without changing NVSentinel's own severity
	// classification.
	FatalErrorCodes map[string]bool
}

func LoadConfig() (Config, error) {
	cfg := Config{
		MetricsPort:             8000,
		MetricsPath:             "/metrics",
		MetricsTimeout:          3 * time.Second,
		QuiesceDeadline:         120 * time.Second,
		PollInterval:            5 * time.Second,
		StuckTerminatingTimeout: 30 * time.Second,
		ClearStuckFinalizers:    false,
		FatalErrorCodes:         map[string]bool{},
	}

	var err error

	if cfg.MetricsPort, err = envInt("VLLM_METRICS_PORT", cfg.MetricsPort); err != nil {
		return Config{}, err
	}

	if v := os.Getenv("VLLM_METRICS_PATH"); v != "" {
		cfg.MetricsPath = v
	}

	if cfg.MetricsTimeout, err = envDuration("VLLM_METRICS_TIMEOUT", cfg.MetricsTimeout); err != nil {
		return Config{}, err
	}

	if cfg.QuiesceDeadline, err = envDuration("QUIESCE_DEADLINE", cfg.QuiesceDeadline); err != nil {
		return Config{}, err
	}

	if cfg.PollInterval, err = envDuration("POLL_INTERVAL", cfg.PollInterval); err != nil {
		return Config{}, err
	}

	forceDeleteGracePeriod, err := envInt("FORCE_DELETE_GRACE_PERIOD_SECONDS", 0)
	if err != nil {
		return Config{}, err
	}

	cfg.ForceDeleteGracePeriodSeconds = int64(forceDeleteGracePeriod)

	if cfg.StuckTerminatingTimeout, err = envDuration("STUCK_TERMINATING_TIMEOUT", cfg.StuckTerminatingTimeout); err != nil {
		return Config{}, err
	}

	if cfg.ClearStuckFinalizers, err = envBool("CLEAR_STUCK_FINALIZERS", cfg.ClearStuckFinalizers); err != nil {
		return Config{}, err
	}

	// Two poll intervals by default: the earliest a reading can be confirmed
	// is the next reconcile after it was first observed at zero.
	cfg.ConfirmWindow = 2 * cfg.PollInterval

	if cfg.ConfirmWindow, err = envDuration("CONFIRM_WINDOW", cfg.ConfirmWindow); err != nil {
		return Config{}, err
	}

	for code := range strings.SplitSeq(os.Getenv("FATAL_ERROR_CODES"), ",") {
		code = strings.TrimSpace(code)
		if code != "" {
			cfg.FatalErrorCodes[code] = true
		}
	}

	return cfg, nil
}

func envInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}

	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", key, v, err)
	}

	return n, nil
}

func envBool(key string, def bool) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}

	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("invalid %s %q: %w", key, v, err)
	}

	return b, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}

	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", key, v, err)
	}

	return d, nil
}
