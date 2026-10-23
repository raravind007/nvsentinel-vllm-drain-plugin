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

package vllmmetrics

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

func serve(t *testing.T, body string) (host string, port int) {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)

	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}

	p, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatalf("parse test server port: %v", err)
	}

	return u.Hostname(), p
}

func TestFetchSumsAcrossLabelSets(t *testing.T) {
	// A pod serving more than one model reports one series per model; a real
	// deployment is idle only once every series is, so Fetch must sum them
	// rather than read a single arbitrary line.
	body := strings.Join([]string{
		`# HELP vllm:num_requests_running Number of requests currently running`,
		`# TYPE vllm:num_requests_running gauge`,
		`vllm:num_requests_running{model_name="a"} 2`,
		`vllm:num_requests_running{model_name="b"} 1`,
		`vllm:num_requests_waiting{model_name="a"} 0`,
		`vllm:num_requests_waiting{model_name="b"} 3`,
	}, "\n")

	host, port := serve(t, body)

	activity, err := NewClient(port, "/metrics", time.Second).Fetch(context.Background(), host)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if activity.Running != 3 {
		t.Errorf("Running = %v, want 3", activity.Running)
	}

	if activity.Waiting != 3 {
		t.Errorf("Waiting = %v, want 3", activity.Waiting)
	}

	if activity.Idle() {
		t.Error("Idle() = true, want false")
	}
}

func TestFetchIdleWhenEverySeriesIsZero(t *testing.T) {
	body := "vllm:num_requests_running 0\nvllm:num_requests_waiting 0\n"

	host, port := serve(t, body)

	activity, err := NewClient(port, "/metrics", time.Second).Fetch(context.Background(), host)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	if !activity.Idle() {
		t.Errorf("Idle() = false, want true (got %+v)", activity)
	}
}

func TestFetchMalformedBodyIsAnErrorNotIdle(t *testing.T) {
	// An HTML error page, a truncated response, or the wrong endpoint entirely
	// all look like this: no recognized metric name anywhere in the body. This
	// must not be read as "confirmed idle" — confusing "we don't know" with
	// "definitely zero" is exactly the failure mode this test guards against.
	host, port := serve(t, "<html><body>502 Bad Gateway</body></html>")

	_, err := NewClient(port, "/metrics", time.Second).Fetch(context.Background(), host)
	if err == nil {
		t.Fatal("Fetch succeeded against a body with no recognized metrics, want an error")
	}
}

func TestFetchUnreachablePodIsAnError(t *testing.T) {
	// Port 1 on loopback: nothing listens there, standing in for a pod that
	// has gone away or hasn't opened its metrics port yet. Callers must treat
	// this as "activity unknown", not as "idle".
	_, err := NewClient(1, "/metrics", 50*time.Millisecond).Fetch(context.Background(), "127.0.0.1")
	if err == nil {
		t.Fatal("Fetch succeeded against an unreachable address, want an error")
	}
}
