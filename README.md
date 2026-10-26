# vllm-drain-plugin

A reference [NVSentinel drain plugin](https://github.com/NVIDIA/NVSentinel/blob/main/docs/tutorials/writing-a-drain-plugin.md)
for vLLM serving pods: when NVSentinel detects a GPU fault and needs to drain a node, this
controller stops routing new requests to the affected vLLM pods, waits for their in-flight
generations (including streaming responses) to finish, and only then evicts — instead of
node-drainer's default behavior of evicting on a fixed grace period with no idea whether vLLM
has active work.

## Description

NVSentinel's node-drainer already has everything needed to coordinate node-level eviction
(PodDisruptionBudgets, termination grace periods, per-pod drain policies). What it doesn't have,
and isn't meant to, is any notion of "is this specific inference server done with its requests."
This plugin fills that gap through NVSentinel's existing `customDrain` extension point: node-drainer
renders a `DrainRequest` CR per health event and polls its `status.conditions` for
`DrainComplete=True`; this controller reconciles that CR and decides when it's actually safe to
say so. **It does not modify node-drainer or any other part of NVSentinel's core.**

On each `DrainRequest`:

1. **Stop routing.** The plugin patches a `nvsentinel.nvidia.com/drain-ready` condition directly onto
   each target pod's `/status` subresource (no sidecar, no `EndpointSlice` edits that a controller
   would just reconcile away) — see [`internal/quiesce/pod.go`](internal/quiesce/pod.go). This only
   removes the pod from a `Service`'s normal routing addresses if the pod's own spec declares it as a
   [readiness gate](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-scheduling-readiness/)
   **at pod creation**:
   ```yaml
   spec:
     readinessGates:
       - conditionType: nvsentinel.nvidia.com/drain-ready
   ```
   **This is a precondition on the vLLM Deployment this plugin targets, not something the plugin
   configures for you.** The controller checks for it (`quiesce.HasReadinessGate`) and reports
   `RoutingConfigured=False` on the CR when a target pod is missing it, so the gap is visible on the
   object rather than a silent no-op. Even correctly configured, this only stops *new* requests
   arriving through ordinary Service routing once the change propagates — it does not terminate a
   client's existing keep-alive connection to the pod, and it does nothing for a custom inference
   gateway or Gateway API implementation that doesn't react to pod readiness. Closing that gap needs
   gateway-specific draining support, which this plugin does not provide.

   > **⚠ Before you declare this readinessGate on anything: Kubernetes treats an *absent* readiness-gate
   > condition as False.** A pod with this gate declared and nothing ever setting the condition to
   > True can **never become Ready at all** — not just during a drain, permanently. This plugin only
   > ever sets the condition to **False**; it does not, and structurally cannot, initialize it to True
   > for every pod in the cluster (it only reconciles `DrainRequest` objects). Your vLLM workload must
   > set `nvsentinel.nvidia.com/drain-ready=True` on itself at startup, or it will never pass readiness.
   > See [`deploy/vllm-workload/readiness-gate-init-example.yaml`](deploy/vllm-workload/readiness-gate-init-example.yaml)
   > for a working initContainer + RBAC reference that does this.
2. **Wait for in-flight work.** The plugin scrapes each pod's vLLM `/metrics` endpoint directly
   (`vllm:num_requests_running` + `vllm:num_requests_waiting`), bypassing Prometheus entirely so
   this never depends on a monitoring stack being installed or on a scrape-interval staleness
   window. A pod is only evicted once activity has read zero for a full confirm window (two poll
   intervals by default), not on a single reading — see
   [`internal/vllmmetrics/client.go`](internal/vllmmetrics/client.go). This narrows, but does not
   close, the race with a request arriving right as routing is pulled (see above). An unreachable pod,
   a non-200 response, a body missing *either* expected metric name (exact match after stripping
   labels, not substring matching), or a sample that's negative, `NaN`, or infinite is always treated
   as **an error, never as zero or partial activity** — `gracefulDrain` reads any scrape failure as
   "still active" and keeps waiting, it never interprets "couldn't find out" as "confirmed idle."
   Both metrics are required specifically so a vLLM version that renames or drops just one of them
   fails the scrape instead of silently reporting that one signal as zero.
3. **Evict, respecting PodDisruptionBudgets**, via the real Eviction API
   (`pods/eviction`), once a pod is confirmed idle.
4. **Force it anyway, deliberately, in two cases:** the quiesce deadline
   (`QUIESCE_DEADLINE`) passes before every pod goes idle, or the triggering health event's own
   `isFatal` flag is true (a fatal fault means there may be nothing left to gracefully finish — see
   "isFatal provenance" below). Both cases bypass the Eviction API and delete directly with an
   explicit, configurable grace period (`FORCE_DELETE_GRACE_PERIOD_SECONDS`, default 0) rather than
   whatever the pod spec's own `terminationGracePeriodSeconds` happens to be — otherwise a "forced,
   bounded" drain could silently run tens of seconds longer than configured. Both cases are recorded
   on the CR's `QuiesceComplete` condition — `DrainComplete=True` on its own never tells you whether
   a request was actually interrupted; check `QuiesceComplete` for that. In both cases, the plugin
   still waits for the pod to actually disappear before reporting `DrainComplete=True`; starting
   deletion is not the same as completion, for either eviction or force-delete. A grace period alone
   doesn't guarantee that disappearance, either: once a pod has sat `Terminating` past
   `STUCK_TERMINATING_TIMEOUT` (default 30s), a finalizer — not the grace period — is almost always
   what's actually holding the object in etcd. **The plugin only logs this by default; it does not
   clear the finalizer unless `CLEAR_STUCK_FINALIZERS=true` is explicitly set.** A finalizer exists to
   guarantee some cleanup completes (a volume detaching, a load balancer deregistering the pod, a
   custom controller's own bookkeeping) before the object is removed, and clearing it skips that
   guarantee rather than honoring it — so this is opt-in, not a default behavior, and should only be
   enabled after auditing what finalizers your vLLM pods actually carry. See `handleStuckTerminating`
   in [`internal/controller/drainrequest_controller.go`](internal/controller/drainrequest_controller.go).

### isFatal provenance — verify before relying on it

The `HealthEvent` proto NVSentinel passes to the `customDrain` template
([`health_event.proto`](https://github.com/NVIDIA/NVSentinel/blob/main/data-models/protobufs/health_event.proto))
has an `isFatal` field, and that's what `DrainRequestSpec.IsFatal` is meant to carry — but the
upstream tutorial's own documented list of optional `DrainRequest` template context (`checkName`,
`recommendedAction`, `errorCode`, `healthEventID`, `entitiesImpacted`, `metadata`, `reason`) does not
mention `isFatal` explicitly. It should still render, since `TemplateData.HealthEvent` is the whole
proto message, not a curated subset — but this hasn't been confirmed against a running node-drainer,
only inferred from the proto definition. **Before depending on `isFatal` in production, confirm it
renders as expected against the `NVSentinel` version you're actually running**, and treat a missing or
unrendered field as `false` (non-fatal) — which is what the Go zero value already gives you by
default, not a change you need to make.

### Native vLLM shutdown: configure this alongside the plugin, not instead of it

vLLM has its own first-party graceful-shutdown support: the `--shutdown-timeout` flag (merged in
[vllm-project/vllm#34730](https://github.com/vllm-project/vllm/pull/34730), default `0` = abort
immediately). On `SIGTERM`, vLLM stops accepting new requests immediately regardless of the flag, and
with `--shutdown-timeout=N` set, waits up to `N` seconds for in-flight requests to finish before the
process exits — exiting early if they finish sooner. Upstream guidance on wiring this into Kubernetes
is still incomplete: an RFC on broader shutdown semantics
([#24885](https://github.com/vllm-project/vllm/issues/24885)) and a docs PR adding this to vLLM's own
Kubernetes deployment guide ([#43208](https://github.com/vllm-project/vllm/pull/43208), approved but
closed stale) didn't land. The [llm-d project](https://llm-d.ai/docs/dev/operations/graceful-shutdown)
documents the concrete pattern this plugin's deployment example follows — see
[`deploy/vllm-workload/readiness-gate-init-example.yaml`](deploy/vllm-workload/readiness-gate-init-example.yaml).

**This plugin does not replace `--shutdown-timeout`; configure both.** The plugin decides *when* it's
safe to start terminating a pod at all (dynamically, from observed `/metrics` activity, and from
NVSentinel's own fault-severity signal) — `--shutdown-timeout` then governs what vLLM itself does
*after* that termination has already started. They operate on different phases of the same timeline,
not as alternatives:

```
Plugin quiesces (QUIESCE_DEADLINE) → Eviction issued → preStop hook → SIGTERM →
vLLM's own --shutdown-timeout window → pod exits → plugin observes it gone → DrainComplete=True
```

**A fatal health event (`isFatal=true`) skips this entire chain.** `forceDrain` deletes the pod
directly with `FORCE_DELETE_GRACE_PERIOD_SECONDS` (default `0`), bypassing preStop, `SIGTERM`
grace, and `--shutdown-timeout` entirely — a fatal GPU fault may mean there is no healthy process left
to gracefully finish anything, regardless of how well the shutdown chain above is configured.

**Sizing `customDrain.timeout` requires the whole chain above, not just this plugin's own settings.**
`complete()` only reports `DrainComplete=True` once the pod has actually disappeared — not once
eviction is merely requested — so for the graceful path the full elapsed time is `QUIESCE_DEADLINE`
(worst case) **plus** the pod's own `terminationGracePeriodSeconds` (which must itself exceed the
preStop delay plus `--shutdown-timeout`, per llm-d's guidance), plus margin for API latency and this
plugin's own poll interval. A worked example with internally consistent numbers:

| Setting | Where it's configured | Example |
|---|---|---|
| Plugin quiesce deadline | this plugin, `QUIESCE_DEADLINE` | 120s |
| Kubernetes `preStop` delay | vLLM pod spec | 10s |
| vLLM `--shutdown-timeout` | vLLM container args | 60s |
| Pod `terminationGracePeriodSeconds` | vLLM pod spec | 90s (> 10s + 60s, with margin) |
| node-drainer `customDrain.timeout` | NVSentinel Helm values | 300s (> 120s + 90s, with margin) |

See [`deploy/node-drainer-wiring/node-drainer-values.yaml`](deploy/node-drainer-wiring/node-drainer-values.yaml)
for this same example wired into actual Helm values.

**Validate this against your actual vLLM version and cluster before relying on it** — the numbers
above are a worked example, not a certified default, and nothing in this reference implementation's
test suite exercises a live vLLM process:

- Confirm `--shutdown-timeout` exists on the vLLM version you're deploying (merged v0.18.0+, but had
  a revert/reapply cycle upstream — check your version's `vllm serve --help`).
- Confirm the container actually receives `SIGTERM` — exec into a test pod during a drain and check
  vLLM's logs for a shutdown message, or that it's still `sh`, not `vllm`, as PID 1 (`ps -p 1`).
- Confirm `terminationGracePeriodSeconds` on the real pod spec, not just the Deployment template
  (`kubectl get pod <pod> -o jsonpath='{.spec.terminationGracePeriodSeconds}'`), actually exceeds your
  chosen preStop delay plus `--shutdown-timeout`.
- Load-test a drain against a pod with a genuinely long-running or streaming request in flight, and
  confirm it completes rather than being cut off — this is the scenario the whole chain exists for.

Finally: none of this closes the gap already noted under "Stop routing" above — a readiness change is
not an instantaneous guarantee against new requests arriving over an existing connection or through a
routing layer that doesn't react to pod readiness. `--shutdown-timeout` and this plugin both reduce how
often that race matters; neither eliminates it.

See [`internal/controller/drainrequest_controller.go`](internal/controller/drainrequest_controller.go)
for the full reconcile logic and [`internal/controller/config.go`](internal/controller/config.go) for
every tunable (`QUIESCE_DEADLINE`, `POLL_INTERVAL`, `CONFIRM_WINDOW`, `VLLM_METRICS_PORT`/`PATH`,
`FORCE_DELETE_GRACE_PERIOD_SECONDS`, `STUCK_TERMINATING_TIMEOUT`, `CLEAR_STUCK_FINALIZERS` — off by
default, see above — `FATAL_ERROR_CODES`).
[`deploy/node-drainer-wiring/`](deploy/node-drainer-wiring/) has the
NVSentinel-side `customDrain` Helm values and drain-template `ConfigMap` needed to point node-drainer
at this plugin in production — see the upstream tutorial's
[§7c](https://github.com/NVIDIA/NVSentinel/blob/main/docs/tutorials/writing-a-drain-plugin.md#7c-verify-the-full-remediation-flow-end-to-end)
for how to drive it end-to-end with a real injected GPU fault.

### Known limitations (reference implementation, not production-hardened)

- **The readiness-gate initializer is a deployment prerequisite, not something this plugin provides.**
  See the warning under "Stop routing" above and
  [`deploy/vllm-workload/readiness-gate-init-example.yaml`](deploy/vllm-workload/readiness-gate-init-example.yaml).
  Skipping it makes every vLLM pod that declares the gate permanently unready, not just during a drain.
- **Existing connections and gateways are out of scope.** Marking a pod not-ready stops new
  Service-routed requests, not traffic over a client's already-open connection or through a
  readiness-agnostic gateway.
- **A drain abandoned mid-flight leaves the pod not-ready.** If a `DrainRequest` stops being reconciled
  toward completion (e.g. node-drainer deletes it without the pod ever being evicted), nothing resets
  `drain-ready` back to True — see the residual-gap note in the deployment example above.
- **`node-drainer`'s own `customDrain.timeout` must cover this plugin's full worst-case runtime,
  including the pod's own shutdown chain** — not just `QUIESCE_DEADLINE` plus the forced-deletion
  path's `FORCE_DELETE_GRACE_PERIOD_SECONDS`. For the graceful path specifically, the dominant term is
  usually the pod's `terminationGracePeriodSeconds` (covering preStop + `SIGTERM` + vLLM's own
  `--shutdown-timeout`), not the forced-deletion grace period. See "Native vLLM shutdown" above for the
  full chain and a worked example, and
  [`deploy/node-drainer-wiring/node-drainer-values.yaml`](deploy/node-drainer-wiring/node-drainer-values.yaml)
  for the same example in Helm values.
- The quiesce deadline currently applies uniformly to every pod in a `DrainRequest`; a mixed batch
  of a 2-second and a 90-second request both drain against the same clock.
- `FATAL_ERROR_CODES` is a flat string set; it doesn't distinguish which GPU a multi-GPU pod actually
  lost versus which codes genuinely mean "nothing more can finish." `isFatal` (see above) is the
  primary signal and doesn't have this problem, but needs its own provenance verified.
- No metrics/observability are emitted by the plugin itself beyond the CR's own status — logs and
  `kubectl get drainrequest -o yaml` are the only visibility today.

### Tracked, not yet implemented

Flagged in review and deliberately left for a follow-up rather than bundled into a single large change:

- `ClearFinalizers` (see `internal/quiesce/pod.go`) removes every finalizer on a stuck pod; an explicit
  allowlist of finalizer names, in addition to the existing `CLEAR_STUCK_FINALIZERS` opt-in, would
  narrow this further.
- The vLLM `/metrics` scrape goes over the pod IP directly; this hasn't been validated against a
  `NetworkPolicy` that would block it, and the client doesn't yet handle IPv6 pod addresses explicitly.
- The readiness-gate initializer example grants `pods/status` patch at the namespace level (see
  [`deploy/vllm-workload/readiness-gate-init-example.yaml`](deploy/vllm-workload/readiness-gate-init-example.yaml)
  for why RBAC can't scope it tighter to "only this pod"); a dedicated controller rather than an
  initContainer would be preferable for stricter environments.
- `Config` (see `internal/controller/config.go`) doesn't reject zero/negative durations or invalid
  port numbers at startup — a misconfigured value currently fails at use rather than at boot.
- The readiness-gate initializer in the deployment example is one-shot; recovering a pod left
  `drain-ready=False` by an abandoned drain (see the limitation above) would need a periodic
  reconciler, not implemented here.

### Test coverage

These are controller/API-level integration and unit tests. They do **not** exercise `EndpointSlice`
or `kube-proxy` routing removal, real `PodDisruptionBudget` admission under load, a Gateway API
implementation, or live vLLM request/streaming behavior — see "Known limitations" and "isFatal
provenance" above for what that leaves unverified.

[`internal/controller/drainrequest_controller_test.go`](internal/controller/drainrequest_controller_test.go)
runs against a real control plane (`envtest`: actual `etcd` + `kube-apiserver`, no mocks — but no
kubelet, endpoint controller, or disruption controller either) and covers: a successful graceful
drain, a quiesce deadline forcing eviction, a fatal health event skipping quiescing entirely, a
simulated controller restart that must not reset the deadline clock, a pod missing the readiness
gate being flagged via `RoutingConfigured=False`, a pod stuck `Terminating` behind a finalizer having
that finalizer cleared once `STUCK_TERMINATING_TIMEOUT` passes with `CLEAR_STUCK_FINALIZERS=true`, and
the same scenario leaving the finalizer untouched with the default `false`. A real `PodDisruptionBudget`
block was deliberately
**not** included here after checking empirically: envtest's apiserver permits eviction against an
unsynced PDB rather than denying it (no disruption controller runs to populate `status`), so a test
built on a real PDB in this environment would pass regardless of whether `quiesce.Evict`'s blocking
logic was even correct.

[`internal/quiesce/pod_test.go`](internal/quiesce/pod_test.go) covers that logic instead, with a fake
client whose interceptors return the actual `TooManyRequests` error a live PDB block produces, plus
the already-gone and generic-error branches, and that `ForceDelete` sends the grace period it was
configured with rather than the pod's own default.

[`internal/vllmmetrics/client_test.go`](internal/vllmmetrics/client_test.go) covers the
Prometheus-text-format parsing: summing across label sets (a pod serving more than one model),
treating an unreachable pod or a malformed body as "unknown" rather than "idle," rejecting a response
missing either expected metric, rejecting negative and non-finite values, and exact-name matching (a
metric whose name merely contains `num_requests_running` as a substring is not mistaken for it).

## Getting Started

### Prerequisites
- go version v1.26+ (matches `go.mod`)
- docker, for building the controller image
- kubectl, matching your cluster's version
- Access to a Kubernetes 1.22+ cluster — this plugin relies on the `policy/v1` Eviction subresource
  (stable since 1.22) and pod readiness gates (stable since 1.14). Tested via `envtest` against 1.37.0;
  see [Test coverage](#test-coverage) for exactly what that does and doesn't verify.

### To Deploy on the cluster

> This deploys the drain-plugin controller itself. It does not by itself wire NVSentinel's
> node-drainer to use it (see [`deploy/node-drainer-wiring/`](deploy/node-drainer-wiring/)) or prepare
> a vLLM workload to be drained by it (see [`deploy/vllm-workload/`](deploy/vllm-workload/), and
> read the readiness-gate warning under "Stop routing" above first).

**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/vllm-drain-plugin:tag
```

**NOTE:** This image ought to be published in the personal registry you specified.
And it is required to have access to pull the image from the working environment.
Make sure you have the proper permission to the registry if the above commands don’t work.

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=<some-registry>/vllm-drain-plugin:tag
```

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin
privileges or be logged in as admin.

**Create instances of your solution**
You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -k config/samples/
```

>**NOTE**: Ensure that the samples has default values to test it out.

### To Uninstall
**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**UnDeploy the controller from the cluster:**

```sh
make undeploy
```

## Project Distribution

Following the options to release and provide this solution to the users.

### By providing a bundle with all YAML files

1. Build the installer for the image built and published in the registry:

```sh
make build-installer IMG=<some-registry>/vllm-drain-plugin:tag
```

**NOTE:** The makefile target mentioned above generates an 'install.yaml'
file in the dist directory. This file contains all the resources built
with Kustomize, which are necessary to install this project without its
dependencies.

2. Using the installer

Users can just run 'kubectl apply -f <URL for YAML BUNDLE>' to install
the project, i.e.:

```sh
kubectl apply -f https://raw.githubusercontent.com/<org>/vllm-drain-plugin/<tag or branch>/dist/install.yaml
```

### By providing a Helm Chart

1. Build the chart using the optional helm plugin

```sh
kubebuilder edit --plugins=helm/v2-alpha
```

2. See that a chart was generated under 'dist/chart', and users
can obtain this solution from there.

**NOTE:** If you change the project, you need to update the Helm Chart
using the same command above to sync the latest changes. Furthermore,
if you create webhooks, you need to use the above command with
the '--force' flag and manually ensure that any custom configuration
previously added to 'dist/chart/values.yaml' or 'dist/chart/manager/manager.yaml'
is manually re-applied afterwards.

## Contributing

Issues and PRs against this repository are welcome. See
[NVIDIA/NVSentinel#2124](https://github.com/NVIDIA/NVSentinel/issues/2124) for the upstream proposal
this reference implementation accompanies.

**NOTE:** Run `make help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

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

