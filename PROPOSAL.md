# Proposal: Reference vLLM Inference-Aware Drain Plugin (using the existing `customDrain` extension point)

> Drafted for an upstream GitHub issue against [NVIDIA/NVSentinel](https://github.com/NVIDIA/NVSentinel).
> Replace the `[link]` placeholder below with this repository's public URL before posting.

## Motivation

When NVSentinel drains a node running a vLLM serving pod, Kubernetes-level eviction and termination
grace periods don't know whether vLLM has active generations in flight — including streaming
responses. A drain can interrupt a long-running or streaming request that would have finished in
seconds.

NVSentinel already solves the hard part of this generically: `customDrain` + the `DrainRequest` CRD
contract (documented in
[`docs/tutorials/writing-a-drain-plugin.md`](https://github.com/NVIDIA/NVSentinel/blob/main/docs/tutorials/writing-a-drain-plugin.md))
let node-drainer delegate the actual draining decision to an external controller, polling
`status.conditions[DrainComplete]` with a bounded timeout. **This proposal does not modify
node-drainer or any other part of NVSentinel's core** — it's a reference plugin for that existing
extension point, specifically for vLLM, that could serve as a template for other inference servers
(Triton, SGLang).

## Design

Working reference implementation: `[link]`. On each `DrainRequest`, the plugin:

1. **Stops routing new requests** by patching a `nvsentinel.nvidia.com/drain-ready` condition directly
   onto the pod's `/status` subresource. This relies on the pod declaring it as a
   [readiness gate](https://kubernetes.io/docs/concepts/scheduling-eviction/pod-scheduling-readiness/)
   **at pod creation** — no `EndpointSlice` edits, no sidecar. The controller checks for the declared
   gate and reports a `RoutingConfigured=False` condition on the CR when a target pod is missing it,
   so a misconfigured workload is visible rather than silently ineffective.

   **Readiness-gate initialization prerequisite.** Kubernetes treats an *absent* readiness-gate
   condition as `False`. A pod that declares this gate and has nothing ever set it to `True` can
   **never become Ready at all** — not just during a drain, permanently. This plugin only ever sets
   the condition to `False`; by design, it reconciles `DrainRequest` objects, not every pod in the
   cluster, so it cannot also be the thing that initializes every vLLM pod to Ready at startup. The
   target workload must set `nvsentinel.nvidia.com/drain-ready=True` on itself when it starts — the
   reference implementation includes a worked initContainer + RBAC example for this
   (`deploy/vllm-workload/readiness-gate-init-example.yaml`). Any adopter of this pattern needs an
   equivalent; it is not optional plumbing.

   **Known limitation:** even correctly initialized, this only stops new requests through ordinary
   Service routing once the change propagates. It does not terminate a client's existing keep-alive
   connection, and it does nothing for a custom inference gateway or Gateway API implementation that
   doesn't react to pod readiness. Closing that gap needs gateway-specific draining support, which
   this plugin doesn't provide.
2. **Waits for in-flight generations to finish** by scraping each pod's vLLM `/metrics` directly over
   its pod IP, rather than through Prometheus. A pod is only evicted once activity reads zero across a
   confirm window — this narrows, but does not close, the race with a request admitted just before
   routing was pulled. A scrape that fails, times out, or returns a body with neither recognized
   metric name in it is always treated as "still active," never as zero.
3. **Evicts through the real Eviction API**, respecting a `PodDisruptionBudget`.
4. **Forces eviction** — bypassing the Eviction API, with an explicit configurable grace period rather
   than the pod's own default, so "forced" actually bounds elapsed time — in two cases: the quiesce
   deadline passes, or the triggering `HealthEvent.isFatal` is true. Both cases are recorded in a
   separate `QuiesceComplete` condition, and in both the plugin still waits for the pod to actually
   disappear before reporting `DrainComplete=True` — starting deletion isn't completion.
5. **Clears finalizers on a pod stuck `Terminating`, but only when explicitly enabled.** A grace period
   alone doesn't guarantee a pod disappears — a finalizer, not the grace period, is what actually holds
   a deleted object in etcd, and one can be left behind by a controller that stopped reconciling
   (plausibly the very node being drained). `CLEAR_STUCK_FINALIZERS` defaults to `false`: a finalizer
   exists to guarantee some cleanup completes first, and clearing it skips that guarantee rather than
   honoring it. This is deliberately an opt-in trade — "the drain might never finish" versus "cleanup
   this finalizer was protecting might not happen" — not a default behavior, and reviewers should treat
   a reference implementation that defaulted this to `true` as a red flag.

**On `isFatal` provenance:** `HealthEvent.isFatal` exists on the proto NVSentinel passes to the
`customDrain` template, but the tutorial's own documented list of optional template context doesn't
enumerate it explicitly. It should render regardless, since the full `HealthEvent` message is
available to the template — but this is inferred from the proto definition, not confirmed against a
running node-drainer. Flagging this for maintainer input: **is `isFatal` guaranteed to render via
`.HealthEvent.IsFatal` in the drain-template, or is there a version/compatibility concern we should
design around?**

**On `customDrain.timeout` interaction:** node-drainer's own timeout must be configured larger than
this plugin's worst-case runtime (quiesce deadline + forced-deletion grace period + margin), or
node-drainer could give up waiting before the plugin's forced-drain fallback finishes. Documented in
the reference wiring, but worth calling out as a general hazard for any drain plugin with its own
internal deadline.

## Validation

These are controller/API-level integration tests against a real control plane (`envtest`: real `etcd`
+ `kube-apiserver`), not full inference end-to-end tests — they do not exercise `EndpointSlice`/
`kube-proxy` routing removal, `PodDisruptionBudget` admission under a real disruption controller, a
Gateway API implementation, or live vLLM behavior. (Concretely: envtest runs no disruption controller,
so a real PDB's status never syncs there, and this apiserver version was empirically found to permit
eviction against an unsynced PDB rather than deny it — so the PDB-blocks-eviction code path is
unit-tested against a fake client instead, not claimed as cluster-verified.) Scenarios covered
end-to-end at the controller level: a successful graceful drain, a deadline forcing eviction, a fatal
health event skipping quiescing entirely, a simulated controller restart that doesn't reset the
deadline, a pod missing the readiness gate being flagged, and a pod stuck `Terminating` behind a
finalizer both with and without `CLEAR_STUCK_FINALIZERS` enabled.

## Planned validation (not yet done)

The most convincing evidence for this approach would be a direct comparison: same workload, same
injected fault, same drain deadline, measuring the number of in-flight requests that fail under
NVSentinel's built-in `AllowCompletion` eviction versus under this plugin. That comparison doesn't
exist yet — flagging it as the next step before (or alongside) wider review, since it would turn "this
should reduce interruptions" into a measured number.

## Questions for maintainers

1. Would you be open to linking a plugin like this from `docs/tutorials/` or an `examples/` directory,
   as a second worked example of `writing-a-drain-plugin.md`?
2. Can you confirm whether `HealthEvent.isFatal` is reliably available to the `customDrain` template
   across NVSentinel versions (see provenance note above)?
3. Is there a preferred home for this (a `contrib/` directory in this repo, a separate
   NVIDIA-maintained repo, or community-maintained outside the org)?

*(Checked for prior art: no existing GitHub issues/discussions mention vLLM or inference-aware
draining in this repo, though that doesn't rule out overlapping work in progress.)*
