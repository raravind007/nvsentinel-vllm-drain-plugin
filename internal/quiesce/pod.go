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

// Package quiesce pulls a pod out of Service routing and evicts it, without
// fighting any controller that also manages the pod.
package quiesce

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// DrainReadyConditionType is the pod condition this plugin writes directly to
// pod.status.conditions. It never touches the Service's EndpointSlice itself:
// the endpoint controller already recomputes Ready addresses from each pod's
// readiness, and EndpointSlice writes from anything else get reconciled away.
//
// For this condition to actually pull a pod out of a Service, the pod's own
// spec must declare it as a readiness gate:
//
//	spec:
//	  readinessGates:
//	    - conditionType: nvsentinel.nvidia.com/drain-ready
//
// Kubelet then ANDs this condition into the pod's overall Ready status
// without otherwise managing it, which is exactly the hook an external
// controller is meant to use — no sidecar or probe changes required. A pod
// that doesn't declare the gate keeps receiving traffic until it is deleted;
// document this prerequisite for any workload this plugin is pointed at.
const DrainReadyConditionType = "nvsentinel.nvidia.com/drain-ready"

// HasReadinessGate reports whether the pod's own spec declares
// DrainReadyConditionType as a readiness gate — the precondition for
// MarkNotReady to have any effect on routing at all. Callers should surface
// this check's result to the operator (e.g. as a status condition), not just
// log it, since a pod missing the gate silently keeps receiving new requests
// for the rest of the drain with no other visible symptom.
func HasReadinessGate(pod *corev1.Pod) bool {
	for _, gate := range pod.Spec.ReadinessGates {
		if string(gate.ConditionType) == DrainReadyConditionType {
			return true
		}
	}

	return false
}

// MarkNotReady sets DrainReadyConditionType to False on the pod. Kubelet ANDs
// every declared readiness gate's condition into the pod's overall Ready
// status (https://kubernetes.io/docs/concepts/scheduling-eviction/pod-scheduling-readiness/),
// and the endpoint controller excludes non-Ready pods from a Service's normal
// routing addresses once that change propagates — but only for a pod whose
// own spec lists DrainReadyConditionType in readinessGates; see
// HasReadinessGate. This plugin's envtest suite cannot exercise that
// propagation (no kubelet or endpoint controller runs there); it is standard,
// documented Kubernetes behavior but has not been verified against a real
// cluster here — do that before relying on it.
//
// Even on a pod with the gate correctly declared, this stops new requests
// arriving through ordinary Service routing only. It does nothing about
// requests sent over a client's existing keep-alive connection to this pod,
// or routed through a gateway/load balancer that doesn't react to pod
// readiness (e.g. most Gateway API implementations and custom inference
// gateways) — closing that gap needs gateway-specific draining support, which
// this plugin does not provide.
//
// Idempotent: setting the same condition value twice is a no-op API call.
func MarkNotReady(ctx context.Context, c client.Client, pod *corev1.Pod, reason, message string) error {
	existing := meta.FindStatusCondition(toMetaConditions(pod.Status.Conditions), DrainReadyConditionType)
	if existing != nil && existing.Status == metav1.ConditionFalse && existing.Reason == reason {
		return nil
	}

	patch := client.MergeFrom(pod.DeepCopy())
	setPodCondition(pod, metav1.Condition{
		Type:    DrainReadyConditionType,
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: message,
	})

	if err := c.Status().Patch(ctx, pod, patch); err != nil {
		return fmt.Errorf("patch pod %s/%s not-ready: %w", pod.Namespace, pod.Name, err)
	}

	return nil
}

// evictionSubResource is the Kubernetes pod subresource name for the
// Eviction API, shared with this package's tests exercising it via a fake
// client's SubResourceCreate interceptor.
const evictionSubResource = "eviction"

// Evict requests graceful termination through the Eviction API, so a
// PodDisruptionBudget protecting the workload is honored. Returns
// (evicted=false, nil) when a PDB currently blocks the eviction — distinct
// from an error, since the caller should keep requeuing rather than treat
// this as a failure.
func Evict(ctx context.Context, c client.Client, pod *corev1.Pod) (evicted bool, err error) {
	eviction := &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name,
			Namespace: pod.Namespace,
		},
	}

	err = c.SubResource(evictionSubResource).Create(ctx, pod, eviction)

	switch {
	case err == nil:
		return true, nil
	case apierrors.IsTooManyRequests(err), apierrors.IsConflict(err):
		// A PodDisruptionBudget (or a concurrent eviction) is blocking progress
		// right now; this is expected back-pressure within the deadline, not a
		// reconcile failure.
		return false, nil
	case apierrors.IsNotFound(err):
		return true, nil // already gone
	default:
		return false, fmt.Errorf("evict pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
}

// ForceDelete bypasses the Eviction API — and therefore any
// PodDisruptionBudget — to guarantee the node-drainer-facing deadline is
// honored. Mirrors node-drainer's own DeleteAfterTimeout semantics: only
// called once the configured quiesce deadline has already passed, or for a
// fault classified as fatal.
//
// gracePeriodSeconds is passed explicitly rather than left to the pod's own
// spec.terminationGracePeriodSeconds: "force" is meant to bound total elapsed
// time, and a vLLM pod's own grace period (commonly tens of seconds, to let
// it shut down cleanly) would otherwise silently extend well past the
// deadline this is supposed to enforce. The caller must still wait for the
// pod to actually disappear before treating the drain as complete — this
// call only starts termination, exactly like the Eviction API does.
func ForceDelete(ctx context.Context, c client.Client, pod *corev1.Pod, gracePeriodSeconds int64) error {
	err := c.Delete(ctx, pod, client.GracePeriodSeconds(gracePeriodSeconds))
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("force delete pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}

	return nil
}

// ClearFinalizers removes every finalizer from a pod, unblocking a deletion
// that would otherwise wait forever for one that will never fire — plausible
// on the very node being drained, if the finalizer's owning controller also
// ran there, or stopped reconciling for any other reason. A grace period
// alone does not guarantee a pod disappears: once DeletionTimestamp is set, a
// finalizer is what actually still holds the object in etcd.
//
// Only call this after a pod has been Terminating for longer than is normal
// (see the reconciler's stuck-pod handling) — clearing a finalizer skips
// whatever cleanup it existed to guarantee. This still cannot fix every way a
// pod can get stuck: on a genuinely unresponsive node, kubelet may never
// acknowledge a deletion at all regardless of finalizers, a limitation
// NVSentinel's own node-drainer has no more of a way around than this does.
//
// Returns cleared=false when the pod already had no finalizers, so the caller
// can distinguish "nothing to do" from "did something" for logging.
func ClearFinalizers(ctx context.Context, c client.Client, pod *corev1.Pod) (cleared bool, err error) {
	if len(pod.Finalizers) == 0 {
		return false, nil
	}

	patch := client.MergeFrom(pod.DeepCopy())
	pod.Finalizers = nil

	if err := c.Patch(ctx, pod, patch); err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("clear finalizers on pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}

	return true, nil
}

func setPodCondition(pod *corev1.Pod, cond metav1.Condition) {
	conditions := toMetaConditions(pod.Status.Conditions)
	meta.SetStatusCondition(&conditions, cond)
	pod.Status.Conditions = toPodConditions(pod.Status.Conditions, conditions)
}

// toMetaConditions projects corev1.PodCondition onto metav1.Condition so
// k8s.io/apimachinery/pkg/api/meta's condition helpers can be reused; only
// the fields those helpers read or write round-trip.
func toMetaConditions(in []corev1.PodCondition) []metav1.Condition {
	out := make([]metav1.Condition, 0, len(in))

	for _, c := range in {
		out = append(out, metav1.Condition{
			Type:               string(c.Type),
			Status:             metav1.ConditionStatus(c.Status),
			Reason:             orUnknown(c.Reason),
			Message:            c.Message,
			LastTransitionTime: c.LastTransitionTime,
		})
	}

	return out
}

// toPodConditions merges meta-condition results back into the pod's
// condition list, preserving every condition kubelet and other controllers
// already set and only touching entries meta.SetStatusCondition changed.
func toPodConditions(original []corev1.PodCondition, updated []metav1.Condition) []corev1.PodCondition {
	byType := make(map[corev1.PodConditionType]corev1.PodCondition, len(original))
	for _, c := range original {
		byType[c.Type] = c
	}

	out := make([]corev1.PodCondition, 0, len(updated))

	for _, c := range updated {
		t := corev1.PodConditionType(c.Type)
		if existing, ok := byType[t]; ok && existing.Status == corev1.ConditionStatus(c.Status) &&
			existing.Reason == c.Reason {
			out = append(out, existing) // unchanged: keep kubelet's original transition time untouched
			continue
		}

		out = append(out, corev1.PodCondition{
			Type:               t,
			Status:             corev1.ConditionStatus(c.Status),
			Reason:             c.Reason,
			Message:            c.Message,
			LastTransitionTime: c.LastTransitionTime,
		})
	}

	return out
}

func orUnknown(reason string) string {
	if reason == "" {
		return "Unknown"
	}

	return reason
}
