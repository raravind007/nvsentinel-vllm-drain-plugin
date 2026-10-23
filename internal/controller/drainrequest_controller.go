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
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	nvsentinelv1alpha1 "github.com/raravind007/nvsentinel-vllm-drain-plugin/api/v1alpha1"
	"github.com/raravind007/nvsentinel-vllm-drain-plugin/internal/quiesce"
	"github.com/raravind007/nvsentinel-vllm-drain-plugin/internal/vllmmetrics"
)

// node-drainer polls for this exact condition; it MUST match the
// statusConditionType/statusConditionStatus configured in node-drainer's
// customDrain block (see docs/tutorials/writing-a-drain-plugin.md upstream).
const drainCompleteConditionType = "DrainComplete"

// quiesceCompleteConditionType is this plugin's own signal, not part of
// node-drainer's contract: True only when every pod reached zero in-flight
// requests before eviction. False means the deadline (or a fatal fault)
// forced eviction, so DrainComplete=True must never be read as "no request
// was interrupted" on its own — check this condition too.
const quiesceCompleteConditionType = "QuiesceComplete"

// routingConfiguredConditionType is this plugin's own signal that every
// target pod actually declares the readiness gate MarkNotReady depends on.
// False means "routing removal almost certainly silently did nothing" for at
// least one pod — a real misconfiguration the operator needs to know about,
// not just a log line. See quiesce.HasReadinessGate.
const routingConfiguredConditionType = "RoutingConfigured"

// DrainRequestReconciler reconciles a DrainRequest object, coordinating
// vLLM-aware draining: stop routing new requests to the target pods, wait for
// in-flight generations to finish (bounded by Config.QuiesceDeadline, or
// skipped outright for faults in Config.FatalErrorCodes), then evict.
type DrainRequestReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Config Config
}

// +kubebuilder:rbac:groups=nvsentinel.nvidia.com,resources=drainrequests,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=nvsentinel.nvidia.com,resources=drainrequests/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete;patch
// +kubebuilder:rbac:groups="",resources=pods/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods/eviction,verbs=create

func (r *DrainRequestReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	dr := &nvsentinelv1alpha1.DrainRequest{}
	if err := r.Get(ctx, req.NamespacedName, dr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Idempotent: once we've reported completion there is nothing left to do.
	if meta.IsStatusConditionTrue(dr.Status.Conditions, drainCompleteConditionType) {
		return ctrl.Result{}, nil
	}

	// Set once, never updated again, and persisted in the same Status().Update
	// call every path below already makes — so a crash before that update just
	// means the clock hasn't started yet, and a restart after it cannot reset
	// or extend an already-running deadline.
	if dr.Status.DrainStartTime == nil {
		now := metav1.Now()
		dr.Status.DrainStartTime = &now
	}

	pods, allGone, err := r.listTargetPods(ctx, dr)
	if err != nil {
		return ctrl.Result{}, err
	}

	if allGone {
		return r.complete(ctx, dr)
	}

	// dr.Spec.IsFatal is NVSentinel's own classification of the triggering fault
	// and is the primary signal; FatalErrorCodes is only a plugin-config escape
	// hatch for codes an operator wants to force immediate eviction for.
	fatal := dr.Spec.IsFatal || (dr.Spec.ErrorCode != "" && r.Config.FatalErrorCodes[dr.Spec.ErrorCode])
	deadlineExceeded := time.Since(dr.Status.DrainStartTime.Time) >= r.Config.QuiesceDeadline

	if fatal || deadlineExceeded {
		return r.forceDrain(ctx, dr, pods, fatal)
	}

	return r.gracefulDrain(ctx, dr, pods)
}

// listTargetPods fetches every pod spec.podsToDrain still names. Only a
// NotFound pod counts as gone; a pod mid-termination from an earlier eviction
// stays in the result so the caller keeps waiting for it instead of reporting
// DrainComplete while it is still shutting down.
func (r *DrainRequestReconciler) listTargetPods(
	ctx context.Context, dr *nvsentinelv1alpha1.DrainRequest,
) (pods []corev1.Pod, allGone bool, err error) {
	for namespace, names := range dr.Spec.PodsToDrain {
		for _, name := range names {
			pod := &corev1.Pod{}

			switch getErr := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, pod); {
			case apierrors.IsNotFound(getErr):
				continue
			case getErr != nil:
				return nil, false, fmt.Errorf("get pod %s/%s: %w", namespace, name, getErr)
			}

			pods = append(pods, *pod)
		}
	}

	return pods, len(pods) == 0, nil
}

// handleStuckTerminating reports a pod that has had DeletionTimestamp set for
// longer than Config.StuckTerminatingTimeout, so neither a graceful nor a
// forced drain hangs silently when something — most plausibly a finalizer
// whose owning controller stopped reconciling, since it also needed the node
// being drained — is blocking removal.
//
// It only acts on that (clearing the pod's finalizers) when
// Config.ClearStuckFinalizers is true, which defaults to false: a finalizer
// exists to guarantee some cleanup completes, and forcibly clearing it skips
// that guarantee rather than honoring it. With the default off, this only
// logs, so the operator sees the stuck pod without the plugin silently
// bypassing whatever the finalizer was protecting.
//
// Even enabled, this is not a complete fix for every way a pod can get stuck
// in Terminating: on a genuinely unresponsive node, kubelet may never
// acknowledge the deletion regardless of finalizers — the same fundamental
// limitation NVSentinel's own node-drainer has.
func (r *DrainRequestReconciler) handleStuckTerminating(ctx context.Context, pod *corev1.Pod) {
	if time.Since(pod.DeletionTimestamp.Time) < r.Config.StuckTerminatingTimeout {
		return
	}

	log := logf.FromContext(ctx)
	key := podKey(pod.Namespace, pod.Name)
	stuckFor := time.Since(pod.DeletionTimestamp.Time)

	if !r.Config.ClearStuckFinalizers {
		log.Info("Pod has been terminating past the stuck-pod timeout; "+
			"ClearStuckFinalizers is disabled (the default), leaving its finalizers as-is",
			"pod", key, "stuckFor", stuckFor, "finalizers", pod.Finalizers)

		return
	}

	cleared, err := quiesce.ClearFinalizers(ctx, r.Client, pod)
	if err != nil {
		log.Error(err, "Failed to clear finalizers on a pod stuck terminating", "pod", key)
		return
	}

	if cleared {
		log.Info("Cleared finalizers on a pod stuck terminating", "pod", key, "stuckFor", stuckFor)
	}
}

// gracefulDrain pulls each pod out of routing, waits for its vLLM activity to
// read zero across ConfirmWindow, and evicts it through the Eviction API
// (respecting any PodDisruptionBudget) once confirmed.
func (r *DrainRequestReconciler) gracefulDrain(
	ctx context.Context, dr *nvsentinelv1alpha1.DrainRequest, pods []corev1.Pod,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	metricsClient := vllmmetrics.NewClient(r.Config.MetricsPort, r.Config.MetricsPath, r.Config.MetricsTimeout)
	quiescedByKey := indexQuiesced(dr.Status.QuiescedPods)

	var stillQuiesced []nvsentinelv1alpha1.PodQuiesceStatus

	routingConfigured := true

	for i := range pods {
		pod := &pods[i]
		key := podKey(pod.Namespace, pod.Name)

		if !pod.DeletionTimestamp.IsZero() {
			r.handleStuckTerminating(ctx, pod)
			continue // already evicted last pass and terminating; wait for it to disappear (or get unstuck above)
		}

		if !quiesce.HasReadinessGate(pod) {
			// MarkNotReady below will still patch the condition, but without the
			// gate declared it has no effect on routing — see HasReadinessGate.
			routingConfigured = false

			log.Info("Pod does not declare the drain readiness gate; routing removal will not take effect",
				"pod", key, "conditionType", quiesce.DrainReadyConditionType)
		}

		if err := quiesce.MarkNotReady(ctx, r.Client, pod, "Draining", "excluded from routing pending drain"); err != nil {
			log.Error(err, "Failed to mark pod not ready", "pod", key)
			continue // retry next reconcile rather than fail the whole pass over one pod
		}

		if pod.Status.PodIP == "" {
			continue // no IP yet to scrape
		}

		activity, err := metricsClient.Fetch(ctx, pod.Status.PodIP)
		if err != nil {
			log.Info("Could not scrape vLLM metrics, treating pod as still active", "pod", key, "error", err.Error())
			continue
		}

		if !activity.Idle() {
			continue // any activity resets the debounce: dropped from stillQuiesced
		}

		firstZero, seenBefore := quiescedByKey[key]
		if !seenBefore {
			now := metav1.Now()
			firstZero = &now
		}

		if time.Since(firstZero.Time) < r.Config.ConfirmWindow {
			stillQuiesced = append(stillQuiesced,
				nvsentinelv1alpha1.PodQuiesceStatus{Namespace: pod.Namespace, Name: pod.Name, FirstZeroObservedAt: firstZero})

			continue
		}

		evicted, err := quiesce.Evict(ctx, r.Client, pod)
		if err != nil {
			return ctrl.Result{}, err
		}

		if !evicted {
			// A PodDisruptionBudget is blocking eviction right now; keep the
			// confirmed-zero record so the debounce doesn't restart once it clears.
			stillQuiesced = append(stillQuiesced,
				nvsentinelv1alpha1.PodQuiesceStatus{Namespace: pod.Namespace, Name: pod.Name, FirstZeroObservedAt: firstZero})

			continue
		}

		log.Info("Evicted quiesced vLLM pod", "pod", key)
	}

	dr.Status.QuiescedPods = stillQuiesced

	if routingConfigured {
		meta.SetStatusCondition(&dr.Status.Conditions, metav1.Condition{
			Type: routingConfiguredConditionType, Status: metav1.ConditionTrue,
			Reason: "ReadinessGateDeclared", Message: "every target pod declares the drain readiness gate",
		})
	} else {
		meta.SetStatusCondition(&dr.Status.Conditions, metav1.Condition{
			Type: routingConfiguredConditionType, Status: metav1.ConditionFalse,
			Reason: "MissingReadinessGate",
			Message: fmt.Sprintf("at least one target pod does not declare %s in spec.readinessGates: "+
				"it keeps receiving new requests through ordinary Service routing until evicted",
				quiesce.DrainReadyConditionType),
		})
	}

	if err := r.Status().Update(ctx, dr); err != nil {
		return ctrl.Result{}, fmt.Errorf("update quiesce status for %s/%s: %w", dr.Namespace, dr.Name, err)
	}

	return ctrl.Result{RequeueAfter: r.Config.PollInterval}, nil
}

// forceDrain deletes pods directly with an explicit, bounded grace period
// (Config.ForceDeleteGracePeriodSeconds), bypassing the Eviction API and
// therefore any PodDisruptionBudget. Mirrors node-drainer's own
// DeleteAfterTimeout semantics: called only once the quiesce deadline has
// passed, or immediately for a fault the plugin treats as fatal.
//
// This only starts termination; it does not itself mark DrainComplete=True.
// Reconcile only does that once listTargetPods reports every pod actually
// gone — including pods this call just deleted, which is why a forced drain
// typically spans more than one reconcile.
func (r *DrainRequestReconciler) forceDrain(
	ctx context.Context, dr *nvsentinelv1alpha1.DrainRequest, pods []corev1.Pod, fatal bool,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	for i := range pods {
		pod := &pods[i]

		if !pod.DeletionTimestamp.IsZero() {
			r.handleStuckTerminating(ctx, pod)
			continue // deletion already requested; wait for it (or get unstuck above)
		}

		if err := quiesce.ForceDelete(ctx, r.Client, pod, r.Config.ForceDeleteGracePeriodSeconds); err != nil {
			return ctrl.Result{}, err
		}
	}

	dr.Status.Forced = true

	reason, message := "DeadlineExceeded",
		fmt.Sprintf("quiesce deadline of %s reached before all pods were confirmed idle", r.Config.QuiesceDeadline)

	switch {
	case dr.Spec.IsFatal:
		reason = "FatalHealthEvent"
		message = "triggering health event is marked fatal: skipped graceful quiescing"
	case fatal:
		reason = "FatalErrorCode"
		message = fmt.Sprintf("errorCode %q is configured as fatal: skipped graceful quiescing", dr.Spec.ErrorCode)
	}

	meta.SetStatusCondition(&dr.Status.Conditions, metav1.Condition{
		Type: quiesceCompleteConditionType, Status: metav1.ConditionFalse, Reason: reason, Message: message,
	})

	if err := r.Status().Update(ctx, dr); err != nil {
		return ctrl.Result{}, fmt.Errorf("update forced-drain status for %s/%s: %w", dr.Namespace, dr.Name, err)
	}

	log.Info("Forcing drain", "node", dr.Spec.NodeName, "reason", reason)

	return ctrl.Result{RequeueAfter: r.Config.PollInterval}, nil
}

// complete marks DrainComplete=True — the one signal node-drainer polls for.
// The reason distinguishes a drain where every pod was confirmed idle first
// from one a deadline or fatal fault forced, so that distinction survives in
// the CR (and any event/log reading it) even though node-drainer itself only
// cares about the boolean.
func (r *DrainRequestReconciler) complete(
	ctx context.Context, dr *nvsentinelv1alpha1.DrainRequest,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	reason, message := "GracefulDrainComplete", "all target pods were confirmed idle before eviction"

	if dr.Status.Forced {
		reason, message = "ForcedDrainComplete", "quiesce deadline or a fatal fault forced eviction before confirming idle"
	} else {
		meta.SetStatusCondition(&dr.Status.Conditions, metav1.Condition{
			Type: quiesceCompleteConditionType, Status: metav1.ConditionTrue,
			Reason: "AllPodsIdle", Message: "every target pod reached zero in-flight requests before eviction",
		})
	}

	meta.SetStatusCondition(&dr.Status.Conditions, metav1.Condition{
		Type: drainCompleteConditionType, Status: metav1.ConditionTrue, Reason: reason, Message: message,
	})
	dr.Status.QuiescedPods = nil

	if err := r.Status().Update(ctx, dr); err != nil {
		return ctrl.Result{}, fmt.Errorf("update completion status for %s/%s: %w", dr.Namespace, dr.Name, err)
	}

	log.Info("Drain complete", "node", dr.Spec.NodeName, "forced", dr.Status.Forced)

	return ctrl.Result{}, nil
}

func indexQuiesced(in []nvsentinelv1alpha1.PodQuiesceStatus) map[string]*metav1.Time {
	out := make(map[string]*metav1.Time, len(in))
	for i := range in {
		out[podKey(in[i].Namespace, in[i].Name)] = in[i].FirstZeroObservedAt
	}

	return out
}

func podKey(namespace, name string) string {
	return namespace + "/" + name
}

// SetupWithManager sets up the controller with the Manager.
func (r *DrainRequestReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nvsentinelv1alpha1.DrainRequest{}).
		Named("drainrequest").
		Complete(r)
}
