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
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nvsentinelv1alpha1 "github.com/raravind007/nvsentinel-vllm-drain-plugin/api/v1alpha1"
	"github.com/raravind007/nvsentinel-vllm-drain-plugin/internal/quiesce"
)

// This suite exercises the four scenarios the reference plugin is expected to
// handle: a successful graceful drain, a quiesce deadline forcing eviction, a
// fatal fault skipping quiescing entirely, and a simulated controller restart
// not resetting the deadline clock.

func zeroGracePeriod() *int64 {
	v := int64(0)
	return &v
}

// newTestPod declares a zero termination-grace-period pod: envtest has no
// kubelet to acknowledge a pod's shutdown, so without this a deleted or
// evicted pod would keep its deletionTimestamp forever and the tests below
// could never observe DrainComplete=True. It also declares the drain
// readiness gate, matching the precondition documented on
// quiesce.DrainReadyConditionType; use newTestPodWithoutReadinessGate to
// exercise a workload that doesn't meet that precondition.
func newTestPod(namespace, name string, labels map[string]string) *corev1.Pod {
	pod := newTestPodWithoutReadinessGate(namespace, name, labels)
	pod.Spec.ReadinessGates = []corev1.PodReadinessGate{
		{ConditionType: corev1.PodConditionType(quiesce.DrainReadyConditionType)},
	}

	return pod
}

func newTestPodWithoutReadinessGate(namespace, name string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Spec: corev1.PodSpec{
			TerminationGracePeriodSeconds: zeroGracePeriod(),
			Containers: []corev1.Container{{
				Name:  "vllm",
				Image: "example/vllm:test",
			}},
		},
	}
}

func setPodIP(ctx context.Context, pod *corev1.Pod, ip string) {
	pod.Status.PodIP = ip
	pod.Status.Phase = corev1.PodRunning
	ExpectWithOffset(1, k8sClient.Status().Update(ctx, pod)).To(Succeed())
}

// fakeVLLMMetrics serves a fixed running/waiting reading, standing in for a
// real vLLM pod's /metrics endpoint.
func fakeVLLMMetrics(running, waiting float64) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, "vllm:num_requests_running %v\nvllm:num_requests_waiting %v\n", running, waiting)
	}))
}

func serverPort(srv *httptest.Server) int {
	parts := strings.Split(srv.URL, ":")
	port, err := strconv.Atoi(parts[len(parts)-1])
	ExpectWithOffset(1, err).NotTo(HaveOccurred())

	return port
}

// reconcileUntilComplete drives reconciles at the plugin's own poll interval
// until DrainComplete=True, the same cadence node-drainer's own polling loop
// would observe in production.
func reconcileUntilComplete(r *DrainRequestReconciler, key types.NamespacedName) *nvsentinelv1alpha1.DrainRequest {
	var dr *nvsentinelv1alpha1.DrainRequest

	Eventually(func(g Gomega) bool {
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		g.Expect(err).NotTo(HaveOccurred())

		dr = &nvsentinelv1alpha1.DrainRequest{}
		g.Expect(k8sClient.Get(ctx, key, dr)).To(Succeed())

		return meta.IsStatusConditionTrue(dr.Status.Conditions, drainCompleteConditionType)
	}, 2*time.Second, 20*time.Millisecond).Should(BeTrue())

	return dr
}

var _ = Describe("DrainRequest Controller", func() {
	const namespace = "default"

	It("evicts a quiesced pod and reports a graceful completion", func() {
		podName, drainName := "graceful-vllm-pod", "graceful-drain"

		metrics := fakeVLLMMetrics(0, 0) // idle from the start: nothing to wait out but the confirm window
		defer metrics.Close()

		pod := newTestPod(namespace, podName, nil)
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		setPodIP(ctx, pod, "127.0.0.1")

		dr := &nvsentinelv1alpha1.DrainRequest{
			ObjectMeta: metav1.ObjectMeta{Name: drainName, Namespace: namespace},
			Spec: nvsentinelv1alpha1.DrainRequestSpec{
				NodeName:    "node-a",
				PodsToDrain: map[string][]string{namespace: {podName}},
			},
		}
		Expect(k8sClient.Create(ctx, dr)).To(Succeed())

		reconciler := &DrainRequestReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			Config: Config{
				MetricsPort:             serverPort(metrics),
				MetricsPath:             "/metrics",
				MetricsTimeout:          time.Second,
				QuiesceDeadline:         time.Minute,
				PollInterval:            10 * time.Millisecond,
				ConfirmWindow:           15 * time.Millisecond,
				FatalErrorCodes:         map[string]bool{},
				StuckTerminatingTimeout: time.Hour,
				ClearStuckFinalizers:    false,
			},
		}

		final := reconcileUntilComplete(reconciler, types.NamespacedName{Name: drainName, Namespace: namespace})

		Expect(final.Status.Forced).To(BeFalse())
		Expect(meta.IsStatusConditionTrue(final.Status.Conditions, quiesceCompleteConditionType)).To(BeTrue())
		Expect(meta.IsStatusConditionTrue(final.Status.Conditions, routingConfiguredConditionType)).To(BeTrue())

		Expect(k8sClient.Delete(ctx, dr)).To(Succeed())
	})

	It("flags a pod that does not declare the drain readiness gate", func() {
		podName, drainName := "no-gate-vllm-pod", "no-gate-drain"

		metrics := fakeVLLMMetrics(1, 0) // never idle: this test only checks the routing-misconfiguration signal
		defer metrics.Close()

		pod := newTestPodWithoutReadinessGate(namespace, podName, nil)
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		setPodIP(ctx, pod, "127.0.0.1")

		dr := &nvsentinelv1alpha1.DrainRequest{
			ObjectMeta: metav1.ObjectMeta{Name: drainName, Namespace: namespace},
			Spec: nvsentinelv1alpha1.DrainRequestSpec{
				NodeName:    "node-e",
				PodsToDrain: map[string][]string{namespace: {podName}},
			},
		}
		Expect(k8sClient.Create(ctx, dr)).To(Succeed())

		key := types.NamespacedName{Name: drainName, Namespace: namespace}
		reconciler := &DrainRequestReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			Config: Config{
				MetricsPort:             serverPort(metrics),
				MetricsPath:             "/metrics",
				MetricsTimeout:          time.Second,
				QuiesceDeadline:         time.Hour,
				PollInterval:            10 * time.Millisecond,
				ConfirmWindow:           15 * time.Millisecond,
				FatalErrorCodes:         map[string]bool{},
				StuckTerminatingTimeout: time.Hour,
				ClearStuckFinalizers:    false,
			},
		}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		current := &nvsentinelv1alpha1.DrainRequest{}
		Expect(k8sClient.Get(ctx, key, current)).To(Succeed())

		cond := meta.FindStatusCondition(current.Status.Conditions, routingConfiguredConditionType)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("MissingReadinessGate"))

		Expect(k8sClient.Delete(ctx, dr)).To(Succeed())
		Expect(k8sClient.Delete(ctx, pod)).To(Succeed())
	})

	// A real PodDisruptionBudget block is NOT exercised here: envtest runs no
	// disruption controller, so a PDB's status (ObservedGeneration,
	// DisruptionsAllowed) never gets synced, and this apiserver version permits
	// the eviction anyway rather than denying it for stale status — verified
	// empirically against this envtest version, not assumed. quiesce.Evict's
	// handling of a genuinely blocked eviction (TooManyRequests) is unit-tested
	// instead, against a fake client that can actually return that error; see
	// internal/quiesce/pod_test.go.

	It("force-deletes pods once the quiesce deadline passes", func() {
		podName, drainName := "timeout-vllm-pod", "timeout-drain"

		pod := newTestPod(namespace, podName, nil)
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		setPodIP(ctx, pod, "127.0.0.1") // nothing listens on the configured metrics port below

		dr := &nvsentinelv1alpha1.DrainRequest{
			ObjectMeta: metav1.ObjectMeta{Name: drainName, Namespace: namespace},
			Spec: nvsentinelv1alpha1.DrainRequestSpec{
				NodeName:    "node-b",
				PodsToDrain: map[string][]string{namespace: {podName}},
			},
		}
		Expect(k8sClient.Create(ctx, dr)).To(Succeed())

		key := types.NamespacedName{Name: drainName, Namespace: namespace}
		reconciler := &DrainRequestReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			Config: Config{
				MetricsPort:             1, // every scrape fails: activity is always treated as "still in flight"
				MetricsPath:             "/metrics",
				MetricsTimeout:          50 * time.Millisecond,
				QuiesceDeadline:         30 * time.Millisecond,
				PollInterval:            10 * time.Millisecond,
				ConfirmWindow:           15 * time.Millisecond,
				FatalErrorCodes:         map[string]bool{},
				StuckTerminatingTimeout: time.Hour,
				ClearStuckFinalizers:    false,
			},
		}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key}) // sets DrainStartTime
		Expect(err).NotTo(HaveOccurred())

		time.Sleep(40 * time.Millisecond) // let the deadline pass

		final := reconcileUntilComplete(reconciler, key)

		Expect(final.Status.Forced).To(BeTrue())

		cond := meta.FindStatusCondition(final.Status.Conditions, quiesceCompleteConditionType)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Status).To(Equal(metav1.ConditionFalse))
		Expect(cond.Reason).To(Equal("DeadlineExceeded"))

		Expect(k8sClient.Delete(ctx, dr)).To(Succeed())
	})

	It("skips quiescing and evicts immediately for a fault configured as fatal", func() {
		podName, drainName := "fatal-vllm-pod", "fatal-drain"

		pod := newTestPod(namespace, podName, nil)
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		setPodIP(ctx, pod, "127.0.0.1")

		dr := &nvsentinelv1alpha1.DrainRequest{
			ObjectMeta: metav1.ObjectMeta{Name: drainName, Namespace: namespace},
			Spec: nvsentinelv1alpha1.DrainRequestSpec{
				NodeName:    "node-c",
				PodsToDrain: map[string][]string{namespace: {podName}},
				IsFatal:     true, // NVSentinel's own classification, not a locally-configured error code
				ErrorCode:   "95",
			},
		}
		Expect(k8sClient.Create(ctx, dr)).To(Succeed())

		key := types.NamespacedName{Name: drainName, Namespace: namespace}
		reconciler := &DrainRequestReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			Config: Config{
				MetricsPort:             1,
				MetricsPath:             "/metrics",
				MetricsTimeout:          50 * time.Millisecond,
				QuiesceDeadline:         time.Hour, // large on purpose: only isFatal should force this, not a deadline
				PollInterval:            10 * time.Millisecond,
				ConfirmWindow:           15 * time.Millisecond,
				FatalErrorCodes:         map[string]bool{},
				StuckTerminatingTimeout: time.Hour,
				ClearStuckFinalizers:    false,
			},
		}

		// A single reconcile already forces eviction: a fatal fault never waits.
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		afterFirst := &nvsentinelv1alpha1.DrainRequest{}
		Expect(k8sClient.Get(ctx, key, afterFirst)).To(Succeed())
		Expect(afterFirst.Status.Forced).To(BeTrue())

		final := reconcileUntilComplete(reconciler, key)

		cond := meta.FindStatusCondition(final.Status.Conditions, quiesceCompleteConditionType)
		Expect(cond).NotTo(BeNil())
		Expect(cond.Reason).To(Equal("FatalHealthEvent"))

		Expect(k8sClient.Delete(ctx, dr)).To(Succeed())
	})

	It("clears finalizers on a pod stuck terminating when explicitly enabled", func() {
		podName, drainName := "stuck-vllm-pod", "stuck-drain"

		pod := newTestPod(namespace, podName, nil)
		pod.Finalizers = []string{"example.com/test-finalizer"} // holds the object in etcd after delete
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		setPodIP(ctx, pod, "127.0.0.1")

		dr := &nvsentinelv1alpha1.DrainRequest{
			ObjectMeta: metav1.ObjectMeta{Name: drainName, Namespace: namespace},
			Spec: nvsentinelv1alpha1.DrainRequestSpec{
				NodeName:    "node-g",
				PodsToDrain: map[string][]string{namespace: {podName}},
				IsFatal:     true, // force-deletes on the first reconcile; the finalizer is what gets it stuck
			},
		}
		Expect(k8sClient.Create(ctx, dr)).To(Succeed())

		key := types.NamespacedName{Name: drainName, Namespace: namespace}
		reconciler := &DrainRequestReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			Config: Config{
				MetricsPort:             1,
				MetricsPath:             "/metrics",
				MetricsTimeout:          50 * time.Millisecond,
				QuiesceDeadline:         time.Hour,
				PollInterval:            10 * time.Millisecond,
				ConfirmWindow:           15 * time.Millisecond,
				FatalErrorCodes:         map[string]bool{},
				StuckTerminatingTimeout: 20 * time.Millisecond,
				ClearStuckFinalizers:    true, // explicit opt-in; see the disabled-by-default test below
			},
		}

		// First reconcile deletes the pod; the finalizer keeps it present with
		// DeletionTimestamp set instead of actually removing it.
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		stuck := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, stuck)).To(Succeed())
		Expect(stuck.DeletionTimestamp).NotTo(BeNil())
		Expect(stuck.Finalizers).To(ContainElement("example.com/test-finalizer"))

		time.Sleep(30 * time.Millisecond) // past StuckTerminatingTimeout

		final := reconcileUntilComplete(reconciler, key)
		Expect(final.Status.Forced).To(BeTrue())

		Expect(k8sClient.Delete(ctx, dr)).To(Succeed())
	})

	It("leaves a stuck pod's finalizers alone when ClearStuckFinalizers is disabled (the default)", func() {
		podName, drainName := "stuck-default-vllm-pod", "stuck-default-drain"

		pod := newTestPod(namespace, podName, nil)
		pod.Finalizers = []string{"example.com/test-finalizer"}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		setPodIP(ctx, pod, "127.0.0.1")

		dr := &nvsentinelv1alpha1.DrainRequest{
			ObjectMeta: metav1.ObjectMeta{Name: drainName, Namespace: namespace},
			Spec: nvsentinelv1alpha1.DrainRequestSpec{
				NodeName:    "node-h",
				PodsToDrain: map[string][]string{namespace: {podName}},
				IsFatal:     true,
			},
		}
		Expect(k8sClient.Create(ctx, dr)).To(Succeed())

		key := types.NamespacedName{Name: drainName, Namespace: namespace}
		reconciler := &DrainRequestReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
			Config: Config{
				MetricsPort:             1,
				MetricsPath:             "/metrics",
				MetricsTimeout:          50 * time.Millisecond,
				QuiesceDeadline:         time.Hour,
				PollInterval:            10 * time.Millisecond,
				ConfirmWindow:           15 * time.Millisecond,
				FatalErrorCodes:         map[string]bool{},
				StuckTerminatingTimeout: 20 * time.Millisecond,
				// ClearStuckFinalizers deliberately omitted: false is the zero value and the default.
			},
		}

		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		time.Sleep(30 * time.Millisecond) // past StuckTerminatingTimeout

		_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		// The finalizer must still be there, the pod must still exist, and the
		// drain must not have been reported complete — disabled-by-default means
		// the plugin never silently bypasses a finalizer's cleanup guarantee.
		stuck := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, stuck)).To(Succeed())
		Expect(stuck.Finalizers).To(ContainElement("example.com/test-finalizer"))

		current := &nvsentinelv1alpha1.DrainRequest{}
		Expect(k8sClient.Get(ctx, key, current)).To(Succeed())
		Expect(meta.IsStatusConditionTrue(current.Status.Conditions, drainCompleteConditionType)).To(BeFalse())

		Expect(k8sClient.Delete(ctx, dr)).To(Succeed())

		// Clean up directly: nothing in this test enables the mechanism that
		// would otherwise remove this finalizer. Clearing it lets the apiserver
		// finish removing the pod, since DeletionTimestamp is already set.
		_, err = quiesce.ClearFinalizers(ctx, k8sClient, stuck)
		Expect(err).NotTo(HaveOccurred())
	})

	It("does not reset the quiesce deadline across a simulated controller restart", func() {
		podName, drainName := "restart-vllm-pod", "restart-drain"

		pod := newTestPod(namespace, podName, nil)
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		setPodIP(ctx, pod, "127.0.0.1") // never reads idle: the DrainRequest stays in progress for both reconciles

		dr := &nvsentinelv1alpha1.DrainRequest{
			ObjectMeta: metav1.ObjectMeta{Name: drainName, Namespace: namespace},
			Spec: nvsentinelv1alpha1.DrainRequestSpec{
				NodeName:    "node-d",
				PodsToDrain: map[string][]string{namespace: {podName}},
			},
		}
		Expect(k8sClient.Create(ctx, dr)).To(Succeed())

		key := types.NamespacedName{Name: drainName, Namespace: namespace}
		cfg := Config{
			MetricsPort:             1,
			MetricsPath:             "/metrics",
			MetricsTimeout:          50 * time.Millisecond,
			QuiesceDeadline:         time.Hour,
			PollInterval:            10 * time.Millisecond,
			ConfirmWindow:           15 * time.Millisecond,
			FatalErrorCodes:         map[string]bool{},
			StuckTerminatingTimeout: time.Hour,
			ClearStuckFinalizers:    false,
		}

		beforeRestart := &DrainRequestReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Config: cfg}
		_, err := beforeRestart.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		afterFirst := &nvsentinelv1alpha1.DrainRequest{}
		Expect(k8sClient.Get(ctx, key, afterFirst)).To(Succeed())
		Expect(afterFirst.Status.DrainStartTime).NotTo(BeNil())
		startTime := afterFirst.Status.DrainStartTime.DeepCopy()

		time.Sleep(20 * time.Millisecond)

		// A brand-new reconciler instance, standing in for a process restart,
		// must read drainStartTime back from the CR rather than recompute it.
		afterRestart := &DrainRequestReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Config: cfg}
		_, err = afterRestart.Reconcile(ctx, reconcile.Request{NamespacedName: key})
		Expect(err).NotTo(HaveOccurred())

		afterSecond := &nvsentinelv1alpha1.DrainRequest{}
		Expect(k8sClient.Get(ctx, key, afterSecond)).To(Succeed())
		Expect(afterSecond.Status.DrainStartTime.Time).To(Equal(startTime.Time))
		Expect(meta.IsStatusConditionTrue(afterSecond.Status.Conditions, drainCompleteConditionType)).To(BeFalse())

		Expect(k8sClient.Delete(ctx, dr)).To(Succeed())
		Expect(k8sClient.Delete(ctx, pod)).To(Succeed())
	})
})
