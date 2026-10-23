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

package quiesce

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// These are unit tests against a fake client with injected API responses, not
// against a real apiserver: envtest (used by the controller's own test suite)
// runs no disruption controller, so a PodDisruptionBudget's status never gets
// synced there, and that apiserver version was verified to permit eviction
// anyway rather than deny it for stale PDB status. A fake client that can
// return the TooManyRequests error directly is the only way, short of a real
// cluster with its controller-manager running, to exercise Evict's handling
// of a genuinely blocked eviction.

func podGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
}

func testPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "ns"},
	}
}

func TestEvictBlockedByPDBIsNotAnError(t *testing.T) {
	blocked := apierrors.NewTooManyRequests("blocked by PodDisruptionBudget", 0)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(testPod()).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceCreate: func(_ context.Context, _ client.Client, subResourceName string,
				_ client.Object, _ client.Object, _ ...client.SubResourceCreateOption) error {
				if subResourceName == "eviction" {
					return blocked
				}

				return nil
			},
		}).
		Build()

	evicted, err := Evict(context.Background(), fakeClient, testPod())
	if err != nil {
		t.Fatalf("Evict returned an error for a PDB block, want evicted=false, err=nil: %v", err)
	}

	if evicted {
		t.Error("Evict reported success despite the PDB block")
	}
}

func TestEvictAlreadyGoneCountsAsEvicted(t *testing.T) {
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceCreate: func(_ context.Context, _ client.Client, subResourceName string,
				_ client.Object, _ client.Object, _ ...client.SubResourceCreateOption) error {
				if subResourceName == "eviction" {
					return apierrors.NewNotFound(podGVR().GroupResource(), "p")
				}

				return nil
			},
		}).
		Build()

	evicted, err := Evict(context.Background(), fakeClient, testPod())
	if err != nil {
		t.Fatalf("Evict returned an error for an already-gone pod: %v", err)
	}

	if !evicted {
		t.Error("Evict(already gone) = false, want true")
	}
}

func TestEvictOtherErrorsPropagate(t *testing.T) {
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceCreate: func(_ context.Context, _ client.Client, subResourceName string,
				_ client.Object, _ client.Object, _ ...client.SubResourceCreateOption) error {
				if subResourceName == "eviction" {
					return apierrors.NewInternalError(context.DeadlineExceeded)
				}

				return nil
			},
		}).
		Build()

	_, err := Evict(context.Background(), fakeClient, testPod())
	if err == nil {
		t.Fatal("Evict swallowed an unexpected API error, want it returned")
	}
}

func TestForceDeleteUsesTheConfiguredGracePeriod(t *testing.T) {
	var gotGracePeriod *int64

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme.Scheme).
		WithObjects(testPod()).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(_ context.Context, _ client.WithWatch, _ client.Object, opts ...client.DeleteOption) error {
				do := &client.DeleteOptions{}
				do.ApplyOptions(opts)
				gotGracePeriod = do.GracePeriodSeconds

				return nil
			},
		}).
		Build()

	const want = int64(7)
	if err := ForceDelete(context.Background(), fakeClient, testPod(), want); err != nil {
		t.Fatalf("ForceDelete: %v", err)
	}

	if gotGracePeriod == nil || *gotGracePeriod != want {
		t.Errorf("grace period = %v, want %d", gotGracePeriod, want)
	}
}

func TestHasReadinessGate(t *testing.T) {
	withGate := testPod()
	withGate.Spec.ReadinessGates = []corev1.PodReadinessGate{
		{ConditionType: corev1.PodConditionType(DrainReadyConditionType)},
	}

	if !HasReadinessGate(withGate) {
		t.Error("HasReadinessGate(pod with the gate) = false, want true")
	}

	if HasReadinessGate(testPod()) {
		t.Error("HasReadinessGate(pod without any readiness gates) = true, want false")
	}
}
