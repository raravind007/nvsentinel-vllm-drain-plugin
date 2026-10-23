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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// DrainRequestSpec is filled in by NVSentinel's node-drainer from the triggering
// HealthEvent, via the customDrain template. Fields this type doesn't declare are
// pruned by node-drainer, so only the ones the plugin actually reads are declared.
type DrainRequestSpec struct {
	// nodeName is the node being drained. Carried for logging/status only.
	// +optional
	NodeName string `json:"nodeName,omitempty"`

	// podsToDrain maps namespace -> pod names node-drainer determined are evictable
	// for this event. The plugin drains exactly these pods; it does not re-derive
	// its own set. Empty means nothing to do.
	// +optional
	PodsToDrain map[string][]string `json:"podsToDrain,omitempty"`

	// isFatal mirrors the triggering HealthEvent's own isFatal flag — the
	// classification NVSentinel's health monitors already made about whether the
	// fault is recoverable. This is the primary signal the plugin uses to decide
	// whether graceful quiescing is safe at all; it deliberately does not
	// re-derive severity from errorCode.
	// +optional
	IsFatal bool `json:"isFatal,omitempty"`

	// errorCode is the first entry of the health event's errorCode list (which can
	// carry more than one code; the plugin surfaces only the first for visibility).
	// Carried for operator visibility and for FatalErrorCodes, a plugin-config
	// escape hatch for forcing specific codes to skip quiescing even when
	// isFatal was false upstream. Not the primary severity signal — see isFatal.
	// +optional
	ErrorCode string `json:"errorCode,omitempty"`

	// recommendedAction is the health event's RecommendedAction enum rendered as
	// its name (e.g. "COMPONENT_RESET"), surfaced for operator visibility on the
	// CR. Not currently used in the plugin's severity decision.
	// +optional
	RecommendedAction string `json:"recommendedAction,omitempty"`

	// healthEventID identifies the triggering health event, for correlating plugin
	// logs/events back to the NVSentinel event that started this drain.
	// +optional
	HealthEventID string `json:"healthEventID,omitempty"`
}

// DrainRequestStatus is written by the plugin. node-drainer polls Conditions for
// the configured statusConditionType/statusConditionStatus (DrainComplete=True)
// and otherwise ignores this type's shape, so additional fields are safe to add.
type DrainRequestStatus struct {
	// conditions record drain progress. node-drainer only polls for DrainComplete;
	// the others exist for operator visibility and to distinguish a graceful
	// completion from one that hit the deadline and was forced.
	//
	// Condition types used by this plugin:
	//   - DrainComplete: the contract node-drainer polls. True once every pod in
	//     spec.podsToDrain is gone, however that was achieved.
	//   - QuiesceComplete: True once in-flight vLLM requests reached zero before
	//     the deadline. Absent or False when the deadline forced eviction instead.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// drainStartTime is set once, on the first reconcile, and never updated again.
	// The quiesce deadline is computed from this field rather than from
	// time-since-reconciler-start, so a controller restart does not reset or
	// extend how long the plugin waits for in-flight requests to finish.
	// +optional
	DrainStartTime *metav1.Time `json:"drainStartTime,omitempty"`

	// forced is true once the plugin gives up waiting for in-flight requests
	// (deadline reached, or a fatal error code skipped quiescing entirely) and
	// evicts regardless of in-flight activity. False means every evicted pod had
	// reached zero in-flight requests first.
	// +optional
	Forced bool `json:"forced,omitempty"`

	// quiescedPods records, per pod still being drained, when its vLLM activity
	// was first observed at zero. A pod is only evicted once this has held for
	// at least the plugin's confirm window, debouncing a single stale or racy
	// zero reading rather than trusting it immediately. Cleared once the drain
	// completes.
	// +optional
	QuiescedPods []PodQuiesceStatus `json:"quiescedPods,omitempty"`
}

// PodQuiesceStatus tracks how long one target pod's vLLM activity has read
// zero, so a single scrape never alone decides a pod is safe to evict.
type PodQuiesceStatus struct {
	// +required
	Namespace string `json:"namespace"`
	// +required
	Name string `json:"name"`
	// firstZeroObservedAt is cleared (the pod drops out of this list) the
	// moment activity is observed again, so the debounce restarts from zero
	// rather than from a stale earlier reading.
	// +optional
	FirstZeroObservedAt *metav1.Time `json:"firstZeroObservedAt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// DrainRequest is the Schema for the drainrequests API
type DrainRequest struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of DrainRequest
	// +required
	Spec DrainRequestSpec `json:"spec"`

	// status defines the observed state of DrainRequest
	// +optional
	Status DrainRequestStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// DrainRequestList contains a list of DrainRequest
type DrainRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []DrainRequest `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &DrainRequest{}, &DrainRequestList{})
		return nil
	})
}
