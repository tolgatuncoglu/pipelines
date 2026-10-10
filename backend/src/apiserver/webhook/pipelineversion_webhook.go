/*
Copyright 2025.
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

package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"

	"github.com/golang/glog"
	"github.com/kubeflow/pipelines/backend/src/apiserver/common"
	"github.com/kubeflow/pipelines/backend/src/apiserver/template"
	k8sapi "github.com/kubeflow/pipelines/backend/src/crd/kubernetes/v2beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrladmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

var scheme *runtime.Scheme

func init() {
	scheme = runtime.NewScheme()
	err := k8sapi.AddToScheme(scheme)
	if err != nil {
		// Panic is okay here because it means there's a code issue and so the package shouldn't initialize.
		panic(fmt.Sprintf("Failed to initialize the Kubernetes API scheme: %v", err))
	}
}

type PipelineVersionsWebhook struct {
	Client        ctrlclient.Client
	ClientNoCache ctrlclient.Client
}

var _ ctrladmission.CustomValidator = &PipelineVersionsWebhook{}

// newBadRequestError reports a problem with the submitted object. The PipelineVersionRejectedCause
// carries the message unprefixed to the KFP API, which reports it as invalid input. Failures of the
// webhook's own dependencies or configuration use apierrors.NewInternalError and carry no cause.
func newBadRequestError(msg string) *apierrors.StatusError {
	return &apierrors.StatusError{
		ErrStatus: metav1.Status{
			Status:  metav1.StatusFailure,
			Code:    http.StatusBadRequest,
			Reason:  metav1.StatusReasonBadRequest,
			Message: msg,
			Details: &metav1.StatusDetails{
				Causes: []metav1.StatusCause{{Type: k8sapi.PipelineVersionRejectedCause, Message: msg}},
			},
		},
	}
}

func (p *PipelineVersionsWebhook) getPipeline(ctx context.Context, namespace string, name string) (*k8sapi.Pipeline, error) {
	pipeline := &k8sapi.Pipeline{}
	nsName := types.NamespacedName{Namespace: namespace, Name: name}
	err := p.Client.Get(ctx, nsName, pipeline)
	if err == nil {
		return pipeline, nil
	}

	if !apierrors.IsNotFound(err) {
		return nil, apierrors.NewInternalError(fmt.Errorf("failed to get the Pipeline %s/%s: %w", namespace, name, err))
	}

	// Fallback to not using the cache
	err = p.ClientNoCache.Get(ctx, nsName, pipeline)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, newBadRequestError("The spec.pipelineName doesn't map to an existing Pipeline object")
		}

		return nil, apierrors.NewInternalError(fmt.Errorf("failed to get the Pipeline %s/%s: %w", namespace, name, err))
	}

	return pipeline, nil
}

func (p *PipelineVersionsWebhook) ValidateCreate(
	ctx context.Context, obj runtime.Object,
) (ctrladmission.Warnings, error) {
	pipelineVersion, ok := obj.(*k8sapi.PipelineVersion)
	if !ok {
		return nil, newBadRequestError(fmt.Sprintf("Expected a PipelineVersion object but got %T", pipelineVersion))
	}

	// Check the size first so an oversized object fails with an actionable message here instead of
	// a generic "request is too large" error from etcd after admission.
	if err := validatePipelineVersionObjectSize(pipelineVersion, nil); err != nil {
		return nil, err
	}

	modelPipelineVersion, err := pipelineVersion.ToModel()
	if err != nil {
		return nil, newBadRequestError(fmt.Sprintf("The pipeline spec is invalid: %v", err))
	}

	// cache enabled or not doesn't matter in this context
	tmpl, err := template.NewV2SpecTemplate([]byte(modelPipelineVersion.PipelineSpec), template.TemplateOptions{})
	if err != nil {
		return nil, newBadRequestError(fmt.Sprintf("The pipeline spec is invalid: %v", err))
	}

	err = common.ValidatePipelineName(tmpl.V2PipelineName())
	if err != nil {
		return nil, newBadRequestError(err.Error())
	}

	return nil, nil
}

// validatePipelineVersionObjectSize rejects a PipelineVersion whose serialized size exceeds the limit. For an
// update, previous is the stored object, and the update is rejected only if it also grows the object, so
// versions already over the limit, for example after an administrator lowered it, stay editable. The limit
// is read on every call so configuration changes apply without restarting the API server.
func validatePipelineVersionObjectSize(pipelineVersion *k8sapi.PipelineVersion, previous *k8sapi.PipelineVersion) error {
	limit, err := common.GetPipelineVersionObjectSizeLimit()
	if err != nil {
		return apierrors.NewInternalError(fmt.Errorf("the PipelineVersion object size limit is misconfigured: %w. Ask an administrator to fix it", err))
	}

	size, err := serializedSize(pipelineVersion)
	if err != nil || size <= limit {
		return err
	}
	if previous == nil {
		logSizeLimitExceeded(limit, size)
		return newBadRequestError(common.PipelineVersionObjectTooLargeMessage(size, limit))
	}

	// Compare only what the request controls. Kubernetes bumps the generation and rewrites managed fields
	// on updates, which would otherwise make an update that shrinks the object look like growth.
	grows, err := growsUserContent(pipelineVersion, previous)
	if err != nil || !grows {
		return err
	}
	previousSize, err := serializedSize(previous)
	if err != nil {
		return err
	}
	logSizeLimitExceeded(limit, size)
	return newBadRequestError(common.PipelineVersionUpdateTooLargeMessage(size, previousSize, limit))
}

// growsUserContent reports whether an update makes the object larger, ignoring metadata that the
// Kubernetes API server maintains.
func growsUserContent(pipelineVersion *k8sapi.PipelineVersion, previous *k8sapi.PipelineVersion) (bool, error) {
	size, err := serializedSize(withoutServerManagedMetadata(pipelineVersion))
	if err != nil {
		return false, err
	}
	previousSize, err := serializedSize(withoutServerManagedMetadata(previous))
	if err != nil {
		return false, err
	}
	return size > previousSize, nil
}

func withoutServerManagedMetadata(pipelineVersion *k8sapi.PipelineVersion) *k8sapi.PipelineVersion {
	stripped := pipelineVersion.DeepCopy()
	stripped.ManagedFields = nil
	stripped.Generation = 0
	stripped.ResourceVersion = ""
	return stripped
}

// serializedSize measures the whole object Kubernetes persists, including metadata such as the
// kubectl.kubernetes.io/last-applied-configuration annotation.
func serializedSize(pipelineVersion *k8sapi.PipelineVersion) (int, error) {
	serialized, err := json.Marshal(pipelineVersion)
	if err != nil {
		return 0, apierrors.NewInternalError(fmt.Errorf("failed to serialize the PipelineVersion object: %w", err))
	}
	return len(serialized), nil
}

func logSizeLimitExceeded(limit int, size int) {
	glog.Warningf("size_limit_exceeded control=pipeline_version_object limit_bytes=%d object_bytes=%d setting=%s",
		limit, size, common.MaxPipelineVersionObjectBytesConfig)
}

func (p *PipelineVersionsWebhook) ValidateUpdate(_ context.Context, oldObj, newObj runtime.Object) (ctrladmission.Warnings, error) {
	oldPipelineVersion, ok := oldObj.(*k8sapi.PipelineVersion)
	if !ok {
		return nil, newBadRequestError(fmt.Sprintf("Expected a PipelineVersion but got %T", oldObj))
	}

	newPipelineVersion, ok := newObj.(*k8sapi.PipelineVersion)
	if !ok {
		return nil, newBadRequestError(fmt.Sprintf("Expected a PipelineVersion but got %T", newObj))
	}

	// Only validate spec changes when the generation changes.
	// Metadata and status-only updates do not bump the generation.
	if oldPipelineVersion.Generation != newPipelineVersion.Generation {
		// Copy the old spec and overwrite only the
		// mutable fields (DisplayName, Tags) from the new spec. If the result
		// differs from the new spec, immutable fields were changed. This ensures
		// new fields are immutable by default without updating this code.
		expected := oldPipelineVersion.Spec.DeepCopy()
		expected.DisplayName = newPipelineVersion.Spec.DisplayName
		expected.Tags = newPipelineVersion.Spec.Tags
		if !reflect.DeepEqual(*expected, newPipelineVersion.Spec) {
			return nil, newBadRequestError("Pipeline spec is immutable; only mutable fields (display_name, tags) can be updated")
		}
	}

	// Check every update, not only spec changes: labels and annotations can grow the object without
	// changing its generation.
	if err := validatePipelineVersionObjectSize(newPipelineVersion, oldPipelineVersion); err != nil {
		return nil, err
	}

	return nil, nil
}

// ValidateDelete is unused but required to implement the ctrladmission.CustomValidator interface.
func (p *PipelineVersionsWebhook) ValidateDelete(_ context.Context, _ runtime.Object) (ctrladmission.Warnings, error) {
	return nil, nil
}

func (p *PipelineVersionsWebhook) Default(ctx context.Context, obj runtime.Object) error {
	pipelineVersion, ok := obj.(*k8sapi.PipelineVersion)
	if !ok {
		return newBadRequestError(fmt.Sprintf("Expected a PipelineVersion object but got %T", obj))
	}

	pipeline, err := p.getPipeline(ctx, pipelineVersion.Namespace, pipelineVersion.Spec.PipelineName)
	if err != nil {
		return err
	}

	if pipelineVersion.Labels == nil {
		pipelineVersion.Labels = map[string]string{}
	}

	// Labels for efficient querying
	pipelineVersion.Labels["pipelines.kubeflow.org/pipeline-id"] = string(pipeline.UID)

	name := pipeline.Name
	if len(name) > 63 {
		name = name[:63]
	}

	pipelineVersion.Labels["pipelines.kubeflow.org/pipeline"] = name

	trueVal := true

	for i := range pipelineVersion.OwnerReferences {
		ownerRef := &pipelineVersion.OwnerReferences[i]
		if ownerRef.APIVersion != k8sapi.GroupVersion.String() || ownerRef.Kind != "Pipeline" {
			continue
		}

		ownerRef.Name = pipeline.Name
		ownerRef.BlockOwnerDeletion = &trueVal
		ownerRef.UID = pipeline.UID

		return nil
	}

	pipelineVersion.OwnerReferences = append(pipelineVersion.OwnerReferences, metav1.OwnerReference{
		APIVersion:         k8sapi.GroupVersion.String(),
		Kind:               "Pipeline",
		Name:               pipeline.Name,
		BlockOwnerDeletion: &trueVal,
		UID:                pipeline.UID,
	})

	return nil
}

// NewPipelineVersionWebhook returns the validating webhook and mutating webhook HTTP handlers
func NewPipelineVersionWebhook(
	client ctrlclient.Client, clientNoCache ctrlclient.Client,
) (http.Handler, http.Handler, error) {
	validating, err := ctrladmission.StandaloneWebhook(
		ctrladmission.WithCustomValidator(
			scheme, &k8sapi.PipelineVersion{}, &PipelineVersionsWebhook{Client: client, ClientNoCache: clientNoCache},
		),
		ctrladmission.StandaloneOptions{},
	)
	if err != nil {
		return nil, nil, err
	}

	mutating, err := ctrladmission.StandaloneWebhook(
		ctrladmission.WithCustomDefaulter(
			scheme, &k8sapi.PipelineVersion{}, &PipelineVersionsWebhook{Client: client, ClientNoCache: clientNoCache},
		),
		ctrladmission.StandaloneOptions{},
	)
	if err != nil {
		return nil, nil, err
	}

	return validating, mutating, nil
}
