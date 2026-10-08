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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/kubeflow/pipelines/backend/src/apiserver/common"
	k8sapi "github.com/kubeflow/pipelines/backend/src/crd/kubernetes/v2beta1"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionv1 "k8s.io/api/admission/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"sigs.k8s.io/controller-runtime/pkg/client"
	k8sfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// jsonToInterface marshals a value to JSON and unmarshals it back to interface{},
// producing the map[string]interface{} types that runtime.DeepCopyJSONValue expects.
func jsonToInterface(t *testing.T, v interface{}) interface{} {
	data, err := json.Marshal(v)
	require.NoError(t, err)
	var result interface{}
	require.NoError(t, json.Unmarshal(data, &result))
	return result
}

func setupPipelineWebhookTest(t *testing.T) (*PipelineVersionsWebhook, interface{}) {
	scheme := runtime.NewScheme()
	require.NoError(t, k8sapi.AddToScheme(scheme))

	fakeClient := k8sfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&k8sapi.Pipeline{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pipeline",
				Namespace: "default",
				UID:       uuid.NewUUID(),
			},
		}).Build()

	pipelineWebhook := &PipelineVersionsWebhook{Client: fakeClient}

	validPipelineSpec := map[string]interface{}{
		"pipelineInfo": map[string]interface{}{
			"name":        "test-pipeline-v1",
			"description": "A simple test pipeline",
		},
		"root": map[string]interface{}{
			"dag": map[string]interface{}{
				"tasks": map[string]interface{}{},
			},
		},
		"schemaVersion": "2.1.0",
		"sdkVersion":    "kfp-2.11.0",
	}

	return pipelineWebhook, jsonToInterface(t, validPipelineSpec)
}

func TestPipelineVersionWebhook_ValidateCreate(t *testing.T) {
	pipelineWebhook, validPipelineSpecJSON := setupPipelineWebhookTest(t)

	pipelineVersion := &k8sapi.PipelineVersion{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pipeline-v1",
			Namespace: "default",
		},
		Spec: k8sapi.PipelineVersionSpec{
			PipelineName: "test-pipeline",
			PipelineSpec: k8sapi.IRSpec{
				Value: validPipelineSpecJSON,
			},
		},
	}
	_, err := pipelineWebhook.ValidateCreate(context.TODO(), pipelineVersion)
	assert.NoError(t, err, "Expected no error for a valid PipelineVersion")
}

func TestPipelineVersionWebhook_ValidateCreate_InvalidObjectType(t *testing.T) {
	pipelineWebhook, _ := setupPipelineWebhookTest(t)

	_, err := pipelineWebhook.ValidateCreate(context.TODO(), &k8sapi.Pipeline{})
	assert.Error(t, err, "Expected error when passing an object that is not a PipelineVersion")
	assert.Contains(t, err.Error(), "Expected a PipelineVersion object")
}

func TestPipelineVersionWebhook_ValidateCreate_InvalidPipelineSpec(t *testing.T) {
	pipelineWebhook, _ := setupPipelineWebhookTest(t)

	invalidPipelineSpec := map[string]interface{}{
		"pipelineInfo": map[string]interface{}{
			"name":        "test-pipeline-v1",
			"description": "A simple test pipeline",
		},
	}

	invalidPipelineVersion := &k8sapi.PipelineVersion{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pipeline-v1",
			Namespace: "default",
		},
		Spec: k8sapi.PipelineVersionSpec{
			PipelineName: "test-pipeline",
			PipelineSpec: k8sapi.IRSpec{
				Value: jsonToInterface(t, invalidPipelineSpec),
			},
		},
	}

	_, err := pipelineWebhook.ValidateCreate(context.TODO(), invalidPipelineVersion)
	assert.Error(t, err, "Expected error for invalid PipelineSpec")
	assert.Contains(t, err.Error(), "The pipeline spec is invalid")
}

func TestPipelineVersionWebhook_ValidateUpdate(t *testing.T) {
	pipelineWebhook, validPipelineSpecJSON := setupPipelineWebhookTest(t)

	oldPipelineVersion := &k8sapi.PipelineVersion{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-pipeline-v1",
			Namespace:  "default",
			Generation: 1,
		},
		Spec: k8sapi.PipelineVersionSpec{
			PipelineName: "test-pipeline",
			PipelineSpec: k8sapi.IRSpec{
				Value: validPipelineSpecJSON,
			},
		},
	}

	updatedPipelineSpec := map[string]interface{}{
		"pipelineInfo": map[string]interface{}{
			"name":        "test-pipeline-v2",
			"description": "Updated pipeline version",
		},
		"root": map[string]interface{}{
			"dag": map[string]interface{}{
				"tasks": map[string]interface{}{},
			},
		},
		"schemaVersion": "2.1.0",
		"sdkVersion":    "kfp-2.11.0",
	}

	newPipelineVersion := &k8sapi.PipelineVersion{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-pipeline-v1",
			Namespace:  "default",
			Generation: 2,
		},
		Spec: k8sapi.PipelineVersionSpec{
			PipelineName: "test-pipeline",
			PipelineSpec: k8sapi.IRSpec{
				Value: jsonToInterface(t, updatedPipelineSpec),
			},
		},
	}
	_, err := pipelineWebhook.ValidateUpdate(context.TODO(), oldPipelineVersion, newPipelineVersion)
	assert.Error(t, err, "Expected error for modifying pipeline spec")
	assert.Contains(t, err.Error(), "Pipeline spec is immutable")
}

func TestPipelineVersionWebhook_ValidateUpdate_MetadataChangeAllowed(t *testing.T) {
	pipelineWebhook, validPipelineSpecJSON := setupPipelineWebhookTest(t)

	oldPipelineVersion := &k8sapi.PipelineVersion{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-pipeline-v1",
			Namespace:  "default",
			Generation: 1,
			Labels:     map[string]string{"version": "v1"},
		},
		Spec: k8sapi.PipelineVersionSpec{
			PipelineName: "test-pipeline",
			PipelineSpec: k8sapi.IRSpec{
				Value: validPipelineSpecJSON,
			},
		},
	}

	newPipelineVersion := &k8sapi.PipelineVersion{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-pipeline-v1",
			Namespace:  "default",
			Generation: 1,
			Labels:     map[string]string{"version": "v2"},
		},
		Spec: k8sapi.PipelineVersionSpec{
			PipelineName: "test-pipeline",
			PipelineSpec: k8sapi.IRSpec{
				Value: validPipelineSpecJSON,
			},
		},
	}

	_, err := pipelineWebhook.ValidateUpdate(context.TODO(), oldPipelineVersion, newPipelineVersion)
	assert.NoError(t, err, "Expected no error for metadata-only change")
}

func TestPipelineVersionWebhook_MutatingUpdate_FixesOwnersRef(t *testing.T) {
	pipelineWebhook, validPipelineSpecJSON := setupPipelineWebhookTest(t)
	badUID := uuid.NewUUID()

	pipelineVersion := &k8sapi.PipelineVersion{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-pipeline-v1",
			Namespace:  "default",
			Generation: 1,
			Labels:     map[string]string{"version": "v2"},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: k8sapi.GroupVersion.String(),
					Kind:       "Pipeline",
					Name:       "test-pipeline2",
					UID:        badUID,
				},
			},
		},
		Spec: k8sapi.PipelineVersionSpec{
			PipelineName: "test-pipeline",
			PipelineSpec: k8sapi.IRSpec{
				Value: validPipelineSpecJSON,
			},
		},
	}

	err := pipelineWebhook.Default(context.TODO(), pipelineVersion)
	require.NoError(t, err, "Expected no error for fixing the owner's reference")
	require.Len(t, pipelineVersion.OwnerReferences, 1)
	require.NotEqual(t, pipelineVersion.OwnerReferences[0].UID, badUID)
	require.Equal(t, pipelineVersion.OwnerReferences[0].Name, "test-pipeline")
	require.True(t, *pipelineVersion.OwnerReferences[0].BlockOwnerDeletion)
}

func TestPipelineVersionWebhook_ValidateCreate_WithPlatformSpec(t *testing.T) {
	pipelineWebhook, _ := setupPipelineWebhookTest(t)

	validPipelineSpec := map[string]interface{}{
		"pipelineInfo": map[string]interface{}{
			"name":        "test-pipeline-v1",
			"description": "A simple test pipeline",
		},
		"root": map[string]interface{}{
			"dag": map[string]interface{}{
				"tasks": map[string]interface{}{},
			},
		},
		"schemaVersion": "2.1.0",
		"sdkVersion":    "kfp-2.11.0",
	}

	validPlatformSpec := map[string]interface{}{
		"platforms": map[string]interface{}{
			"kubernetes": map[string]interface{}{
				"pipelineConfig": map[string]interface{}{
					"workspace": map[string]interface{}{
						"size": "10Gi",
					},
				},
			},
		},
	}

	pipelineVersion := &k8sapi.PipelineVersion{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pipeline-v1",
			Namespace: "default",
		},
		Spec: k8sapi.PipelineVersionSpec{
			PipelineName: "test-pipeline",
			PipelineSpec: k8sapi.IRSpec{
				Value: validPipelineSpec,
			},
			PlatformSpec: &k8sapi.IRSpec{
				Value: validPlatformSpec,
			},
		},
	}
	_, err := pipelineWebhook.ValidateCreate(context.TODO(), pipelineVersion)
	assert.NoError(t, err, "Expected no error for a valid PipelineVersion with platform spec")
}

func TestPipelineVersionWebhook_Default_TruncatesLongPipelineNameLabel(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, k8sapi.AddToScheme(scheme))

	longName := "pipeline-name-0123456789012345678901234567890123456789012345678901234567890"
	expectedTrunc := longName[:63]

	fakeClient := k8sfake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(&k8sapi.Pipeline{
			ObjectMeta: metav1.ObjectMeta{
				Name:      longName,
				Namespace: "default",
				UID:       uuid.NewUUID(),
			},
		}).Build()

	webhook := &PipelineVersionsWebhook{Client: fakeClient, ClientNoCache: fakeClient}

	pipelineVersion := &k8sapi.PipelineVersion{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pipeline-v1",
			Namespace: "default",
		},
		Spec: k8sapi.PipelineVersionSpec{
			PipelineName: longName,
			PipelineSpec: k8sapi.IRSpec{
				Value: json.RawMessage("{}"),
			},
		},
	}

	require.NoError(t, webhook.Default(context.TODO(), pipelineVersion))

	got, ok := pipelineVersion.Labels["pipelines.kubeflow.org/pipeline"]
	require.True(t, ok, "expected pipeline label to be set")
	assert.Equal(t, expectedTrunc, got)
}

func newPipelineVersionWithAnnotation(specJSON interface{}, annotationBytes int) *k8sapi.PipelineVersion {
	return &k8sapi.PipelineVersion{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pipeline-v1",
			Namespace: "default",
			Annotations: map[string]string{
				"kubectl.kubernetes.io/last-applied-configuration": strings.Repeat("x", annotationBytes),
			},
		},
		Spec: k8sapi.PipelineVersionSpec{
			PipelineName: "test-pipeline",
			PipelineSpec: k8sapi.IRSpec{Value: specJSON},
		},
	}
}

func setPipelineVersionObjectSizeConfig(t *testing.T, value interface{}) {
	t.Helper()
	viper.Set(common.MaxPipelineVersionObjectBytesConfig, value)
	t.Cleanup(func() { viper.Set(common.MaxPipelineVersionObjectBytesConfig, nil) })
}

func TestPipelineVersionWebhook_ValidateCreate_RejectsOversizedObject(t *testing.T) {
	setPipelineVersionObjectSizeConfig(t, nil)
	pipelineWebhook, validPipelineSpecJSON := setupPipelineWebhookTest(t)

	// The annotation alone exceeds the default limit, so the whole persisted object is measured,
	// not just the pipeline spec.
	pipelineVersion := newPipelineVersionWithAnnotation(validPipelineSpecJSON, common.DefaultPipelineVersionObjectBytes)

	_, err := pipelineWebhook.ValidateCreate(context.TODO(), pipelineVersion)
	require.Error(t, err)
	assert.True(t, apierrors.IsBadRequest(err))
	assert.Contains(t, err.Error(), "The pipeline version is too large to store in Kubernetes")
	assert.Contains(t, err.Error(), "the limit is 1556480 bytes (1.48 MiB)")
	assert.Contains(t, err.Error(), "single etcd object")
	assert.Contains(t, err.Error(), common.MaxPipelineVersionObjectBytesConfig)
}

func TestPipelineVersionWebhook_ValidateCreate_AppliesConfigChangesWithoutRestart(t *testing.T) {
	pipelineWebhook, validPipelineSpecJSON := setupPipelineWebhookTest(t)
	pipelineVersion := newPipelineVersionWithAnnotation(validPipelineSpecJSON, 4096)

	setPipelineVersionObjectSizeConfig(t, "1024")
	_, err := pipelineWebhook.ValidateCreate(context.TODO(), pipelineVersion)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "the limit is 1024 bytes")

	setPipelineVersionObjectSizeConfig(t, "65536")
	_, err = pipelineWebhook.ValidateCreate(context.TODO(), pipelineVersion)
	assert.NoError(t, err)
}

func TestPipelineVersionWebhook_ValidateCreate_AllowsObjectsUpToKubernetesLimit(t *testing.T) {
	pipelineWebhook, validPipelineSpecJSON := setupPipelineWebhookTest(t)
	setPipelineVersionObjectSizeConfig(t, strconv.Itoa(common.MaximumPipelineVersionObjectBytes))

	pipelineVersion := newPipelineVersionWithAnnotation(validPipelineSpecJSON, 2000<<10)
	_, err := pipelineWebhook.ValidateCreate(context.TODO(), pipelineVersion)
	assert.NoError(t, err)
}

func TestPipelineVersionWebhook_ValidateCreate_RejectsLimitAboveKubernetesLimit(t *testing.T) {
	pipelineWebhook, validPipelineSpecJSON := setupPipelineWebhookTest(t)
	setPipelineVersionObjectSizeConfig(t, strconv.Itoa(common.MaximumPipelineVersionObjectBytes+1))

	_, err := pipelineWebhook.ValidateCreate(context.TODO(), newPipelineVersionWithAnnotation(validPipelineSpecJSON, 0))
	require.Error(t, err)
	assert.True(t, apierrors.IsInternalError(err), "a misconfigured limit is not the caller's fault")
	assert.Contains(t, err.Error(), "misconfigured")
	assert.Contains(t, err.Error(), "must not exceed 2080768 bytes")
}

func TestPipelineVersionWebhook_ValidateCreate_RejectsInvalidLimit(t *testing.T) {
	pipelineWebhook, validPipelineSpecJSON := setupPipelineWebhookTest(t)
	setPipelineVersionObjectSizeConfig(t, "1.5MiB")

	_, err := pipelineWebhook.ValidateCreate(context.TODO(), newPipelineVersionWithAnnotation(validPipelineSpecJSON, 0))
	require.Error(t, err)
	assert.True(t, apierrors.IsInternalError(err))
	assert.Contains(t, err.Error(), common.MaxPipelineVersionObjectBytesConfig)
}

func newPipelineWebhookWithPipelineLookupError(t *testing.T, lookupErr error) *PipelineVersionsWebhook {
	scheme := runtime.NewScheme()
	require.NoError(t, k8sapi.AddToScheme(scheme))
	failingClient := k8sfake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(_ context.Context, _ client.WithWatch, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
			return lookupErr
		},
	}).Build()
	return &PipelineVersionsWebhook{Client: failingClient, ClientNoCache: failingClient}
}

func TestPipelineVersionWebhook_Default_ClassifiesPipelineLookupErrors(t *testing.T) {
	pipelineVersion := &k8sapi.PipelineVersion{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pipeline-v1", Namespace: "default"},
		Spec:       k8sapi.PipelineVersionSpec{PipelineName: "test-pipeline"},
	}
	pipelineResource := schema.GroupResource{Group: k8sapi.GroupVersion.Group, Resource: "pipelines"}

	testCases := []struct {
		name           string
		lookupErr      error
		expectedStatus int32
	}{
		{"missing pipeline is a caller error", apierrors.NewNotFound(pipelineResource, "test-pipeline"), http.StatusBadRequest},
		{"unavailable Kubernetes API is a server error", apierrors.NewServiceUnavailable("Kubernetes API temporarily unavailable"), http.StatusInternalServerError},
		{"unexpected lookup failure is a server error", errors.New("connection reset"), http.StatusInternalServerError},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			pipelineWebhook := newPipelineWebhookWithPipelineLookupError(t, testCase.lookupErr)
			err := pipelineWebhook.Default(context.TODO(), pipelineVersion.DeepCopy())
			var statusErr apierrors.APIStatus
			require.True(t, errors.As(err, &statusErr))
			assert.Equal(t, testCase.expectedStatus, statusErr.Status().Code)
		})
	}
}

// sendAdmissionReview exercises the HTTP handlers returned by NewPipelineVersionWebhook, which is
// what the Kubernetes API server calls, and returns the admission response.
func sendAdmissionReview(t *testing.T, handler http.Handler, pipelineVersion *k8sapi.PipelineVersion) *admissionv1.AdmissionResponse {
	t.Helper()
	pipelineVersion.TypeMeta = metav1.TypeMeta{APIVersion: k8sapi.GroupVersion.String(), Kind: "PipelineVersion"}
	rawObject, err := json.Marshal(pipelineVersion)
	require.NoError(t, err)
	review := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"},
		Request: &admissionv1.AdmissionRequest{
			UID:       types.UID("test-request"),
			Kind:      metav1.GroupVersionKind{Group: k8sapi.GroupVersion.Group, Version: k8sapi.GroupVersion.Version, Kind: "PipelineVersion"},
			Resource:  metav1.GroupVersionResource{Group: k8sapi.GroupVersion.Group, Version: k8sapi.GroupVersion.Version, Resource: "pipelineversions"},
			Namespace: pipelineVersion.Namespace,
			Operation: admissionv1.Create,
			Object:    runtime.RawExtension{Raw: rawObject},
		},
	}
	body, err := json.Marshal(review)
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodPost, "/webhooks", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code)

	var response admissionv1.AdmissionReview
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.NotNil(t, response.Response)
	return response.Response
}

func TestPipelineVersionWebhook_AdmissionHandlers_StatusCodes(t *testing.T) {
	setPipelineVersionObjectSizeConfig(t, nil)
	pipelineWebhook, validPipelineSpecJSON := setupPipelineWebhookTest(t)
	validating, mutating, err := NewPipelineVersionWebhook(pipelineWebhook.Client, pipelineWebhook.Client)
	require.NoError(t, err)

	response := sendAdmissionReview(t, validating, newPipelineVersionWithAnnotation(validPipelineSpecJSON, 0))
	assert.True(t, response.Allowed, "a valid PipelineVersion is admitted")

	response = sendAdmissionReview(t, validating, newPipelineVersionWithAnnotation(validPipelineSpecJSON, common.DefaultPipelineVersionObjectBytes))
	require.False(t, response.Allowed)
	assert.Equal(t, int32(http.StatusBadRequest), response.Result.Code)
	assert.Contains(t, response.Result.Message, "too large to store in Kubernetes")
	require.NotNil(t, response.Result.Details, "the denial carries the cause the KFP API uses to report invalid input")
	require.Len(t, response.Result.Details.Causes, 1)
	assert.Equal(t, k8sapi.PipelineVersionRejectedCause, response.Result.Details.Causes[0].Type)
	assert.Equal(t, response.Result.Message, response.Result.Details.Causes[0].Message)

	setPipelineVersionObjectSizeConfig(t, []interface{}{1024})
	response = sendAdmissionReview(t, validating, newPipelineVersionWithAnnotation(validPipelineSpecJSON, 0))
	require.False(t, response.Allowed)
	assert.Equal(t, int32(http.StatusInternalServerError), response.Result.Code)
	assert.Contains(t, response.Result.Message, "misconfigured")
	assert.False(t, hasRejectedCause(response.Result), "server-side failures must not be reported as invalid input")

	failingWebhook := newPipelineWebhookWithPipelineLookupError(t, apierrors.NewServiceUnavailable("Kubernetes API temporarily unavailable"))
	_, failingMutating, err := NewPipelineVersionWebhook(failingWebhook.Client, failingWebhook.ClientNoCache)
	require.NoError(t, err)
	response = sendAdmissionReview(t, failingMutating, newPipelineVersionWithAnnotation(validPipelineSpecJSON, 0))
	require.False(t, response.Allowed)
	assert.Equal(t, int32(http.StatusInternalServerError), response.Result.Code)
	assert.False(t, hasRejectedCause(response.Result), "server-side failures must not be reported as invalid input")

	response = sendAdmissionReview(t, mutating, newPipelineVersionWithAnnotation(validPipelineSpecJSON, 0))
	assert.True(t, response.Allowed, "the mutating webhook admits a PipelineVersion whose Pipeline exists")
}

func hasRejectedCause(status *metav1.Status) bool {
	return apierrors.HasStatusCause(&apierrors.StatusError{ErrStatus: *status}, k8sapi.PipelineVersionRejectedCause)
}
