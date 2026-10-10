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

package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/golang/glog"
	"github.com/kubeflow/pipelines/backend/src/apiserver/common"
	"github.com/kubeflow/pipelines/backend/src/apiserver/list"
	"github.com/kubeflow/pipelines/backend/src/apiserver/model"
	"github.com/kubeflow/pipelines/backend/src/common/util"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/kubeflow/pipelines/backend/src/crd/kubernetes/v2beta1"
)

func TestBackwardCompat_GetPipelineVersion_LegacyCR(t *testing.T) {
	podNamespace := viper.Get("POD_NAMESPACE")
	viper.Set("POD_NAMESPACE", "Test")
	defer viper.Set("POD_NAMESPACE", podNamespace)

	store := NewPipelineStoreKubernetes(getClient())

	// The pre-loaded "test-pipeline-version-3" CR is legacy-style (bare metadata.name, no spec.VersionName)
	pv, err := store.GetPipelineVersion(DefaultFakePipelineIdTwo)
	require.NoError(t, err)
	assert.Equal(t, "test-pipeline-version-3", pv.Name)
}

func TestBackwardCompat_GetPipelineVersionByName_LegacyCR_WrongPipeline(t *testing.T) {
	podNamespace := viper.Get("POD_NAMESPACE")
	viper.Set("POD_NAMESPACE", "Test")
	defer viper.Set("POD_NAMESPACE", podNamespace)

	scheme := runtime.NewScheme()
	require.NoError(t, v2beta1.AddToScheme(scheme))

	// Pipeline that owns the version
	ownerPipeline := &v2beta1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			UID:       DefaultFakePipelineIdTwo,
			Name:      "owner-pipeline",
			Namespace: "Test",
		},
	}
	// A different pipeline that does NOT own the version
	otherPipeline := &v2beta1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			UID:       DefaultFakePipelineIdThree,
			Name:      "other-pipeline",
			Namespace: "Test",
		},
	}
	// Legacy-style CR belonging to ownerPipeline
	oldVersion := &v2beta1.PipelineVersion{
		ObjectMeta: metav1.ObjectMeta{
			UID:       DefaultFakePipelineIdFour,
			Name:      "shared-version-name",
			Namespace: "Test",
			Labels: map[string]string{
				"pipelines.kubeflow.org/pipeline-id": DefaultFakePipelineIdTwo,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: v2beta1.GroupVersion.String(),
					Kind:       "Pipeline",
					UID:        DefaultFakePipelineIdTwo,
					Name:       "owner-pipeline",
				},
			},
		},
		Spec: v2beta1.PipelineVersionSpec{
			PipelineName: "owner-pipeline",
			PipelineSpec: getBasicPipelineSpec(),
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(ownerPipeline, otherPipeline, oldVersion).
		Build()

	store := NewPipelineStoreKubernetes(k8sClient, k8sClient)

	// Looking up with the wrong pipeline should fail with NotFound
	_, err := store.GetPipelineVersionByName(DefaultFakePipelineIdThree, "shared-version-name")
	require.NotNil(t, err)
	assert.Equal(t, codes.NotFound, err.(*util.UserError).ExternalStatusCode())

	// Looking up with the correct pipeline should succeed
	pv, err := store.GetPipelineVersionByName(DefaultFakePipelineIdTwo, "shared-version-name")
	require.NoError(t, err)
	assert.Equal(t, "shared-version-name", pv.Name)
}

func TestBackwardCompat_GetPipelineVersionByName_OwnerRefLabelMismatch(t *testing.T) {
	podNamespace := viper.Get("POD_NAMESPACE")
	viper.Set("POD_NAMESPACE", "Test")
	defer viper.Set("POD_NAMESPACE", podNamespace)

	scheme := runtime.NewScheme()
	require.NoError(t, v2beta1.AddToScheme(scheme))

	pipelineA := &v2beta1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			UID:       DefaultFakePipelineIdTwo,
			Name:      "pipeline-a",
			Namespace: "Test",
		},
	}
	pipelineB := &v2beta1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			UID:       DefaultFakePipelineIdThree,
			Name:      "pipeline-b",
			Namespace: "Test",
		},
	}
	// CR with OwnerReference pointing to pipeline A but label edited to pipeline B
	mismatchVersion := &v2beta1.PipelineVersion{
		ObjectMeta: metav1.ObjectMeta{
			UID:       DefaultFakePipelineIdFour,
			Name:      "mismatch-version",
			Namespace: "Test",
			Labels: map[string]string{
				"pipelines.kubeflow.org/pipeline-id": DefaultFakePipelineIdThree,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: v2beta1.GroupVersion.String(),
					Kind:       "Pipeline",
					UID:        DefaultFakePipelineIdTwo,
					Name:       "pipeline-a",
				},
			},
		},
		Spec: v2beta1.PipelineVersionSpec{
			PipelineName: "pipeline-a",
			PipelineSpec: getBasicPipelineSpec(),
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pipelineA, pipelineB, mismatchVersion).
		Build()

	store := NewPipelineStoreKubernetes(k8sClient, k8sClient)

	// OwnerRef points to A, label points to B. Querying with B should reject
	// because ownership is determined by OwnerReferences.
	_, err := store.GetPipelineVersionByName(DefaultFakePipelineIdThree, "mismatch-version")
	require.NotNil(t, err)
	assert.Equal(t, codes.NotFound, err.(*util.UserError).ExternalStatusCode())

	// Querying with A (the true owner) should succeed
	pv, err := store.GetPipelineVersionByName(DefaultFakePipelineIdTwo, "mismatch-version")
	require.NoError(t, err)
	assert.Equal(t, "mismatch-version", pv.Name)
}

func TestBackwardCompat_ListPipelineVersions_MixedCRs(t *testing.T) {
	podNamespace := viper.Get("POD_NAMESPACE")
	viper.Set("POD_NAMESPACE", "Test")
	defer viper.Set("POD_NAMESPACE", podNamespace)

	store := NewPipelineStoreKubernetes(getClient())

	// The pre-loaded "test-pipeline-version-3" is legacy-style (bare name, under DefaultFakePipelineIdTwo).
	// Create a new-style version under the same pipeline.
	_, err := store.CreatePipelineVersion(&model.PipelineVersion{
		Name:         "new-style-version",
		PipelineId:   DefaultFakePipelineIdTwo,
		PipelineSpec: model.LargeText(getBasicPipelineSpecYAML()),
	})
	require.NoError(t, err)

	options := list.EmptyOptions()
	versions, total, _, err := store.ListPipelineVersions(DefaultFakePipelineIdTwo, options, nil)
	require.NoError(t, err)
	require.Equal(t, 2, total)

	// Both old and new versions should have correct bare names
	names := make([]string, len(versions))
	for i, v := range versions {
		names[i] = v.Name
	}
	assert.Contains(t, names, "test-pipeline-version-3")
	assert.Contains(t, names, "new-style-version")
}

func TestBackwardCompat_GetDefaultPipelineVersion_MixedCRs(t *testing.T) {
	podNamespace := viper.Get("POD_NAMESPACE")
	viper.Set("POD_NAMESPACE", "Test")
	defer viper.Set("POD_NAMESPACE", podNamespace)

	k8sClient, k8sClientNoCache := getClient()
	store := NewPipelineStoreKubernetes(k8sClient, k8sClientNoCache)

	// Create a new-style version alongside the pre-seeded legacy "test-pipeline-version-3".
	_, err := store.CreatePipelineVersion(&model.PipelineVersion{
		Name:         "new-latest-version",
		PipelineId:   DefaultFakePipelineIdTwo,
		PipelineSpec: model.LargeText(getBasicPipelineSpecYAML()),
	})
	require.NoError(t, err)

	// The fake client leaves creationTimestamp at zero, so stamp the new version to make it
	// genuinely newer than the legacy CR rather than relying on list ordering.
	ctx := context.Background()
	versions := &v2beta1.PipelineVersionList{}
	require.NoError(t, k8sClient.List(ctx, versions, client.InNamespace("Test")))

	stamped := false
	for i := range versions.Items {
		if versions.Items[i].Spec.VersionName != "new-latest-version" {
			continue
		}
		versions.Items[i].CreationTimestamp = metav1.Unix(1700000000, 0)
		require.NoError(t, k8sClient.Update(ctx, &versions.Items[i]))
		stamped = true
	}
	require.True(t, stamped, "expected the newly created pipeline version to be present")

	latest, err := store.GetDefaultPipelineVersion(DefaultFakePipelineIdTwo)
	require.NoError(t, err)
	assert.Equal(t, "new-latest-version", latest.Name)
}

func TestBackwardCompat_DeletePipelineVersion_LegacyCR(t *testing.T) {
	podNamespace := viper.Get("POD_NAMESPACE")
	viper.Set("POD_NAMESPACE", "Test")
	defer viper.Set("POD_NAMESPACE", podNamespace)

	store := NewPipelineStoreKubernetes(getClient())

	// Delete legacy-style CR by UUID
	err := store.DeletePipelineVersion(DefaultFakePipelineIdTwo)
	require.NoError(t, err)

	// Verify it's gone
	_, err = store.GetPipelineVersion(DefaultFakePipelineIdTwo)
	require.NotNil(t, err)
	assert.Equal(t, err.(*util.UserError).ExternalStatusCode(), codes.NotFound)
}

func TestCreatePipelineVersion_DuplicateUnderSamePipeline(t *testing.T) {
	podNamespace := viper.Get("POD_NAMESPACE")
	viper.Set("POD_NAMESPACE", "Test")
	defer viper.Set("POD_NAMESPACE", podNamespace)

	store := NewPipelineStoreKubernetes(getClient())

	pipeline, err := store.CreatePipeline(&model.Pipeline{
		Name:      "dup-test-pipeline",
		Namespace: "Test",
	})
	require.NoError(t, err)

	_, err = store.CreatePipelineVersion(&model.PipelineVersion{
		Name:         "v1",
		PipelineId:   pipeline.UUID,
		PipelineSpec: model.LargeText(getBasicPipelineSpecYAML()),
	})
	require.NoError(t, err)

	// Second creation with same name under same pipeline should fail
	_, err = store.CreatePipelineVersion(&model.PipelineVersion{
		Name:         "v1",
		PipelineId:   pipeline.UUID,
		PipelineSpec: model.LargeText(getBasicPipelineSpecYAML()),
	})
	require.NotNil(t, err)
	assert.Contains(t, err.Error(), "already exist")
}

func TestCreatePipelineAndPipelineVersion_SameVersionNameDifferentPipelines(t *testing.T) {
	podNamespace := viper.Get("POD_NAMESPACE")
	viper.Set("POD_NAMESPACE", "Test")
	defer viper.Set("POD_NAMESPACE", podNamespace)

	store := NewPipelineStoreKubernetes(getClient())

	_, _, err := store.CreatePipelineAndPipelineVersion(
		&model.Pipeline{Name: "atomic-pipeline-a"},
		&model.PipelineVersion{
			Name:         "initial",
			PipelineSpec: model.LargeText(getBasicPipelineSpecYAML()),
		},
	)
	require.NoError(t, err)

	_, _, err = store.CreatePipelineAndPipelineVersion(
		&model.Pipeline{Name: "atomic-pipeline-b"},
		&model.PipelineVersion{
			Name:         "initial",
			PipelineSpec: model.LargeText(getBasicPipelineSpecYAML()),
		},
	)
	require.NoError(t, err)
}

func TestCreatePipelineVersion_HyphenCollision(t *testing.T) {
	podNamespace := viper.Get("POD_NAMESPACE")
	viper.Set("POD_NAMESPACE", "Test")
	defer viper.Set("POD_NAMESPACE", podNamespace)

	scheme := runtime.NewScheme()
	require.NoError(t, v2beta1.AddToScheme(scheme))

	pipelineFoo := &v2beta1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			UID:       DefaultFakePipelineIdThree,
			Name:      "foo",
			Namespace: "Test",
		},
	}
	pipelineFooBar := &v2beta1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			UID:       DefaultFakePipelineIdFour,
			Name:      "foo-bar",
			Namespace: "Test",
		},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pipelineFoo, pipelineFooBar).
		Build()

	store := NewPipelineStoreKubernetes(k8sClient, k8sClient)

	// Pipeline "foo" + version "bar-baz" → composite "foo-bar-baz"
	_, err := store.CreatePipelineVersion(&model.PipelineVersion{
		Name:         "bar-baz",
		PipelineId:   DefaultFakePipelineIdThree,
		PipelineSpec: model.LargeText(getBasicPipelineSpecYAML()),
	})
	require.NoError(t, err)

	// Pipeline "foo-bar" + version "baz" → also composite "foo-bar-baz"
	// Known limitation: this collides because both produce the same composite K8s name
	_, err = store.CreatePipelineVersion(&model.PipelineVersion{
		Name:         "baz",
		PipelineId:   DefaultFakePipelineIdFour,
		PipelineSpec: model.LargeText(getBasicPipelineSpecYAML()),
	})
	require.NotNil(t, err, "Expected collision due to identical composite K8s names")
	assert.Contains(t, err.Error(), "already exist")
	assert.Contains(t, err.Error(), "foo-bar-baz")
}

func TestCreatePipelineVersion_DuplicateLegacyBareName(t *testing.T) {
	podNamespace := viper.Get("POD_NAMESPACE")
	viper.Set("POD_NAMESPACE", "Test")
	defer viper.Set("POD_NAMESPACE", podNamespace)

	scheme := runtime.NewScheme()
	require.NoError(t, v2beta1.AddToScheme(scheme))

	pipeline := &v2beta1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			UID:       DefaultFakePipelineIdThree,
			Name:      "my-pipeline",
			Namespace: "Test",
		},
	}

	// Pre-seed a legacy bare-name CR "v1" owned by the same pipeline
	legacyVersion := &v2beta1.PipelineVersion{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "v1",
			Namespace: "Test",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: v2beta1.GroupVersion.String(),
					Kind:       "Pipeline",
					UID:        DefaultFakePipelineIdThree,
					Name:       "my-pipeline",
				},
			},
		},
		Spec: v2beta1.PipelineVersionSpec{
			PipelineSpec: getBasicPipelineSpec(),
			PipelineName: "my-pipeline",
			VersionName:  "v1",
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pipeline, legacyVersion).
		Build()

	store := NewPipelineStoreKubernetes(k8sClient, k8sClient)

	// Creating a new version "v1" under the same pipeline should fail,
	// even though the composite K8s name "my-pipeline-v1" differs from legacy "v1"
	_, err := store.CreatePipelineVersion(&model.PipelineVersion{
		Name:         "v1",
		PipelineId:   DefaultFakePipelineIdThree,
		PipelineSpec: model.LargeText(getBasicPipelineSpecYAML()),
	})
	require.NotNil(t, err, "Expected duplicate rejection against legacy bare-name CR")
	assert.Contains(t, err.Error(), "already exist")
}

func TestCreatePipelineVersion_TransientErrorDuringLookup(t *testing.T) {
	podNamespace := viper.Get("POD_NAMESPACE")
	viper.Set("POD_NAMESPACE", "Test")
	defer viper.Set("POD_NAMESPACE", podNamespace)

	scheme := runtime.NewScheme()
	require.NoError(t, v2beta1.AddToScheme(scheme))

	pipeline := &v2beta1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			UID:       DefaultFakePipelineIdThree,
			Name:      "my-pipeline",
			Namespace: "Test",
		},
	}

	legacyVersion := &v2beta1.PipelineVersion{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "v1",
			Namespace: "Test",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: v2beta1.GroupVersion.String(),
					Kind:       "Pipeline",
					UID:        DefaultFakePipelineIdThree,
					Name:       "my-pipeline",
				},
			},
		},
		Spec: v2beta1.PipelineVersionSpec{
			PipelineSpec: getBasicPipelineSpec(),
			PipelineName: "my-pipeline",
			VersionName:  "v1",
		},
	}

	// Inject a transient error on the first Get call (the one inside
	// getPipelineVersionByNameInNamespace during the pre-create collision check).
	getCallCount := 0
	transientError := fmt.Errorf("simulated transient API server error")
	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pipeline, legacyVersion).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, client client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*v2beta1.PipelineVersion); ok {
					getCallCount++
					if getCallCount == 1 {
						return transientError
					}
				}
				return client.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	store := NewPipelineStoreKubernetes(k8sClient, k8sClient)

	_, err := store.CreatePipelineVersion(&model.PipelineVersion{
		Name:         "v1",
		PipelineId:   DefaultFakePipelineIdThree,
		PipelineSpec: model.LargeText(getBasicPipelineSpecYAML()),
	})
	require.NotNil(t, err, "Expected error to propagate from transient lookup failure")
	assert.Contains(t, err.Error(), "simulated transient")
}

func TestGetPipelineVersionByName_InvalidPipelineId(t *testing.T) {
	podNamespace := viper.Get("POD_NAMESPACE")
	viper.Set("POD_NAMESPACE", "Test")
	defer viper.Set("POD_NAMESPACE", podNamespace)

	store := NewPipelineStoreKubernetes(getClient())

	_, err := store.GetPipelineVersionByName("nonexistent-pipeline-id", "v1.0")
	require.NotNil(t, err)
	assert.Equal(t, codes.NotFound, err.(*util.UserError).ExternalStatusCode())
}

func TestGetPipelineVersionByName_HyphenCollisionFallthrough(t *testing.T) {
	podNamespace := viper.Get("POD_NAMESPACE")
	viper.Set("POD_NAMESPACE", "Test")
	defer viper.Set("POD_NAMESPACE", podNamespace)

	scheme := runtime.NewScheme()
	require.NoError(t, v2beta1.AddToScheme(scheme))

	// Pipeline "foo" and pipeline "foo-bar"
	pipelineFoo := &v2beta1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			UID:       DefaultFakePipelineIdThree,
			Name:      "foo",
			Namespace: "Test",
		},
	}
	pipelineFooBar := &v2beta1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			UID:       DefaultFakePipelineIdFour,
			Name:      "foo-bar",
			Namespace: "Test",
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pipelineFoo, pipelineFooBar).
		Build()

	store := NewPipelineStoreKubernetes(k8sClient, k8sClient)

	// Create "foo" + "bar-baz" → composite "foo-bar-baz", owned by pipelineFoo
	_, err := store.CreatePipelineVersion(&model.PipelineVersion{
		Name:         "bar-baz",
		PipelineId:   DefaultFakePipelineIdThree,
		PipelineSpec: model.LargeText(getBasicPipelineSpecYAML()),
	})
	require.NoError(t, err)

	// Lookup "foo-bar" + "baz" → composite is also "foo-bar-baz", but owned by
	// pipelineFoo, not pipelineFooBar. The code should detect the ownership
	// mismatch and fall through to bare-name lookup, which also fails → NotFound.
	_, err = store.GetPipelineVersionByName(DefaultFakePipelineIdFour, "baz")
	require.NotNil(t, err)
	assert.Equal(t, codes.NotFound, err.(*util.UserError).ExternalStatusCode())
}

func TestGetPipelineVersionByName_HyphenCollisionFallbackSuccess(t *testing.T) {
	podNamespace := viper.Get("POD_NAMESPACE")
	viper.Set("POD_NAMESPACE", "Test")
	defer viper.Set("POD_NAMESPACE", podNamespace)

	scheme := runtime.NewScheme()
	require.NoError(t, v2beta1.AddToScheme(scheme))

	pipelineFoo := &v2beta1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			UID:       DefaultFakePipelineIdThree,
			Name:      "foo",
			Namespace: "Test",
		},
	}
	pipelineFooBar := &v2beta1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			UID:       DefaultFakePipelineIdFour,
			Name:      "foo-bar",
			Namespace: "Test",
		},
	}

	// Legacy bare-name CR owned by pipeline "foo-bar"
	legacyVersion := &v2beta1.PipelineVersion{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "legacy-version-uid",
			Name:      "baz",
			Namespace: "Test",
			Labels: map[string]string{
				"pipelines.kubeflow.org/pipeline-id": DefaultFakePipelineIdFour,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: v2beta1.GroupVersion.String(),
					Kind:       "Pipeline",
					UID:        DefaultFakePipelineIdFour,
					Name:       "foo-bar",
				},
			},
		},
		Spec: v2beta1.PipelineVersionSpec{
			PipelineName: "foo-bar",
			PipelineSpec: getBasicPipelineSpec(),
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pipelineFoo, pipelineFooBar, legacyVersion).
		Build()

	store := NewPipelineStoreKubernetes(k8sClient, k8sClient)

	// Pipeline "foo" + version "bar-baz" → composite "foo-bar-baz"
	_, err := store.CreatePipelineVersion(&model.PipelineVersion{
		Name:         "bar-baz",
		PipelineId:   DefaultFakePipelineIdThree,
		PipelineSpec: model.LargeText(getBasicPipelineSpecYAML()),
	})
	require.NoError(t, err)

	// Lookup "foo-bar" + "baz": composite "foo-bar-baz" exists but is owned by
	// pipeline "foo". The code detects the ownership mismatch, falls back to
	// bare-name "baz", and finds the legacy CR owned by pipeline "foo-bar".
	pv, err := store.GetPipelineVersionByName(DefaultFakePipelineIdFour, "baz")
	require.NoError(t, err)
	assert.Equal(t, "baz", pv.Name)
	assert.Equal(t, DefaultFakePipelineIdFour, pv.PipelineId)
	assert.Equal(t, "legacy-version-uid", pv.UUID)
}

func TestCreatePipelineAndPipelineVersion_InvalidVersionName(t *testing.T) {
	podNamespace := viper.Get("POD_NAMESPACE")
	viper.Set("POD_NAMESPACE", "Test")
	defer viper.Set("POD_NAMESPACE", podNamespace)

	store := NewPipelineStoreKubernetes(getClient())

	// Invalid version name should fail before creating the pipeline (no orphan)
	_, _, err := store.CreatePipelineAndPipelineVersion(
		&model.Pipeline{Name: "should-not-be-created"},
		&model.PipelineVersion{
			Name:         "INVALID-UPPERCASE",
			PipelineSpec: model.LargeText(getBasicPipelineSpecYAML()),
		},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid pipeline version name")

	// Verify the pipeline was NOT created
	_, err = store.GetPipelineByNameAndNamespace("should-not-be-created", "")
	require.NotNil(t, err)
	assert.Equal(t, codes.NotFound, err.(*util.UserError).ExternalStatusCode())
}

func TestGetPipelineVersionByName_DifferentNamespace(t *testing.T) {
	podNamespace := viper.Get("POD_NAMESPACE")
	viper.Set("POD_NAMESPACE", "DefaultNS")
	defer viper.Set("POD_NAMESPACE", podNamespace)

	multiUser := viper.Get("MULTIUSER")
	viper.Set("MULTIUSER", "true")
	defer viper.Set("MULTIUSER", multiUser)

	scheme := runtime.NewScheme()
	require.NoError(t, v2beta1.AddToScheme(scheme))

	pipeline := &v2beta1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			UID:       DefaultFakePipelineIdThree,
			Name:      "user-pipeline",
			Namespace: "UserNS",
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pipeline).
		Build()

	store := NewPipelineStoreKubernetes(k8sClient, k8sClient)

	_, err := store.CreatePipelineVersion(&model.PipelineVersion{
		Name:         "v1",
		PipelineId:   DefaultFakePipelineIdThree,
		PipelineSpec: model.LargeText(getBasicPipelineSpecYAML()),
	})
	require.NoError(t, err)

	// POD_NAMESPACE is "DefaultNS" but the version lives in "UserNS"
	version, err := store.GetPipelineVersionByName(DefaultFakePipelineIdThree, "v1")
	require.NoError(t, err)
	assert.Equal(t, "v1", version.Name)
	assert.Equal(t, DefaultFakePipelineIdThree, version.PipelineId)
}

func getClientWithTwoPipelines() (client.Client, client.Client) {
	scheme := runtime.NewScheme()
	err := v2beta1.AddToScheme(scheme)
	if err != nil {
		glog.Fatalf("Failed to add to scheme: %v", err)
	}

	pipelineAlpha := &v2beta1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			UID:       DefaultFakePipelineIdThree,
			Name:      "pipeline-alpha",
			Namespace: "Test",
		},
		Spec: v2beta1.PipelineSpec{
			Description: "Pipeline Alpha",
		},
	}

	pipelineBeta := &v2beta1.Pipeline{
		ObjectMeta: metav1.ObjectMeta{
			UID:       DefaultFakePipelineIdFour,
			Name:      "pipeline-beta",
			Namespace: "Test",
		},
		Spec: v2beta1.PipelineSpec{
			Description: "Pipeline Beta",
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pipelineAlpha, pipelineBeta).
		Build()

	return k8sClient, k8sClient
}

// webhookDenial builds the error the Kubernetes API server returns when an admission webhook denies a
// request: it keeps the webhook's status and prefixes the message (see ToStatusErr in k8s.io/apiserver).
func webhookDenial(status metav1.Status) error {
	status.Message = `admission webhook "pipelineversions.pipelines.kubeflow.org" denied the request: ` + status.Message
	return &k8serrors.StatusError{ErrStatus: status}
}

func TestCreatePipelineVersion_MapsKubernetesCreateErrors(t *testing.T) {
	podNamespace := viper.Get("POD_NAMESPACE")
	viper.Set("POD_NAMESPACE", "Test")
	defer viper.Set("POD_NAMESPACE", podNamespace)

	const webhookMessage = "The pipeline version is too large to store in Kubernetes: the PipelineVersion object is 2000000 bytes"
	sizeAdvice := common.PipelineVersionRejectedByKubernetesMessage()

	testCases := []struct {
		name            string
		createError     error
		expectedCode    codes.Code
		expectedMessage string
	}{
		// The size errors match the Status responses of a Kubernetes v1.37 kind cluster.
		{
			name:            "etcd request too large",
			createError:     &k8serrors.StatusError{ErrStatus: metav1.Status{Status: metav1.StatusFailure, Code: 500, Message: "etcdserver: request is too large"}},
			expectedCode:    codes.InvalidArgument,
			expectedMessage: sizeAdvice,
		},
		{
			name: "API server etcd client send limit",
			createError: &k8serrors.StatusError{ErrStatus: metav1.Status{Status: metav1.StatusFailure, Code: 500,
				Message: "rpc error: code = ResourceExhausted desc = trying to send message larger than max (2097703 vs. 2097152)"}},
			expectedCode:    codes.InvalidArgument,
			expectedMessage: sizeAdvice,
		},
		{
			name:            "API server request entity too large",
			createError:     k8serrors.NewRequestEntityTooLargeError("limit is 3145728"),
			expectedCode:    codes.InvalidArgument,
			expectedMessage: sizeAdvice,
		},
		{
			name: "KFP webhook rejects the submitted object",
			createError: webhookDenial(metav1.Status{Status: metav1.StatusFailure, Code: 400, Reason: metav1.StatusReasonBadRequest,
				Message: webhookMessage,
				Details: &metav1.StatusDetails{Causes: []metav1.StatusCause{{Type: v2beta1.PipelineVersionRejectedCause, Message: webhookMessage}}}}),
			expectedCode:    codes.InvalidArgument,
			expectedMessage: webhookMessage,
		},
		{
			// Only the KFP cause marks a denial as invalid input; any other 400 is a server-side problem.
			name:            "bad request without the KFP cause",
			createError:     k8serrors.NewBadRequest("the server rejected our request for an unknown reason"),
			expectedCode:    codes.Internal,
			expectedMessage: "Internal Server Error",
		},
		{
			// The API server keeps a webhook's 5xx status, so webhook dependency failures stay server errors.
			name: "validating webhook infrastructure failure",
			createError: webhookDenial(metav1.Status{Status: metav1.StatusFailure, Code: 500, Reason: metav1.StatusReasonInternalError,
				Message: "Internal error occurred: failed to get the Pipeline Test/my-pipeline: Kubernetes API temporarily unavailable"}),
			expectedCode:    codes.Internal,
			expectedMessage: "Internal Server Error",
		},
		{
			name:            "unexpected error",
			createError:     k8serrors.NewInternalError(fmt.Errorf("simulated failure")),
			expectedCode:    codes.Internal,
			expectedMessage: "Internal Server Error",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, v2beta1.AddToScheme(scheme))

			pipeline := &v2beta1.Pipeline{
				ObjectMeta: metav1.ObjectMeta{
					UID:       DefaultFakePipelineIdThree,
					Name:      "my-pipeline",
					Namespace: "Test",
				},
			}
			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(pipeline).
				WithInterceptorFuncs(interceptor.Funcs{
					Create: func(ctx context.Context, client client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
						if _, ok := obj.(*v2beta1.PipelineVersion); ok {
							return testCase.createError
						}
						return client.Create(ctx, obj, opts...)
					},
				}).
				Build()

			store := NewPipelineStoreKubernetes(k8sClient, k8sClient)

			_, err := store.CreatePipelineVersion(&model.PipelineVersion{
				Name:         "v1",
				PipelineId:   DefaultFakePipelineIdThree,
				PipelineSpec: model.LargeText(getBasicPipelineSpecYAML()),
			})
			require.Error(t, err)
			var userError *util.UserError
			require.True(t, errors.As(err, &userError))
			assert.Equal(t, testCase.expectedCode, userError.ExternalStatusCode())
			assert.Contains(t, userError.ExternalMessage(), testCase.expectedMessage)
			assert.NotContains(t, userError.ExternalMessage(), "admission webhook", "clients see the KFP message, not the API server's wrapping")
			// The gRPC status message shown by clients contains each piece of text exactly once.
			grpcMessage := userError.GRPCStatus().Message()
			if testCase.expectedCode == codes.InvalidArgument {
				assert.Equal(t, 1, strings.Count(grpcMessage, testCase.expectedMessage), grpcMessage)
			}
			if kubernetesMessage := testCase.createError.Error(); testCase.expectedMessage == sizeAdvice {
				assert.Equal(t, 1, strings.Count(grpcMessage, kubernetesMessage), grpcMessage)
			}
		})
	}
}

// pipelineCleanupHarness is a fake Kubernetes client whose first PipelineVersion create runs onVersionCreate
// in place of the real create, and which records the options of every Pipeline delete.
type pipelineCleanupHarness struct {
	client          client.WithWatch
	store           *PipelineStoreKubernetes
	pipelineDeletes []*client.DeleteOptions
}

func newPipelineCleanupHarness(t *testing.T, onVersionCreate func(ctx context.Context, c client.WithWatch, version *v2beta1.PipelineVersion) error) *pipelineCleanupHarness {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, v2beta1.AddToScheme(scheme))

	harness := &pipelineCleanupHarness{}
	intercepted := false
	harness.client = fake.NewClientBuilder().
		WithScheme(scheme).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if pipeline, ok := obj.(*v2beta1.Pipeline); ok && pipeline.UID == "" {
					// The fake client does not assign UIDs; Kubernetes always does.
					pipeline.UID = types.UID(fmt.Sprintf("uid-%s", pipeline.Name))
				}
				if version, ok := obj.(*v2beta1.PipelineVersion); ok && !intercepted {
					intercepted = true
					return onVersionCreate(ctx, c, version)
				}
				return c.Create(ctx, obj, opts...)
			},
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if _, ok := obj.(*v2beta1.Pipeline); ok {
					deleteOptions := &client.DeleteOptions{}
					deleteOptions.ApplyOptions(opts)
					harness.pipelineDeletes = append(harness.pipelineDeletes, deleteOptions)
				}
				return c.Delete(ctx, obj, opts...)
			},
		}).
		Build()
	harness.store = NewPipelineStoreKubernetes(harness.client, harness.client)
	return harness
}

func newLargePipelineRequest() (*model.Pipeline, *model.PipelineVersion) {
	return &model.Pipeline{Name: "large-pipeline", Namespace: "Test"},
		&model.PipelineVersion{Name: "large-pipeline", PipelineSpec: model.LargeText(getBasicPipelineSpecYAML())}
}

func (h *pipelineCleanupHarness) objectCounts(t *testing.T) (pipelines int, versions int) {
	t.Helper()
	pipelineList := &v2beta1.PipelineList{}
	require.NoError(t, h.client.List(context.TODO(), pipelineList))
	versionList := &v2beta1.PipelineVersionList{}
	require.NoError(t, h.client.List(context.TODO(), versionList))
	return len(pipelineList.Items), len(versionList.Items)
}

func TestCreatePipelineAndPipelineVersion_DeletesPipelineWhenVersionIsRefused(t *testing.T) {
	harness := newPipelineCleanupHarness(t, func(context.Context, client.WithWatch, *v2beta1.PipelineVersion) error {
		return webhookDenial(metav1.Status{Status: metav1.StatusFailure, Code: 400, Reason: metav1.StatusReasonBadRequest,
			Message: "The pipeline version is too large to store in Kubernetes",
			Details: &metav1.StatusDetails{Causes: []metav1.StatusCause{{
				Type: v2beta1.PipelineVersionRejectedCause, Message: "The pipeline version is too large to store in Kubernetes"}}}})
	})

	_, _, err := harness.store.CreatePipelineAndPipelineVersion(newLargePipelineRequest())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "too large to store in Kubernetes", "the version error is returned, not a cleanup error")

	pipelines, _ := harness.objectCounts(t)
	assert.Zero(t, pipelines, "the Pipeline created for the refused version is deleted")
	require.Len(t, harness.pipelineDeletes, 1)
	require.NotNil(t, harness.pipelineDeletes[0].PropagationPolicy)
	assert.Equal(t, metav1.DeletePropagationOrphan, *harness.pipelineDeletes[0].PropagationPolicy,
		"the cleanup must never garbage-collect pipeline versions")
	require.NotNil(t, harness.pipelineDeletes[0].Preconditions)
	assert.NotNil(t, harness.pipelineDeletes[0].Preconditions.UID, "only the Pipeline created by this request is deleted")

	// The user fixes the pipeline and retries with the same name.
	_, _, err = harness.store.CreatePipelineAndPipelineVersion(newLargePipelineRequest())
	require.NoError(t, err)
}

func TestCreatePipelineAndPipelineVersion_KeepsVersionAttachedConcurrently(t *testing.T) {
	// Another client attaches a valid version to the new Pipeline before this request's version is refused.
	harness := newPipelineCleanupHarness(t, func(ctx context.Context, c client.WithWatch, version *v2beta1.PipelineVersion) error {
		concurrent := version.DeepCopy()
		concurrent.Name = "large-pipeline-concurrent"
		require.NoError(t, c.Create(ctx, concurrent))
		return k8serrors.NewRequestEntityTooLargeError("limit is 3145728")
	})

	_, _, err := harness.store.CreatePipelineAndPipelineVersion(newLargePipelineRequest())
	require.Error(t, err)

	// The fake client does not garbage-collect, so the guarantee is the propagation policy: Kubernetes keeps
	// dependents of a Pipeline deleted with orphan propagation.
	require.Len(t, harness.pipelineDeletes, 1)
	require.NotNil(t, harness.pipelineDeletes[0].PropagationPolicy)
	assert.Equal(t, metav1.DeletePropagationOrphan, *harness.pipelineDeletes[0].PropagationPolicy)
	_, versions := harness.objectCounts(t)
	assert.Equal(t, 1, versions, "the concurrently attached version remains")
}

func TestCreatePipelineAndPipelineVersion_KeepsPipelineWhenOutcomeIsUnknown(t *testing.T) {
	// Kubernetes stores the version, but the response is lost.
	harness := newPipelineCleanupHarness(t, func(ctx context.Context, c client.WithWatch, version *v2beta1.PipelineVersion) error {
		require.NoError(t, c.Create(ctx, version))
		return fmt.Errorf("http2: client connection lost")
	})

	_, _, err := harness.store.CreatePipelineAndPipelineVersion(newLargePipelineRequest())
	require.Error(t, err)

	assert.Empty(t, harness.pipelineDeletes, "a failure that may follow a successful write never triggers cleanup")
	pipelines, versions := harness.objectCounts(t)
	assert.Equal(t, 1, pipelines)
	assert.Equal(t, 1, versions)
}

func TestUpdatePipelineVersionFields_MapsKubernetesUpdateErrors(t *testing.T) {
	podNamespace := viper.Get("POD_NAMESPACE")
	viper.Set("POD_NAMESPACE", "Test")
	defer viper.Set("POD_NAMESPACE", podNamespace)

	const webhookMessage = "This update would make the pipeline version too large to store in Kubernetes"
	testCases := []struct {
		name            string
		updateError     error
		expectedCode    codes.Code
		expectedMessage string
	}{
		{
			name: "KFP webhook rejects the update",
			updateError: webhookDenial(metav1.Status{Status: metav1.StatusFailure, Code: 400, Reason: metav1.StatusReasonBadRequest,
				Message: webhookMessage,
				Details: &metav1.StatusDetails{Causes: []metav1.StatusCause{{Type: v2beta1.PipelineVersionRejectedCause, Message: webhookMessage}}}}),
			expectedCode:    codes.InvalidArgument,
			expectedMessage: webhookMessage,
		},
		{
			name:            "etcd request too large",
			updateError:     &k8serrors.StatusError{ErrStatus: metav1.Status{Status: metav1.StatusFailure, Code: 500, Message: "etcdserver: request is too large"}},
			expectedCode:    codes.InvalidArgument,
			expectedMessage: common.PipelineVersionUpdateRejectedByKubernetesMessage(),
		},
		{
			name:            "unexpected error",
			updateError:     k8serrors.NewInternalError(fmt.Errorf("simulated failure")),
			expectedCode:    codes.Internal,
			expectedMessage: "Internal Server Error",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			require.NoError(t, v2beta1.AddToScheme(scheme))
			version := &v2beta1.PipelineVersion{
				ObjectMeta: metav1.ObjectMeta{UID: DefaultFakePipelineIdTwo, Name: "my-pipeline-v1", Namespace: "Test"},
				Spec:       v2beta1.PipelineVersionSpec{PipelineName: "my-pipeline", DisplayName: "v1"},
			}
			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(version).
				WithInterceptorFuncs(interceptor.Funcs{
					Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
						return testCase.updateError
					},
				}).
				Build()
			store := NewPipelineStoreKubernetes(k8sClient, k8sClient)

			err := store.UpdatePipelineVersionFields(DefaultFakePipelineIdTwo, strings.Repeat("n", 16<<10), nil)
			require.Error(t, err)
			var userError *util.UserError
			require.True(t, errors.As(err, &userError))
			assert.Equal(t, testCase.expectedCode, userError.ExternalStatusCode())
			assert.Contains(t, userError.ExternalMessage(), testCase.expectedMessage)
			assert.NotContains(t, userError.ExternalMessage(), "admission webhook")
		})
	}
}
