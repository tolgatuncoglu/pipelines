package util

import (
	"testing"

	wfapi "github.com/argoproj/argo-workflows/v4/pkg/apis/workflow/v1alpha1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func taskAuthFixture() (*Workflow, TaskRuntimeAuthConfig) {
	config := TaskRuntimeAuthConfig{Audience: "pipelines", DriverImage: "driver:patched", LauncherImage: "launcher:patched", DriverCommand: []string{"driver"}, LauncherCommand: []string{"launcher-v2"}}
	w := NewWorkflow(&wfapi.Workflow{Spec: wfapi.WorkflowSpec{ServiceAccountName: "custom-runner", Templates: []wfapi.Template{
		{Name: "driver", Metadata: wfapi.Metadata{Annotations: map[string]string{AnnotationKeyRuntimeRole: "driver"}}, Container: &corev1.Container{Image: config.DriverImage, Command: config.DriverCommand, Args: []string{"--type", "CONTAINER"}}},
		{Name: "executor", Metadata: wfapi.Metadata{Annotations: map[string]string{AnnotationKeyRuntimeRole: "launcher"}}, Container: &corev1.Container{Image: "user-image", Command: []string{"user-command"}, VolumeMounts: []corev1.VolumeMount{{Name: "kfp-launcher", MountPath: "/kfp-launcher"}}}, InitContainers: []wfapi.UserContainer{{Container: corev1.Container{Name: "kfp-launcher", Image: config.LauncherImage, Command: config.LauncherCommand, Args: []string{"--copy", "/kfp-launcher/launch"}, VolumeMounts: []corev1.VolumeMount{{Name: "kfp-launcher", MountPath: "/kfp-launcher"}}}}}, Volumes: []corev1.Volume{{Name: "kfp-launcher", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}}, PodSpecPatch: "{{inputs.parameters.pod-spec-patch}}"},
	}}})
	return w, config
}

func TestTaskRuntimeCredentials(t *testing.T) {
	w, config := taskAuthFixture()
	original := w.Workflow.DeepCopy()
	require.NoError(t, w.EnsureTaskRuntimeCredentials(config))
	require.NoError(t, w.ValidateTaskRuntimeCredentials(config.Audience))
	require.Equal(t, original.Spec.ServiceAccountName, w.Spec.ServiceAccountName)
	for i, tmpl := range w.Spec.Templates {
		require.Equal(t, original.Spec.Templates[i].Container.Image, tmpl.Container.Image)
		require.Equal(t, original.Spec.Templates[i].Container.Command, tmpl.Container.Command)
		require.Contains(t, tmpl.Container.Env, corev1.EnvVar{Name: TaskTokenPathEnv, Value: TaskTokenPath})
		require.Contains(t, tmpl.Container.VolumeMounts, corev1.VolumeMount{Name: "kfp-task-api-token", MountPath: "/var/run/secrets/kubeflow/pipelines", ReadOnly: true})
		v := tmpl.Volumes[len(tmpl.Volumes)-1]
		token := v.Projected.Sources[0].ServiceAccountToken
		require.Equal(t, "pipelines", token.Audience)
		require.Equal(t, int64(3600), *token.ExpirationSeconds)
		require.Equal(t, "token", token.Path)
	}
	prepared := w.Workflow.DeepCopy()
	require.NoError(t, w.EnsureTaskRuntimeCredentials(config))
	require.Equal(t, prepared, w.Workflow)
	w.Spec.Templates[0].Container.Image = "custom:already-authenticated"
	require.NoError(t, w.EnsureTaskRuntimeCredentials(config))
	require.Equal(t, "custom:already-authenticated", w.Spec.Templates[0].Container.Image)
}

func TestTaskRuntimeCredentialsRejectBeforeMutation(t *testing.T) {
	for name, change := range map[string]func(*Workflow){
		"old runtime": func(w *Workflow) { w.Spec.Templates[1].InitContainers[0].Image = "launcher:old" },
		"token env": func(w *Workflow) {
			w.Spec.Templates[1].Container.Env = []corev1.EnvVar{{Name: TaskTokenPathEnv, Value: "/user/token"}}
		},
		"shadow mount": func(w *Workflow) {
			w.Spec.Templates[1].Container.VolumeMounts = append(w.Spec.Templates[1].Container.VolumeMounts, corev1.VolumeMount{Name: "user", MountPath: "/var/run/secrets"})
		},
		"stored template": func(w *Workflow) { w.Status.StoredTemplates = map[string]wfapi.Template{"old": w.Spec.Templates[1]} },
		"saved output": func(w *Workflow) {
			w.Status.Nodes = wfapi.Nodes{"driver": wfapi.NodeStatus{Outputs: &wfapi.Outputs{Parameters: []wfapi.Parameter{{Name: "pod-spec-patch", Value: wfapi.AnyStringPtr(`{"containers":[{"name":"main","env":[{"name":"KF_PIPELINES_SA_TOKEN_PATH","value":"/other"}]}]}`)}}}}}
		},
		"literal deletion": func(w *Workflow) {
			w.Spec.PodSpecPatch = `{"containers":[{"name":"main","volumeMounts":[{"$patch":"replace"}]}]}`
		},
	} {
		t.Run(name, func(t *testing.T) {
			w, config := taskAuthFixture()
			change(w)
			before := w.Workflow.DeepCopy()
			require.Error(t, w.EnsureTaskRuntimeCredentials(config))
			require.Equal(t, before, w.Workflow)
		})
	}
	w, config := taskAuthFixture()
	config.Audience = ""
	before := w.Workflow.DeepCopy()
	require.NoError(t, w.EnsureTaskRuntimeCredentials(config))
	require.Equal(t, before, w.Workflow)
}

func TestTaskRuntimePodPatch(t *testing.T) {
	require.NoError(t, ValidateTaskRuntimePodPatch(&corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "user", Command: []string{"/kfp-launcher/launch", "--execution_id", "42"}}}}))
	require.Error(t, ValidateTaskRuntimePodPatch(&corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Env: []corev1.EnvVar{{Name: TaskTokenPathEnv, Value: "bad"}}}}}))
	require.Error(t, ValidateTaskRuntimePodPatch(&corev1.PodSpec{Volumes: []corev1.Volume{{Name: "kfp-task-api-token"}}}))
	require.Error(t, ValidateTaskRuntimePodPatch(&corev1.PodSpec{Containers: []corev1.Container{{Name: "main", VolumeMounts: []corev1.VolumeMount{{Name: "x", MountPath: "/var/run"}}}}}))
}
