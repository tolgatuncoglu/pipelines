package driver

import (
	"testing"

	"github.com/kubeflow/pipelines/backend/src/common/util"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
)

func TestContainerTaskCredentialPatchCollision(t *testing.T) {
	t.Setenv(util.TaskTokenPathEnv, util.TaskTokenPath)
	pod := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "user-image", Command: []string{"user-command"}, Env: []corev1.EnvVar{{Name: util.TaskTokenPathEnv, Value: "/user/token"}}}}}
	_, err := marshalTaskPodSpecPatch(pod)
	require.Error(t, err)
	pod.Containers[0].Env = nil
	raw, err := marshalTaskPodSpecPatch(pod)
	require.NoError(t, err)
	require.Contains(t, string(raw), "user-image")
	require.Contains(t, string(raw), "user-command")
	pod.Containers[0].Env = []corev1.EnvVar{{Name: util.TaskTokenPathEnv, Value: "/user/token"}}
	t.Setenv(util.TaskTokenPathEnv, "")
	_, err = marshalTaskPodSpecPatch(pod)
	require.NoError(t, err)
}
