package util

const (
	// TaskTokenPathEnv selects the projected workload credential used for task writes.
	TaskTokenPathEnv = "KF_PIPELINES_SA_TOKEN_PATH"
	// TaskTokenPath follows the existing KFP API client token path convention.
	TaskTokenPath = "/var/run/secrets/kubeflow/pipelines/token"
)
