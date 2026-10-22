# Task API authentication and upgrades

In multi-user deployments, task creation requires an authenticated caller with
`createTask` on `runs` in `pipelines.kubeflow.org` in the stored run namespace.
The shipped pipeline-runner and edit roles include that permission. Bind an
equivalent namespaced role for custom workload service accounts; retain their
existing service-account `use` permissions. Single-user operation is unchanged.

Use driver and launcher images built with the credential-aware cache client.
Set `V2_DRIVER_IMAGE` and `V2_LAUNCHER_IMAGE` to those tested image versions or
digests on every API replica. An image string alone cannot establish that an
older or custom binary sends credentials. The compiler projects a token for the
actual workload service account with the API's `TOKEN_REVIEW_AUDIENCE` and sets
`KF_PIPELINES_SA_TOKEN_PATH`. The directory mount allows kubelet rotation; the
client rereads the file through the existing cached token source. Keep these
reserved env/volume/mount fields intact in custom runtime or pod patches.

Existing V2 schedules and uploaded V2 IR need no recreation or SDK recompilation.
Fresh ticks compile through the current API server. Default/custom service
accounts, version selection, replay and scheduling policies are preserved.

Roll out the credential-aware runtime images, compiler output and RBAC before
server enforcement. After every API replica is prepared, let whole workflows
created before that boundary finish, including pending steps and automatic Argo
retries. Recurring schedules can continue creating prepared workflows. The
preparation stage alone does not enforce task authorization. Running old pods
cannot gain the new client or missing token mounts from an API upgrade.

Explicit retry preserves saved images, completed nodes and outputs. Prepared
workflows receive fresh tokens in recreated pods. Missing credentials can be
added only to recognized current credential-aware runtimes. An incompatible
historical snapshot is rejected before a retry claim or pod deletion, with a log
warning and an actionable error. The failed run remains intact. Create a new run
from V2 IR if needed; this uses current templates but may repeat completed work.
Inline custom runtime templates and retained external templates are outside this
bounded preparation path and require an explicit update. Saved patches must keep
the credential declarations and launcher shared volume/mounts intact.
Old snapshots do not self-upgrade, and no cutoff date is imposed. Precompiled
Argo/custom legacy runtime templates need their owner's explicit update.

Verify actual images, projected-token rotation, cache misses/hits, PVC tasks,
existing/new schedules and custom service accounts in an isolated environment
before enforcement. Offline tests do not establish cluster rollout compatibility.
