// Copyright 2018 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/golang/glog"
	"github.com/kubeflow/pipelines/backend/src/common/util"
	"github.com/spf13/viper"
)

const (
	// MaxPipelineUploadBytesEnv limits the uploaded or downloaded pipeline package.
	MaxPipelineUploadBytesEnv = "MAX_PIPELINE_UPLOAD_BYTES"
	// MaxPipelineSpecBytesEnv limits extracted specifications and object-store spec reads.
	MaxPipelineSpecBytesEnv = "MAX_PIPELINE_SPEC_BYTES"
	// MaxPipelineUpdateBodyBytesEnv limits pipeline PUT/PATCH request bodies.
	MaxPipelineUpdateBodyBytesEnv = "MAX_PIPELINE_UPDATE_BODY_BYTES"
	// MaximumPipelineSizeBytes bounds administrator overrides; requests remain buffered in memory.
	MaximumPipelineSizeBytes = 128 << 20

	// MaxPipelineVersionObjectBytesConfig limits serialized PipelineVersion objects in the Kubernetes
	// pipeline store. It is read from the environment or config.json on every check.
	MaxPipelineVersionObjectBytesConfig = "MAX_PIPELINE_VERSION_OBJECT_BYTES"
	// PipelineVersionObjectOverheadBytes is reserved below each Kubernetes limit for what etcd counts
	// beyond the serialized object: the storage key and the transaction framing. It is a generous bound;
	// the real overhead is a few hundred bytes.
	PipelineVersionObjectOverheadBytes = 16 << 10
	// DefaultPipelineVersionObjectBytes is the etcd --max-request-bytes default (1.5 MiB) minus the reserved overhead.
	DefaultPipelineVersionObjectBytes = 1572864 - PipelineVersionObjectOverheadBytes
	// MaximumPipelineVersionObjectBytes is the send limit of the Kubernetes API server's etcd client (2 MiB) minus
	// the reserved overhead. That send limit is not configurable, so larger objects cannot be stored even if
	// etcd accepts more.
	MaximumPipelineVersionObjectBytes = 2<<20 - PipelineVersionObjectOverheadBytes
)

// PipelineSizeLimits contains independent, finite pipeline byte ceilings.
type PipelineSizeLimits struct {
	UploadBytes     int
	SpecBytes       int
	UpdateBodyBytes int
}

// GetPipelineSizeLimits validates operator environment settings. Unset or empty
// settings retain the 32 MiB defaults; invalid settings never disable a ceiling.
func GetPipelineSizeLimits() (PipelineSizeLimits, error) {
	limits := PipelineSizeLimits{MaxFileLength, MaxFileLength, MaxFileLength}
	for _, setting := range []struct {
		name  string
		value *int
	}{
		{MaxPipelineUploadBytesEnv, &limits.UploadBytes},
		{MaxPipelineSpecBytesEnv, &limits.SpecBytes},
		{MaxPipelineUpdateBodyBytesEnv, &limits.UpdateBodyBytes},
	} {
		raw := os.Getenv(setting.name)
		if raw == "" {
			continue
		}
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || value < 1 || value > MaximumPipelineSizeBytes {
			return PipelineSizeLimits{}, fmt.Errorf("%s must be an integer from 1 to %d bytes; unset it to use the %d-byte default", setting.name, MaximumPipelineSizeBytes, MaxFileLength)
		}
		*setting.value = int(value)
	}
	return limits, nil
}

// GetPipelineVersionObjectSizeLimit returns the largest serialized PipelineVersion object the
// Kubernetes pipeline store accepts. Unset or empty settings use the etcd default.
func GetPipelineVersionObjectSizeLimit() (int, error) {
	invalidValueErr := fmt.Errorf("%s must be a positive integer number of bytes; unset it to use the %d-byte default",
		MaxPipelineVersionObjectBytesConfig, DefaultPipelineVersionObjectBytes)

	// viper.GetString silently turns unsupported types such as JSON arrays into "", so inspect the raw value.
	var value int64
	switch raw := viper.Get(MaxPipelineVersionObjectBytesConfig).(type) {
	case nil:
		return DefaultPipelineVersionObjectBytes, nil
	case string:
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			return DefaultPipelineVersionObjectBytes, nil
		}
		parsed, err := strconv.ParseInt(trimmed, 10, 64)
		if err != nil {
			return 0, invalidValueErr
		}
		value = parsed
	case int:
		value = int64(raw)
	case int64:
		value = raw
	case float64:
		// config.json numbers are decoded as float64.
		if raw != math.Trunc(raw) || raw > math.MaxInt64 || raw < math.MinInt64 {
			return 0, invalidValueErr
		}
		value = int64(raw)
	default:
		return 0, fmt.Errorf("%s has unsupported type %T; %w", MaxPipelineVersionObjectBytesConfig, raw, invalidValueErr)
	}

	if value < 1 {
		return 0, invalidValueErr
	}
	if value > MaximumPipelineVersionObjectBytes {
		return 0, fmt.Errorf("%s must not exceed %d bytes: the Kubernetes API server cannot send requests over 2 MiB to etcd, "+
			"and %d bytes are reserved for etcd request overhead. Larger PipelineVersion objects are rejected by Kubernetes regardless of this setting",
			MaxPipelineVersionObjectBytesConfig, MaximumPipelineVersionObjectBytes, PipelineVersionObjectOverheadBytes)
	}
	return int(value), nil
}

// SizeLimitError contains only safe, operator-controlled rejection details.
type SizeLimitError struct {
	Control string
	Limit   int64
	Setting string
}

func (e *SizeLimitError) Error() string {
	return SizeLimitErrorMessage(e.Control, e.Limit, e.Setting)
}

// SizeLimitErrorMessage describes a finite ceiling without claiming the truncated
// read measured the entire payload. Controls must be static, not user input.
func SizeLimitErrorMessage(control string, limit int64, setting string) string {
	label := control
	advice := "Reduce the payload"
	switch control {
	case "pipeline_upload":
		label = "File"
		advice = "Move large embedded artifacts, notebooks, or Python code into a container image or object store, or reduce the package"
	case "pipeline_spec":
		label = "Pipeline spec file"
	case "pipeline_decompressed_spec":
		label = "Decompressed file"
	case "pipeline_archive_traversal":
		label = "Archive extraction traversal budget"
		advice = "Reduce archive entries and metadata; the traversal budget is derived from the pipeline spec limit"
	case "pipeline_update_body":
		label = "Request body"
	}
	return fmt.Sprintf("%s size too large: exceeds maximum %d bytes (%.2f MiB). %s or ask an administrator to adjust %s within its supported range.", label, limit, float64(limit)/(1<<20), advice, setting)
}

// NewSizeLimitError logs a bounded rejection and preserves InvalidArgument for
// API callers while allowing upload handlers to expose only the safe message.
func NewSizeLimitError(control string, limit int64, setting string) error {
	err := &SizeLimitError{Control: control, Limit: limit, Setting: setting}
	glog.Warningf("size_limit_exceeded control=%s limit_bytes=%d setting=%s: %s", control, limit, setting, err.Error())
	return util.NewInvalidInputErrorWithDetails(err, err.Error())
}

const (
	pipelineVersionTooLargeRemedy = "To fix this, make the compiled pipeline smaller, for example by moving embedded files, " +
		"notebooks, or large inline Python code into a container image or object storage."
	pipelineVersionUpdateTooLargeRemedy = "To fix this, use a shorter display name, or fewer or shorter tags, labels, or annotations."
)

// PipelineVersionObjectTooLargeMessage explains a new PipelineVersion rejected by the KFP size limit. It is
// shown to API and kubectl users, so it says what happened, why, and what each audience can do.
func PipelineVersionObjectTooLargeMessage(objectBytes int, limit int) string {
	return fmt.Sprintf("The pipeline version is too large to store in Kubernetes: the PipelineVersion object is %d bytes (%.2f MiB) "+
		"and the limit is %d bytes (%.2f MiB). "+
		"Kubernetes stores each pipeline version as a single etcd object, so this limit applies even when the pipeline spec is within %s. "+
		"%s Administrators can raise %s only after raising the etcd --max-request-bytes setting of the cluster to match.",
		objectBytes, float64(objectBytes)/(1<<20), limit, float64(limit)/(1<<20),
		MaxPipelineSpecBytesEnv, pipelineVersionTooLargeRemedy, MaxPipelineVersionObjectBytesConfig)
}

// PipelineVersionUpdateTooLargeMessage explains an update rejected because it grows a PipelineVersion past the
// KFP size limit. Only the fields an update can change are offered as the remedy.
func PipelineVersionUpdateTooLargeMessage(objectBytes int, previousBytes int, limit int) string {
	return fmt.Sprintf("This update would make the pipeline version too large to store in Kubernetes: the PipelineVersion object "+
		"would grow from %d bytes to %d bytes (%.2f MiB), and the limit is %d bytes (%.2f MiB). "+
		"Kubernetes stores each pipeline version as a single etcd object. %s",
		previousBytes, objectBytes, float64(objectBytes)/(1<<20), limit, float64(limit)/(1<<20),
		pipelineVersionUpdateTooLargeRemedy)
}

// PipelineVersionRejectedByKubernetesMessage explains a new PipelineVersion that passed the KFP size check but
// was rejected by a Kubernetes size limit, whose error reports neither the object size nor the limit.
func PipelineVersionRejectedByKubernetesMessage() string {
	return fmt.Sprintf("The pipeline version is too large to store in Kubernetes. "+
		"Kubernetes stores each pipeline version as a single etcd object, which is limited to 1.5 MiB by default "+
		"regardless of %s. %s Alternatively, use the database pipeline store.",
		MaxPipelineSpecBytesEnv, pipelineVersionTooLargeRemedy)
}

// PipelineVersionUpdateRejectedByKubernetesMessage explains an update that a Kubernetes size limit rejected.
func PipelineVersionUpdateRejectedByKubernetesMessage() string {
	return "This update would make the pipeline version too large to store in Kubernetes. " +
		"Kubernetes stores each pipeline version as a single etcd object, which is limited to 1.5 MiB by default. " +
		pipelineVersionUpdateTooLargeRemedy
}
