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
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/kubeflow/pipelines/backend/src/common/util"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestPipelineSizeLimits(t *testing.T) {
	names := []string{MaxPipelineUploadBytesEnv, MaxPipelineSpecBytesEnv, MaxPipelineUpdateBodyBytesEnv}
	for _, name := range names {
		t.Setenv(name, "")
	}
	limits, err := GetPipelineSizeLimits()
	require.NoError(t, err)
	require.Equal(t, PipelineSizeLimits{32 << 20, 32 << 20, 32 << 20}, limits)
	for i, name := range names {
		t.Run(name, func(t *testing.T) {
			for _, value := range []string{"1", "67108864", strconv.Itoa(MaximumPipelineSizeBytes)} {
				t.Run(value, func(t *testing.T) {
					t.Setenv(name, value)
					got, err := GetPipelineSizeLimits()
					require.NoError(t, err)
					want := []int{32 << 20, 32 << 20, 32 << 20}
					want[i], _ = strconv.Atoi(value)
					require.Equal(t, PipelineSizeLimits{want[0], want[1], want[2]}, got)
				})
			}
			for _, value := range []string{"0", "-1", "32MiB", "1.5", "secret-invalid-value", "9223372036854775808", strconv.Itoa(MaximumPipelineSizeBytes + 1)} {
				t.Run(value, func(t *testing.T) {
					t.Setenv(name, value)
					_, err := GetPipelineSizeLimits()
					require.ErrorContains(t, err, name)
					require.ErrorContains(t, err, "unset it")
					require.NotContains(t, err.Error(), "secret-invalid-value")
				})
			}
		})
	}
}

func TestSizeLimitErrorIsSafeAndInvalidArgument(t *testing.T) {
	err := NewSizeLimitError("pipeline_upload", 32<<20, MaxPipelineUploadBytesEnv)
	var limitErr *SizeLimitError
	require.True(t, errors.As(err, &limitErr))
	require.Contains(t, limitErr.Error(), "33554432 bytes (32.00 MiB)")
	require.Contains(t, limitErr.Error(), MaxPipelineUploadBytesEnv)
	require.Contains(t, limitErr.Error(), "container image or object store")
	var userErr *util.UserError
	require.True(t, errors.As(err, &userErr))
	require.Equal(t, codes.InvalidArgument, userErr.ExternalStatusCode())
}

func setPipelineVersionObjectSizeConfig(t *testing.T, value interface{}) {
	t.Helper()
	viper.Set(MaxPipelineVersionObjectBytesConfig, value)
	t.Cleanup(func() { viper.Set(MaxPipelineVersionObjectBytesConfig, nil) })
}

func TestPipelineVersionObjectSizeLimit_Defaults(t *testing.T) {
	for _, value := range []interface{}{nil, "", "  "} {
		setPipelineVersionObjectSizeConfig(t, value)
		limit, err := GetPipelineVersionObjectSizeLimit()
		require.NoError(t, err)
		require.Equal(t, DefaultPipelineVersionObjectBytes, limit)
	}
}

func TestPipelineVersionObjectSizeLimit_ReservesEtcdOverhead(t *testing.T) {
	// Both limits stay below the Kubernetes limits they derive from, so an object the webhook admits
	// still fits once etcd adds its storage key and transaction framing.
	require.Equal(t, 1572864, DefaultPipelineVersionObjectBytes+PipelineVersionObjectOverheadBytes)
	require.Equal(t, 2097152, MaximumPipelineVersionObjectBytes+PipelineVersionObjectOverheadBytes)
}

func TestPipelineVersionObjectTooLargeMessage(t *testing.T) {
	message := PipelineVersionObjectTooLargeMessage(1887437, DefaultPipelineVersionObjectBytes)
	for _, expected := range []string{
		"The pipeline version is too large to store in Kubernetes",
		"the PipelineVersion object is 1887437 bytes (1.80 MiB)",
		"the limit is 1556480 bytes (1.48 MiB)",
		"single etcd object",
		MaxPipelineSpecBytesEnv,
		"make the compiled pipeline smaller",
		MaxPipelineVersionObjectBytesConfig,
		"--max-request-bytes",
	} {
		require.Contains(t, message, expected)
	}
}

func TestPipelineVersionUpdateTooLargeMessage(t *testing.T) {
	message := PipelineVersionUpdateTooLargeMessage(1600000, 1500000, DefaultPipelineVersionObjectBytes)
	for _, expected := range []string{
		"This update would make the pipeline version too large to store in Kubernetes",
		"would grow from 1500000 bytes to 1600000 bytes (1.53 MiB)",
		"the limit is 1556480 bytes (1.48 MiB)",
		"shorter display name",
	} {
		require.Contains(t, message, expected)
	}
	require.NotContains(t, message, "compiled pipeline", "an update cannot change the pipeline spec")
}

func TestPipelineVersionObjectSizeLimit_AcceptsValuesUpToKubernetesLimit(t *testing.T) {
	for _, value := range []interface{}{"1", "2080768", 2080768, strconv.Itoa(MaximumPipelineVersionObjectBytes)} {
		setPipelineVersionObjectSizeConfig(t, value)
		limit, err := GetPipelineVersionObjectSizeLimit()
		require.NoError(t, err)
		require.Equal(t, viper.GetInt(MaxPipelineVersionObjectBytesConfig), limit)
	}
}

func TestPipelineVersionObjectSizeLimit_RejectsValuesAboveKubernetesLimit(t *testing.T) {
	for _, value := range []string{strconv.Itoa(MaximumPipelineVersionObjectBytes + 1), "2097152", "3145728", strconv.Itoa(MaximumPipelineSizeBytes)} {
		t.Run(value, func(t *testing.T) {
			setPipelineVersionObjectSizeConfig(t, value)
			_, err := GetPipelineVersionObjectSizeLimit()
			require.ErrorContains(t, err, MaxPipelineVersionObjectBytesConfig)
			require.ErrorContains(t, err, "must not exceed 2080768 bytes")
			require.ErrorContains(t, err, "over 2 MiB to etcd")
			require.ErrorContains(t, err, "reserved for etcd request overhead")
		})
	}
}

func TestPipelineVersionObjectSizeLimit_RejectsInvalidValues(t *testing.T) {
	for _, value := range []string{"0", "-1", "1.5MiB", "1.5", "9223372036854775808"} {
		t.Run(value, func(t *testing.T) {
			setPipelineVersionObjectSizeConfig(t, value)
			_, err := GetPipelineVersionObjectSizeLimit()
			require.ErrorContains(t, err, MaxPipelineVersionObjectBytesConfig)
			require.ErrorContains(t, err, "positive integer")
			require.ErrorContains(t, err, strconv.Itoa(DefaultPipelineVersionObjectBytes))
		})
	}
}

func TestPipelineVersionObjectSizeLimit_ReadsEnvironment(t *testing.T) {
	viper.AutomaticEnv()
	t.Setenv(MaxPipelineVersionObjectBytesConfig, "2080768")
	limit, err := GetPipelineVersionObjectSizeLimit()
	require.NoError(t, err)
	require.Equal(t, 2080768, limit)
}

func TestPipelineVersionObjectSizeLimit_ConfigFileValues(t *testing.T) {
	testCases := []struct {
		config        string
		expectedLimit int
		expectedError string
	}{
		{`{"MAX_PIPELINE_VERSION_OBJECT_BYTES": 1024}`, 1024, ""},
		{`{"MAX_PIPELINE_VERSION_OBJECT_BYTES": "1024"}`, 1024, ""},
		{`{"MAX_PIPELINE_VERSION_OBJECT_BYTES": 2080768}`, 2080768, ""},
		{`{"MAX_PIPELINE_VERSION_OBJECT_BYTES": [1024]}`, 0, "unsupported type []interface {}"},
		{`{"MAX_PIPELINE_VERSION_OBJECT_BYTES": {"bytes": 1024}}`, 0, "unsupported type map[string]interface {}"},
		{`{"MAX_PIPELINE_VERSION_OBJECT_BYTES": true}`, 0, "unsupported type bool"},
		{`{"MAX_PIPELINE_VERSION_OBJECT_BYTES": 1024.5}`, 0, "positive integer"},
		{`{"MAX_PIPELINE_VERSION_OBJECT_BYTES": 2080769}`, 0, "must not exceed 2080768 bytes"},
	}
	for _, testCase := range testCases {
		t.Run(testCase.config, func(t *testing.T) {
			configFile := viper.New()
			configFile.SetConfigType("json")
			require.NoError(t, configFile.ReadConfig(strings.NewReader(testCase.config)))
			setPipelineVersionObjectSizeConfig(t, configFile.Get(MaxPipelineVersionObjectBytesConfig))

			limit, err := GetPipelineVersionObjectSizeLimit()
			if testCase.expectedError != "" {
				require.ErrorContains(t, err, testCase.expectedError)
				require.ErrorContains(t, err, MaxPipelineVersionObjectBytesConfig)
				return
			}
			require.NoError(t, err)
			require.Equal(t, testCase.expectedLimit, limit)
		})
	}
}
