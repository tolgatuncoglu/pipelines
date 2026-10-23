#!/usr/bin/env python3
# Copyright 2026 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pathlib import Path
import re
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[3]
WORKFLOW_JOBS = {
    '.github/workflows/api-server-tests.yml': (
        'api-test-standalone',
        'api-test-k8s-native',
        'api-test-multi-user',
    ),
    '.github/workflows/e2e-test-frontend.yml': ('frontend-integration-test',),
    '.github/workflows/e2e-test.yml': (
        'end-to-end-scenario-tests',
        'end-to-end-critical-scenario-multi-user-tests',
        'end-to-end-critical-mlflow-tests',
    ),
    '.github/workflows/integration-tests-v1.yml':
        ('initialization-integration-tests-v1',),
    '.github/workflows/kfp-kubernetes-native-migration-tests.yaml':
        ('kfp-kubernetes-native-migration-tests',),
    '.github/workflows/kfp-sdk-client-tests.yml': ('sdk-client-tests',),
    '.github/workflows/kfp-webhooks.yml': ('webhook-tests',),
    '.github/workflows/legacy-v2-api-integration-tests.yml':
        ('api-integration-tests-v2',),
    '.github/workflows/upgrade-test.yml': (
        'upgrade-test',
        'readiness-schedules',
        'readiness-adoption',
        'custom-role-acceptance',
        'readiness-reporting',
    ),
}


def _job_block(workflow: str, job_name: str) -> str:
    marker = f'  {job_name}:\n'
    start = workflow.index(marker)
    next_job = re.search(r'^  [a-zA-Z0-9_-]+:\n',
                         workflow[start + len(marker):], re.MULTILINE)
    end = (
        start + len(marker) + next_job.start() if next_job else len(workflow))
    return workflow[start:end]


def _dependencies(job: dict) -> list[str]:
    needs = job.get('needs', [])
    return [needs] if isinstance(needs, str) else needs


class LoadedWorkflowOrderingTest(unittest.TestCase):

    def test_deploy_jobs_wait_for_image_builds_without_consuming_runners(self):
        deploy_callers = {}
        for workflow_path in (ROOT / '.github/workflows').glob('*.y*ml'):
            document = yaml.safe_load(workflow_path.read_text(encoding='utf-8'))
            callers = {
                name for name, job in document.get('jobs', {}).items() if any(
                    step.get('uses') == './.github/actions/deploy'
                    for step in job.get('steps', []))
            }
            if callers:
                deploy_callers[str(workflow_path.relative_to(ROOT))] = callers

        self.assertEqual(deploy_callers, {
            path: set(names) for path, names in WORKFLOW_JOBS.items()
        })

        for relative_path, job_names in WORKFLOW_JOBS.items():
            workflow = (ROOT / relative_path).read_text(encoding='utf-8')
            jobs = yaml.safe_load(workflow)['jobs']
            self.assertEqual(jobs['build']['uses'],
                             './.github/workflows/image-builds.yml')
            self.assertFalse(_dependencies(jobs['build']), relative_path)
            self.assertNotIn('needs.build.outputs', workflow, relative_path)
            for job_name in job_names:
                with self.subTest(workflow=relative_path, job=job_name):
                    job = _job_block(workflow, job_name)
                    self.assertIn('build', _dependencies(jobs[job_name]))
                    self.assertIn(
                        'permissions:\n      actions: read\n'
                        '      contents: read', job)
                    self.assertIn('image_path: images_${{ github.run_id }}',
                                  job)
                    self.assertIn('image_tag: latest', job)
                    self.assertIn('image_registry: kind-registry:5000', job)

    def test_workflow_dependencies_exist_and_are_acyclic(self):
        for relative_path in WORKFLOW_JOBS:
            jobs = yaml.safe_load(
                (ROOT / relative_path).read_text(encoding='utf-8'))['jobs']
            visited = set()
            visiting = set()

            def visit(name):
                self.assertIn(name, jobs, relative_path)
                self.assertNotIn(
                    name, visiting,
                    f'{relative_path}: dependency cycle at {name}')
                if name in visited:
                    return
                visiting.add(name)
                for dependency in _dependencies(jobs[name]):
                    visit(dependency)
                visiting.remove(name)
                visited.add(name)

            for name in jobs:
                visit(name)

    def test_ci_guard_runs_when_any_deploy_workflow_changes(self):
        workflow = (ROOT / '.github/workflows/ci-scripts-tests.yml').read_text(
            encoding='utf-8')
        for relative_path in WORKFLOW_JOBS:
            with self.subTest(workflow=relative_path):
                self.assertIn(f"      - '{relative_path}'", workflow)

    def test_deploy_waits_before_downloading_images(self):
        deploy_action = (ROOT / '.github/actions/deploy/action.yml').read_text(
            encoding='utf-8')

        wait_position = deploy_action.index(
            'run: ./.github/resources/scripts/wait-for-image-artifacts.sh')
        download_position = deploy_action.index(
            '- name: Download Docker Images')
        self.assertLess(wait_position, download_position)
        self.assertIn('GH_TOKEN: ${{ github.token }}', deploy_action)

    def test_multi_user_artifact_proxy_lane_uses_critical_shards(self):
        workflow = (ROOT / '.github/workflows/e2e-test.yml').read_text(
            encoding='utf-8')
        job = _job_block(workflow,
                         'end-to-end-critical-scenario-multi-user-tests')

        self.assertIn("'[\"E2ECriticalShardA\", \"E2ECriticalShardB\"]'", job)
        self.assertNotIn('        exclude:', job)
        self.assertIn('artifact_proxy: ${{ matrix.cache_enabled }}', job)
        self.assertIn("matrix.cache_enabled == 'true'", job)
        self.assertIn('Multi User ${{ matrix.test_label }} Tests', job)
        self.assertIn('E2EMultiUser${{ matrix.test_label }}Tests', job)


if __name__ == '__main__':
    unittest.main()
