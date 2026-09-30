#!/usr/bin/env python3
"""Exercise publication failures with a fake Docker daemon; no registry writes."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
IMAGE_ID = 'sha256:' + '1' * 64
DIGEST = 'sha256:' + '2' * 64
NAME = 'ghcr.io/example/recovery'
MOCK = r'''import json, os, sys
from pathlib import Path
args = sys.argv[1:]
with open(os.environ['DOCKER_LOG'], 'a') as log:
    log.write(json.dumps(args) + '\n')
if args[:2] == ['image', 'inspect']:
    fmt = args[-1]
    if fmt == '{{.Id}}':
        print('sha256:' + ('3' if os.environ.get('CHANGED_ID') and args[2].startswith('ghcr.') else '1') * 64)
    elif 'RepoDigests' in fmt:
        print(os.environ['IMAGE_NAME'] + '@sha256:' + '2' * 64)
    else:
        print(os.environ.get('REVISION', 'expected'))
elif args[0] == 'run':
    sys.exit(int(os.environ.get('HELP_EXIT', '0')))
'''


class ImagePublication(unittest.TestCase):
    def run_image(self, **overrides):
        with tempfile.TemporaryDirectory() as tmp:
            mock = Path(tmp, 'docker')
            mock.write_text('#!' + sys.executable + '\n' + MOCK)
            mock.chmod(0o755)
            log = Path(tmp, 'docker.log')
            output = Path(tmp, 'outputs')
            env = dict(os.environ, PATH=tmp + os.pathsep + os.environ['PATH'],
                       DOCKER_LOG=str(log), IMAGE=IMAGE_ID,
                       EXPECTED_REVISION='expected', IMAGE_NAME=NAME,
                       PUBLISH_TAGS=NAME + ':main\n' + NAME + ':sha-test',
                       GITHUB_OUTPUT=str(output))
            env.update(overrides)
            result = subprocess.run(['bash', str(ROOT / '.github/scripts/validate-image.sh')],
                                    env=env, capture_output=True, text=True)
            calls = [json.loads(line) for line in log.read_text().splitlines()]
            outputs = output.read_text() if output.exists() else ''
            return result, calls, outputs

    def assert_no_push(self, **env):
        result, calls, _ = self.run_image(**env)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any(call[0] == 'push' for call in calls), calls)

    def test_bad_revision_never_pushes(self):
        for suffix in ('', '-fault'):
            with self.subTest(image=suffix or 'agent'):
                name = NAME + suffix
                self.assert_no_push(REVISION='wrong', IMAGE_NAME=name, PUBLISH_TAGS=name + ':main')

    def test_failing_help_never_pushes(self):
        for suffix in ('', '-fault'):
            with self.subTest(image=suffix or 'agent'):
                name = NAME + suffix
                self.assert_no_push(HELP_EXIT='23', IMAGE_NAME=name, PUBLISH_TAGS=name + ':main')

    def test_all_tags_are_checked_before_first_push(self):
        self.assert_no_push(PUBLISH_TAGS=NAME + ':main\nother/repo:tag')

    def test_changed_tag_identity_never_pushes(self):
        self.assert_no_push(CHANGED_ID='1')

    def test_pr_validation_has_no_push(self):
        result, calls, outputs = self.run_image(PUBLISH_TAGS='')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(any(call[0] in ('tag', 'push') for call in calls))
        self.assertEqual(outputs, '')

    def test_success_pushes_only_the_validated_id(self):
        result, calls, outputs = self.run_image()
        self.assertEqual(result.returncode, 0, result.stderr)
        run = next(call for call in calls if call[0] == 'run')
        self.assertEqual(run[-2:], [IMAGE_ID, '--help'])
        self.assertIn('--read-only', run)
        self.assertIn('no-new-privileges', run)
        self.assertEqual([call for call in calls if call[0] == 'tag'],
                         [['tag', IMAGE_ID, NAME + ':main'], ['tag', IMAGE_ID, NAME + ':sha-test']])
        self.assertEqual([call for call in calls if call[0] == 'push'],
                         [['push', NAME + ':main'], ['push', NAME + ':sha-test']])
        self.assertLess(calls.index(run), next(i for i, call in enumerate(calls) if call[0] == 'push'))
        self.assertIn('digest=' + DIGEST, outputs)

    def test_fault_happy_uses_same_immutable_id(self):
        name = NAME + '-fault'
        result, calls, outputs = self.run_image(
            IMAGE_NAME=name, PUBLISH_TAGS=name + ':main\n' + name + ':sha-test')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual([call for call in calls if call[0] == 'tag'],
                         [['tag', IMAGE_ID, name + ':main'], ['tag', IMAGE_ID, name + ':sha-test']])
        self.assertEqual(len([call for call in calls if call[0] == 'push']), 2)
        self.assertIn('digest=' + DIGEST, outputs)

    def test_trusted_workflow_builds_locally_before_validated_push(self):
        path = Path(os.environ.get('TEST_WORKFLOW', ROOT / '.github/workflows/build.yml'))
        workflow = path.read_text()
        publish = workflow.split('\n  publish:', 1)[1]
        build = publish.split('id: build', 1)[1].split('\n      - name:', 1)[0]
        self.assertIn('push: false', build)
        self.assertIn('load: true', build)
        self.assertIn('IMAGE: ${{ steps.build.outputs.imageid }}', publish)
        self.assertIn('bash .github/scripts/validate-image.sh', publish)
        self.assertIn('DIGEST: ${{ steps.publish.outputs.digest }}', publish)
        self.assertIn("github.event_name == 'push'", publish)
        self.assertIn("github.event_name == 'workflow_dispatch' && github.ref == 'refs/heads/main'", publish)
        self.assertIn("branches: [main]", workflow)
        self.assertIn("tags: ['v*']", workflow)
        self.assertIn('target: agent', publish)
        self.assertIn('target: fault', publish)
        images = workflow.split('\n  images:', 1)[1].split('\n  publish:', 1)[0]
        self.assertIn("if: github.event_name == 'pull_request'", images)
        self.assertNotIn('PUBLISH_TAGS:', images)


if __name__ == '__main__':
    unittest.main()
