#!/usr/bin/env python3
"""Execute actual workflow privacy gates using read-only fake API responses."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = Path(os.environ.get('TEST_WORKFLOW', ROOT / '.github/workflows/build.yml')).read_text()
MOCK = r'''import json, os, sys
args = sys.argv[1:]
with open(os.environ['CALL_LOG'], 'a') as log:
    log.write(json.dumps([os.path.basename(sys.argv[0])] + args) + '\n')
if os.path.basename(sys.argv[0]) == 'docker':
    sys.exit(0)
if args[1].startswith('repos/'):
    private = os.environ['REPO_PRIVATE']
    print(private + '\t' + os.environ['OWNER_TYPE'] if '--include' not in args and '@tsv' in args[-1] else private)
else:
    status = os.environ['PACKAGE_STATUS']
    if '--include' in args:
        print('HTTP/2 ' + status + '\n')
    print(os.environ['PACKAGE_VISIBILITY'])
    sys.exit(0 if status == '200' else 1)
'''


def privacy_gate(phase):
    if phase == 'pre':
        step = WORKFLOW.split('id: privacy\n', 1)[1].split('\n      - uses:', 1)[0]
        return textwrap.dedent(step.split('        run: |\n', 1)[1])
    # The post-publication gate re-reads current API state after digest validation.
    step = WORKFLOW.split('      - name: Verify published digest,', 1)[1]
    return textwrap.dedent('          private=$(gh api ' +
                           step.split('          private=$(gh api ', 1)[1].split('          echo "$IMAGE@$DIGEST"', 1)[0])


class PackageVisibility(unittest.TestCase):
    def run_gate(self, phase, private='false', visibility='public', status='200', owner='User', suffix=''):
        with tempfile.TemporaryDirectory() as tmp:
            for name in ('gh', 'docker'):
                mock = Path(tmp, name)
                mock.write_text('#!' + sys.executable + '\n' + MOCK)
                mock.chmod(0o755)
            log = Path(tmp, 'calls')
            output = Path(tmp, 'outputs')
            env = dict(os.environ, PATH=tmp + os.pathsep + os.environ['PATH'], CALL_LOG=str(log),
                       REPOSITORY='Example/Recovery', OWNER='Example', SUFFIX=suffix,
                       PACKAGE_ENDPOINT=('users' if owner == 'User' else 'orgs') + '/Example/packages/container/recovery' + suffix,
                       REPO_PRIVATE=private, OWNER_TYPE=owner, PACKAGE_STATUS=status,
                       PACKAGE_VISIBILITY=visibility, GITHUB_OUTPUT=str(output))
            downstream = '\ndocker push must-not-run-until-gate-passes\n' if phase == 'pre' else ''
            result = subprocess.run(['bash', '-euo', 'pipefail', '-c', privacy_gate(phase) + downstream],
                                    env=env, capture_output=True, text=True)
            calls = [json.loads(line) for line in log.read_text().splitlines()]
            return result, calls, output.read_text() if output.exists() else ''

    def test_visibility_policy_for_both_packages_and_owner_types(self):
        for phase in ('pre', 'post'):
            for private, expected in (('false', 'public'), ('true', 'private')):
                for visibility in ('public', 'private', 'internal', 'unknown'):
                    for owner in ('User', 'Organization'):
                        for suffix in ('', '-fault'):
                            with self.subTest(phase=phase, private=private, visibility=visibility, owner=owner, suffix=suffix):
                                result, calls, output = self.run_gate(phase, private, visibility, owner=owner, suffix=suffix)
                                allowed = visibility == expected
                                self.assertEqual(result.returncode == 0, allowed, result.stderr)
                                if phase == 'pre':
                                    self.assertEqual(any(c[0] == 'docker' for c in calls), allowed)
                                    namespace = 'users' if owner == 'User' else 'orgs'
                                    self.assertIn(f'endpoint={namespace}/Example/packages/container/recovery{suffix}', output)
                                self.assertTrue(all('--method' not in c and '-X' not in c for c in calls))

    def test_missing_public_package_stops_before_push(self):
        for suffix in ('', '-fault'):
            result, calls, _ = self.run_gate('pre', status='404', suffix=suffix)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('separate owner approval', result.stderr)
            self.assertFalse(any(c[0] == 'docker' for c in calls))

    def test_missing_private_package_preserves_first_publication(self):
        result, calls, _ = self.run_gate('pre', private='true', status='404')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(any(c[0] == 'docker' for c in calls))

    def test_api_errors_and_unknown_repository_visibility_fail(self):
        for phase in ('pre', 'post'):
            for private, status in (('false', '401'), ('true', '403'), ('false', '500'), ('false', '404'), ('unknown', '200')):
                result, calls, _ = self.run_gate(phase, private=private, status=status)
                self.assertNotEqual(result.returncode, 0)
                self.assertFalse(any(c[0] == 'docker' for c in calls))
            self.assertEqual(next(c for c in calls if c[0] == 'gh')[1:3], ['api', 'repos/Example/Recovery'])

    def test_workflow_gate_precedes_every_trusted_publish_path(self):
        publish = WORKFLOW.split('\n  publish:', 1)[1]
        self.assertLess(publish.index('id: privacy'), publish.index('docker/login-action@'))
        self.assertLess(publish.index('id: privacy'), publish.index('id: publish'))
        self.assertIn("github.event_name == 'push'", publish)
        self.assertIn("github.event_name == 'workflow_dispatch' && github.ref == 'refs/heads/main'", publish)
        self.assertIn('branches: [main]', WORKFLOW)
        self.assertIn("tags: ['v*']", WORKFLOW)


if __name__ == '__main__':
    unittest.main()
