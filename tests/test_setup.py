import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('setup_tool', ROOT/'scripts/setup.py')
setup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(setup)
spec2 = importlib.util.spec_from_file_location('release_tool', ROOT/'scripts/check_release.py')
release = importlib.util.module_from_spec(spec2)
spec2.loader.exec_module(release)


class SetupTests(unittest.TestCase):
    def test_init_and_upstream_do_not_overwrite_existing_credentials(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            shutil.copy(ROOT/'.env.example', root/'.env.example')
            shutil.copytree(ROOT/'config', root/'config')
            setup.init(root, 'https://images.example.com/', 'admin@example.com')
            env = (root/'.env').read_text()
            self.assertNotIn('CHANGE_ME', env)
            values = dict(line.split('=', 1) for line in env.splitlines()
                          if line and not line.startswith('#'))
            self.assertEqual(len(values['JWT_SECRET']), 64)
            self.assertNotEqual(values['JWT_SECRET'], values['POSTGRES_PASSWORD'])
            with self.assertRaises(ValueError):
                setup.init(root, 'https://images.example.com', 'admin@example.com')
            self.assertEqual((root/'.env').read_text(), env)
            setup.configure(root, 'isolated-test-key', 23)
            upstream = json.loads((root/'runtime/chatgpt2api/data/sub2api-image-upstream.json').read_text())
            self.assertEqual(upstream['key_id'], 23)
            self.assertEqual(upstream['base_url'], 'http://sub2api:8080')
            cfg = json.loads((root/'runtime/chatgpt2api/config.json').read_text())
            self.assertEqual(cfg['base_url'], 'https://images.example.com')

    def test_invalid_url_and_email_rejected_before_writing(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            for url, email in [('file:///tmp/test', 'a@example.com'),
                               ('https://example.com', 'a@example.com\nEVIL=true')]:
                with self.assertRaises(ValueError):
                    setup.init(root, url, email)
            self.assertFalse((root/'.env').exists())

    def test_release_scan_rejects_private_files_and_detects_tokens(self):
        self.assertIn('private-runtime-file', release.inspect_file(Path('runtime/config.json'), b'{}'))
        self.assertIn('private-runtime-file', release.inspect_file(Path('.env'), b'A=B'))
        token = ('ghp_' + 'Abcdef1234567890' * 3).encode()
        self.assertIn('github-token', release.inspect_file(Path('source.py'), token))
        self.assertEqual(release.inspect_file(Path('config/example.json'), b'{"api_key":"REPLACE_ME"}'), [])


if __name__ == '__main__':
    unittest.main()
