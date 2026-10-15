"""Check selected release files without printing secret values."""
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
EXCLUDED = {'.git', '.venv', 'node_modules', '__pycache__', '.next'}


def release_files(root):
    if (root / '.git').exists():
        result = subprocess.run(['git', 'ls-files', '-z'], cwd=root,
                                capture_output=True, check=True)
        return [root / item.decode('utf-8') for item in result.stdout.split(b'\0') if item]
    return [p for p in root.rglob('*') if p.is_file()
            and not any(part in EXCLUDED for part in p.relative_to(root).parts)]


def inspect_file(relative, data):
    issues = []
    parts = relative.parts
    if parts[0] in {'runtime', 'backups', 'dist'} or relative.name == '.env':
        issues.append('private-runtime-file')
    if relative.suffix.lower() in {'.pem', '.p12', '.pfx', '.db', '.sqlite', '.sqlite3'}:
        issues.append('private-or-runtime-extension')
    text = data.decode('utf-8', errors='ignore')
    if re.search(r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----\s+[A-Za-z0-9+/=\r\n]{80,}', text):
        issues.append('private-key-material')
    if re.search(r'\bgh[pousr]_[A-Za-z0-9]{30,}\b', text):
        issues.append('github-token')
    if re.search(r'GOCSPX-[A-Za-z0-9_-]{20,}', text):
        issues.append('google-oauth-client-secret')
    if re.search(r'\b[0-9]+-[A-Za-z0-9_-]+\.apps\.googleusercontent\.com', text):
        issues.append('google-oauth-client-id')
    # Ignore obvious generated fixtures, but never print matches.
    for match in re.finditer(r'\bsk-[A-Za-z0-9_-]{32,}', text):
        token = match.group()
        if len(set(token[3:])) > 12 and not any(word in token.lower() for word in ('example', 'placeholder', 'test', 'dummy')):
            issues.append('possible-api-key')
    return sorted(set(issues))


def main():
    files = release_files(ROOT)
    failures = []
    for path in files:
        if not path.is_file():
            failures.append((str(path.relative_to(ROOT)), 'missing-selected-file'))
            continue
        for reason in inspect_file(path.relative_to(ROOT), path.read_bytes()):
            failures.append((path.relative_to(ROOT).as_posix(), reason))
    for name, reason in failures:
        print(f'{reason}: {name}')
    print(f'Checked {len(files)} release files; findings: {len(failures)}')
    return 1 if failures else 0


if __name__ == '__main__':
    sys.exit(main())
