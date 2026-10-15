"""Generate private runtime configuration; no third-party dependencies."""
import argparse
import getpass
import json
import os
from pathlib import Path
import secrets
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]

def write_private(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    if os.name != 'nt':
        path.parent.chmod(0o700)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as out:
        out.write(text)
    if os.name != 'nt':
        path.chmod(0o600)

def init(root, public_url, admin_email):
    if urlsplit(public_url).scheme not in ('http', 'https') or not urlsplit(public_url).netloc:
        raise ValueError('public-url must be a full HTTP(S) URL')
    if '\n' in admin_email or '\r' in admin_email or '@' not in admin_email:
        raise ValueError('Invalid admin email')
    target = root/'runtime/chatgpt2api'
    if (root/'.env').exists() or target.exists():
        raise ValueError('Existing configuration detected; refusing to replace credentials')
    template = (root/'.env.example').read_text(encoding='utf-8')
    values = {k:secrets.token_hex(32) for k in (
        'ADMIN_PASSWORD','POSTGRES_PASSWORD','REDIS_PASSWORD','JWT_SECRET',
        'TOTP_ENCRYPTION_KEY','CHATGPT2API_AUTH_KEY')}
    values['ADMIN_EMAIL'] = admin_email
    lines=[]
    for line in template.splitlines():
        key=line.split('=',1)[0]
        lines.append(key+'='+values[key] if key in values else line)
    write_private(root/'.env', '\n'.join(lines)+'\n')
    cfg=json.loads((root/'config/chatgpt2api.example.json').read_text(encoding='utf-8'))
    cfg['base_url']=public_url.rstrip('/')
    write_private(target/'config.json',json.dumps(cfg,ensure_ascii=False,indent=2)+'\n')
    write_private(target/'data/sub2api-image-upstream.json',
        (root/'config/sub2api-image-upstream.example.json').read_text(encoding='utf-8'))
    (root/'runtime/sub2api').mkdir(parents=True,exist_ok=True)

def configure(root, api_key, key_id):
    path=root/'runtime/chatgpt2api/data/sub2api-image-upstream.json'
    if not path.exists():
        raise ValueError('Run init first')
    if not api_key.strip() or key_id <= 0:
        raise ValueError('A real API key and positive key ID are required')
    write_private(path,json.dumps({'base_url':'http://sub2api:8080',
        'api_key':api_key.strip(),'key_id':key_id},indent=2)+'\n')

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='command',required=True)
    cmd=sub.add_parser('init')
    cmd.add_argument('--public-url',default='http://localhost:3000')
    cmd.add_argument('--admin-email',default='admin@example.com')
    sub.add_parser('upstream')
    args=parser.parse_args()
    try:
        if args.command=='init':
            init(ROOT,args.public_url,args.admin_email)
            print('Created .env and runtime/. Secrets are stored only in .env; no values printed.')
        else:
            configure(ROOT,getpass.getpass('Dedicated Sub2API API key (hidden): '),
                int(input('Numeric ID of that key (not group ID): ').strip()))
            print('Upstream configuration saved. Start or restart chatgpt2api.')
    except (ValueError,OSError) as exc:
        parser.exit(1,str(exc)+'\n')

if __name__=='__main__':
    main()
