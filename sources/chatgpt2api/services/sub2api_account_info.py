"""Sync account metadata from the configured Sub2API, without Web probes."""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

def _lookup(account):
    from services.routing_mode import native_selected
    if native_selected():
        return None
    from services import sub2_gateway
    if sub2_gateway.enabled():
        sub2_gateway.settings()
    if not os.environ.get("IMAGE_UPSTREAM_CONFIG") or (not sub2_gateway.enabled() and account.get("source_type") != "codex"):
        return None
    from services.config import DATA_DIR
    from services.sub2api_service import _auth_headers
    from curl_cffi.requests import Session
    route=json.loads(Path(os.environ["IMAGE_UPSTREAM_CONFIG"]).read_text())
    servers=json.loads((Path(DATA_DIR)/"sub2api_config.json").read_text())
    matching=[s for s in servers if s.get("base_url","").rstrip("/")==route["base_url"].rstrip("/")]
    if len(matching)!=1:
        raise RuntimeError("Sub2API account sync requires one matching configured server")
    server=matching[0]
    matches=[]
    with Session() as client:
        for page in range(1,101):
            r=client.get(server["base_url"].rstrip("/")+"/api/v1/admin/accounts",
                headers=_auth_headers(server),params={"platform":"openai","type":"oauth","page":page,"page_size":200},timeout=20)
            if r.status_code!=200:raise RuntimeError("Sub2API account sync failed: HTTP "+str(r.status_code))
            data=r.json().get("data",{});items=data.get("items",[])
            for item in items:
                cred=item.get("credentials") or {}
                uid=str(account.get("user_id") or "")
                email=str(account.get("email") or "").strip().lower()
                if (uid and uid==str(cred.get("chatgpt_user_id") or "")) or (email and email==str(cred.get("email") or "").strip().lower()):
                    matches.append(item)
            if len(items)<200 or page*200>=int(data.get("total",0)):break
    if len(matches)!=1:
        raise RuntimeError("Sub2API account sync found "+str(len(matches))+" matching accounts")
    return server, matches[0]

def fetch(account):
    found = _lookup(account)
    if found is None:
        return None
    server, item = found
    cred=item.get("credentials") or {};extra=item.get("extra") or {}
    state=item.get("status")
    status="正常" if state=="active" and item.get("schedulable",False) else "禁用" if state in ("disabled","inactive") else "异常"
    return {
        "status":status,"type":cred.get("plan_type") or account.get("type"),
        "account_info_source":"sub2api","account_info_updated_at":datetime.now(timezone.utc).isoformat(),
        "image_quota_known":False,"limits_progress":[],
        "quota":0,"restore_at":None,"default_model_slug":None,
        "image_quota_reported":None,"image_quota_estimated":False,
        "sub2api_account_id":item["id"],"sub2api_schedulable":bool(item.get("schedulable")),
        "sub2api_usage":{k:extra.get(k) for k in ("codex_5h_used_percent","codex_7d_used_percent","codex_usage_updated_at")},
    }

def refresh_credentials(account):
    from services.sub2api_service import _auth_headers
    from curl_cffi.requests import Session
    server, item = _lookup(account)
    with Session() as client:
        response = client.post(server["base_url"].rstrip("/")+"/api/v1/admin/openai/accounts/"+str(item["id"])+"/refresh",
            headers=_auth_headers(server), timeout=60)
        if response.status_code != 200:
            raise RuntimeError("Sub2API account refresh failed: HTTP "+str(response.status_code))
    _, item = _lookup(account)
    credentials = item.get("credentials") or {}
    if not credentials.get("access_token"):
        raise RuntimeError("Sub2API did not return refreshed account credentials")
    return {k:credentials[k] for k in ("access_token","refresh_token","id_token") if credentials.get(k)}
