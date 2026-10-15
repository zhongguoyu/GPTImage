"""Read-only operational image status, never an estimate of image quota."""
import json,sqlite3,time,statistics,threading
from datetime import datetime,timedelta,timezone
from pathlib import Path
from services.config import DATA_DIR
_lock=threading.Lock()
_pool_cache=None
_pool_at=0.0

def timestamp(value):
    if isinstance(value,(int,float)):return float(value)
    try:
        d=datetime.fromisoformat(str(value).replace("Z","+00:00"))
        return (d if d.tzinfo else d.replace(tzinfo=timezone.utc)).timestamp()
    except (ValueError,TypeError):return 0.0

def summarize(rows,now):
    day=datetime.fromtimestamp(now,timezone(timedelta(hours=8))).replace(hour=0,minute=0,second=0,microsecond=0).timestamp()
    finished=sorted([r for r in rows if r["status"] in ("success","error")],key=lambda r:r["updated_at"],reverse=True)
    recent=[r for r in finished if r["updated_at"]>=now-86400][:20]
    successful=[r for r in finished if r["status"]=="success"]
    today={"generate":0,"edit":0}
    for row in successful:
        if row["updated_at"]>=day:
            result=json.loads(row.get("result_json") or "{}")
            today["edit" if row["task_type"]=="edit" else "generate"]+=int(result.get("count") or 1)
    durations=[]
    for r in recent:
        if r["status"]!="success":continue
        meta=json.loads(r.get("metadata_json") or "{}")
        created=timestamp(meta.get("created_ts")) or timestamp(meta.get("created_at")) or r["created_at"]
        elapsed=r["updated_at"]-created
        if elapsed>0:durations.append(elapsed)
    last=finished[0] if finished else None
    last_success=successful[0]["updated_at"] if successful else None
    consecutive=0
    for row in finished:
        if row["status"]!="error":break
        consecutive+=1
    state="unverified"
    if last and now-last["updated_at"]<=1800:
        state="success" if last["status"]=="success" else "failed"
    error=""
    if last and last["status"]=="error":
        raw=str(last.get("error") or "").lower()
        error=("上游限流" if "429" in raw else "上游访问被拒绝" if "403" in raw else
               "上游模型或资源不存在" if "404" in raw else "请求超时" if "timeout" in raw else "图片任务失败，请查看任务详情")
    return {"state":state,"today":today,"recent_count":len(recent),
        "success_rate":round(100*sum(r["status"]=="success" for r in recent)/len(recent),1) if recent else None,
        "median_seconds":round(statistics.median(durations),1) if durations else None,
        "duration_samples":len(durations),"last_success":last_success,
        "consecutive_failures":consecutive,"last_error":error,
        "recent_from":recent[-1]["updated_at"] if recent else None,
        "recent_to":recent[0]["updated_at"] if recent else None}

def get_pool():
    global _pool_cache,_pool_at
    with _lock:
        if _pool_cache is not None and time.time()-_pool_at<30:return _pool_cache
        from services.sub2api_service import _auth_headers
        from curl_cffi.requests import Session
        cfg=json.loads((Path(DATA_DIR)/"sub2api-image-upstream.json").read_text())
        servers=json.loads((Path(DATA_DIR)/"sub2api_config.json").read_text())
        server=next(s for s in servers if s.get("base_url")==cfg["base_url"])
        with Session() as c:
            headers=_auth_headers(server)
            r=c.get(cfg["base_url"]+"/api/v1/keys",headers=headers,params={"page_size":200},timeout=8)
            r.raise_for_status()
            key=next(k for k in r.json()["data"]["items"] if k["id"]==cfg["key_id"])
            group=key.get("group_id")
            if group is None:raise RuntimeError("Unknown upstream group")
            items=[]
            for page in range(1,21):
                r=c.get(cfg["base_url"]+"/api/v1/admin/accounts",headers=headers,
                    params={"platform":"openai","type":"oauth","group":group,"page":page,"page_size":200},timeout=8)
                r.raise_for_status();data=r.json()["data"]
                items.extend(x for x in data["items"] if group in x.get("group_ids",[]))
                if page*200>=data.get("total",0):break
        _pool_cache=items;_pool_at=time.time()
        return items

def pool_summary(accounts,model,now):
    entries=[]
    for a in accounts:
        extra=a.get("extra") or {}
        limits=extra.get("model_rate_limits") or {}
        cooldowns=[timestamp(a.get(k)) for k in ("rate_limit_reset_at","overload_until","temp_unschedulable_until")]
        for key in ("openai:image_generation",model):
            cooldowns.append(timestamp((limits.get(key) or {}).get("rate_limit_reset_at")))
        until=max(cooldowns or [0])
        cooling=until>now
        active=a.get("status")=="active" and bool(a.get("schedulable")) and not cooling
        windows=[]
        for key,label in [("5h","5小时"),("7d","7天")]:
            used=extra.get("codex_"+key+"_used_percent")
            if isinstance(used,(int,float)):
                windows.append({"label":label,"remaining":round(max(0,min(100,100-used)),1),
                    "reset_at":timestamp(extra.get("codex_"+key+"_reset_at")),
                    "stale":bool(timestamp(extra.get("codex_"+key+"_reset_at")) and timestamp(extra.get("codex_"+key+"_reset_at"))<=now)})
        entries.append({"label":"账户 "+str(a["id"]),"available":active,"cooling":cooling,
            "cooldown_until":until if cooling else None,"cooldown_source":"unknown" if cooling else None,
            "windows":windows,"usage_updated_at":extra.get("codex_usage_updated_at")})
    return {"available":sum(x["available"] for x in entries),"cooling":sum(x["cooling"] for x in entries),
            "total":len(entries),"accounts":entries,"checked_at":_pool_at or now}

def status(model="gpt-image-2"):
    now=time.time()
    path=Path(DATA_DIR)/"app.db"
    with sqlite3.connect("file:"+str(path)+"?mode=ro",uri=True,timeout=3) as db:
        db.row_factory=sqlite3.Row
        rows=[dict(r) for r in db.execute("SELECT status,task_type,created_at,updated_at,result_json,error,metadata_json FROM tasks WHERE task_type IN ('generate','edit')")]
    result=summarize(rows,now)
    result.update({"updated_at":now,"model":model,"scope":"ChatGPT2API 图片任务；不含直接调用 Sub2API 的请求","pool":None,"pool_error":None})
    try:result["pool"]=pool_summary(get_pool(),model,now)
    except Exception:result["pool_error"]="账户池状态暂时无法获取"
    return result
