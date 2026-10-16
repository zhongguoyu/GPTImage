"""Account display metadata belongs to the upstream that supplied it."""
from datetime import datetime, timezone

def native_metadata(result):
    result = dict(result)
    known = any(isinstance(item, dict) and item.get("feature_name") == "image_gen"
                and isinstance(item.get("remaining"), (int, float))
                and not isinstance(item.get("remaining"), bool) and item["remaining"] >= 0
                for item in result.get("limits_progress", []))
    result.update(account_info_source="native", image_quota_known=known,
                  account_info_updated_at=datetime.now(timezone.utc).isoformat(),
                  sub2api_account_id=None, sub2api_schedulable=None, sub2api_usage={})
    return result

def display_metadata(account):
    from services.routing_mode import state, switched_at
    result = dict(account)
    mode = state()["mode"]
    try:
        updated = datetime.fromisoformat(str(result.get("account_info_updated_at", ""))).timestamp()
    except (ValueError, TypeError):
        updated = 0
    stale = not updated or result.get("account_info_source") != mode or updated < switched_at()
    result["account_info_stale"] = stale
    result["account_info_mode"] = mode
    if stale:
        result.update(image_quota_known=False, quota=0, restore_at=None,
                      limits_progress=[], default_model_slug=None, sub2api_usage={})
    if mode == "sub2api":
        result.update(image_quota_known=False, quota=0, restore_at=None,
                      limits_progress=[], default_model_slug=None, image_rate_limit_remaining_secs=0)
    return result


def _time(value):
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return (parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).timestamp()
    except (ValueError, TypeError):
        return 0

def native_quota_snapshot(account):
    saved = account.get("native_quota_state")
    if isinstance(saved, dict):
        return dict(saved)
    if account.get("account_info_source") == "native" and account.get("image_quota_known") is True:
        remaining = max(0, int(account.get("quota") or 0))
        reported = next((item.get("remaining") for item in account.get("limits_progress", [])
                         if isinstance(item, dict) and item.get("feature_name") == "image_gen"), remaining)
        return {"remaining": remaining, "reported": reported, "restore_at": account.get("restore_at"),
                "updated_at": account.get("account_info_updated_at")}
    return None

def merge_native_quota(current, updates):
    """Called under the account lock, so an in-flight refresh cannot erase debits."""
    updates = dict(updates)
    if updates.get("account_info_source") != "native":
        return updates
    saved = native_quota_snapshot(current)
    if updates.get("image_quota_known") is not True:
        if saved:
            updates["native_quota_state"] = saved
        return updates
    reported = max(0, int(updates.get("quota") or 0))
    remaining = reported
    now = _time(updates.get("account_info_updated_at"))
    if saved:
        previous = max(0, int(saved.get("remaining") or 0))
        old_reset, new_reset = _time(saved.get("restore_at")), _time(updates.get("restore_at"))
        # A changed future deadline alone can be a sliding window, not a refill.
        reset_elapsed = bool(old_reset and _time(saved.get("updated_at")) < old_reset <= now)
        upstream_increased = reported > int(saved.get("reported") or 0)
        if not reset_elapsed and not upstream_increased:
            remaining = min(reported, previous)
    updates["quota"] = remaining
    updates["image_quota_reported"] = reported
    updates["image_quota_estimated"] = remaining != reported
    updates["native_quota_state"] = {"remaining": remaining, "reported": reported,
                                    "restore_at": updates.get("restore_at"),
                                    "updated_at": updates.get("account_info_updated_at")}
    if remaining == 0 and updates.get("status") == "正常":
        updates["status"] = "限流"
    return updates

def record_native_debit(account):
    if account.get("account_info_source") != "native":
        return
    saved = native_quota_snapshot(account)
    if saved:
        saved["remaining"] = account["quota"]
        account["native_quota_state"] = saved
        account["image_quota_reported"] = saved["reported"]
        account["image_quota_estimated"] = account["quota"] != saved["reported"]
