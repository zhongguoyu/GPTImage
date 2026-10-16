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
