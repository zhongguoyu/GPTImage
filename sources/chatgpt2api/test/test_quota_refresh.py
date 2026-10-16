from pathlib import Path
from services.account_service import AccountService
from services.storage.json_storage import JSONStorageBackend
from services.account_metadata import native_metadata

def info(quota=1000, restore=None):
    return native_metadata({"quota":quota,"status":"正常" if quota else "限流","restore_at":restore,
                            "limits_progress":[{"feature_name":"image_gen","remaining":quota}]})

def service(tmp_path):
    svc=AccountService(JSONStorageBackend(tmp_path/"accounts.json"))
    svc.add_account_items([{"access_token":"test","quota":1000,"status":"正常"}])
    svc.update_account("test",info(),quiet=True)
    return svc

def test_refresh_does_not_restore_debited_quota(tmp_path):
    svc=service(tmp_path)
    pending_result=info()
    svc.mark_image_result("test",True,release_slot=False)
    for _ in range(3):
        svc.update_account("test",pending_result,quiet=True)
        assert svc.get_account("test")["quota"]==999
    assert svc.get_account("test")["image_quota_estimated"] is True
    assert svc.get_account("test")["image_quota_reported"]==1000

def test_switch_to_gateway_and_back_keeps_native_debit(tmp_path):
    svc=service(tmp_path);svc.mark_image_result("test",True,release_slot=False)
    svc.update_account("test",{"account_info_source":"sub2api","quota":0,"image_quota_known":False},quiet=True)
    svc.update_account("test",info(),quiet=True)
    assert svc.get_account("test")["quota"]==999

def test_restart_preserves_debit(tmp_path):
    svc=service(tmp_path);svc.mark_image_result("test",True,release_slot=False)
    reloaded=AccountService(JSONStorageBackend(tmp_path/"accounts.json"))
    reloaded.update_account("test",info(),quiet=True)
    assert reloaded.get_account("test")["quota"]==999

def test_lower_remote_quota_and_real_increase(tmp_path):
    svc=service(tmp_path);svc.mark_image_result("test",True,release_slot=False)
    svc.update_account("test",info(800),quiet=True)
    assert svc.get_account("test")["quota"]==800
    svc.update_account("test",info(1000),quiet=True)
    assert svc.get_account("test")["quota"]==1000

def test_elapsed_reset_allows_one_refill(tmp_path):
    svc=service(tmp_path)
    saved=info(1000,"2000-01-02T00:00:00+00:00")
    saved["account_info_updated_at"]="2000-01-01T00:00:00+00:00"
    svc.update_account("test",saved,quiet=True)
    svc.mark_image_result("test",True,release_slot=False)
    svc.update_account("test",info(1000,"2000-01-02T00:00:00+00:00"),quiet=True)
    assert svc.get_account("test")["quota"]==1000
    svc.mark_image_result("test",True,release_slot=False)
    svc.update_account("test",info(1000,"2000-01-02T00:00:00+00:00"),quiet=True)
    assert svc.get_account("test")["quota"]==999
