import tempfile
from pathlib import Path
from unittest.mock import patch
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
import api.accounts as accounts
from services.quota_service import AuthQuotaStore, QuotaService, QuotaExceededError, QuotaLimitError
from test.test_quota_api import FakeAuthService

@pytest.fixture
def service(tmp_path):
    s = QuotaService(AuthQuotaStore(tmp_path / "quota.db"))
    s.initialize()
    yield s
    s.close()

def test_unlimited_counts_usage_and_can_return_to_finite(service):
    service.set_limits("u", text_unlimited=True, image_unlimited=True)
    for kind in ("text", "image"):
        r=service.reserve({"id":"u","role":"user"},kind,12)
        assert service.get_quota("u")[kind+"_reserved"] == 12
        service.commit(r.reservation_id,10)
        q=service.get_quota("u")
        assert q[kind+"_used"]==10 and q[kind+"_reserved"]==0
        assert q[kind+"_remaining"] is None
    with pytest.raises(QuotaLimitError):
        service.set_limits("u",image_unlimited=False,image_limit=9)
    service.set_limits("u",image_unlimited=False,image_limit=11)
    r=service.reserve({"id":"u","role":"user"},"image",1)
    with pytest.raises(QuotaExceededError):
        service.reserve({"id":"u","role":"user"},"image",1)
    service.release(r.reservation_id)
    service.mark_deleted("u")
    with pytest.raises(QuotaExceededError):
        service.reserve({"id":"u","role":"user"},"text",1)

def test_admin_defaults_and_explicit_limits(service):
    app=FastAPI()
    app.include_router(accounts.create_router())
    with patch.object(accounts,"auth_service",FakeAuthService()), patch.object(accounts,"quota_service",service), patch.object(accounts,"require_admin",return_value={"role":"admin"}):
        client=TestClient(app)
        r=client.post("/api/auth/users",json={"name":"isolated"})
        assert r.status_code==200
        q=r.json()["item"]
        assert q["text_unlimited"] and q["image_unlimited"]
        assert q["text_remaining"] is None
        r=client.post("/api/auth/users",json={"text_limit":2,"image_limit":0})
        assert r.status_code==200
        q=r.json()["item"]
        assert not q["text_unlimited"] and not q["image_unlimited"]
        assert q["text_remaining"]==2 and q["image_remaining"]==0
        r=client.post("/api/auth/users/"+q["id"],json={"image_unlimited":True})
        assert r.status_code==200 and r.json()["item"]["image_unlimited"]
        r=client.post("/api/auth/users/"+q["id"],json={"image_unlimited":False})
        assert r.status_code==200 and not r.json()["item"]["image_unlimited"]
