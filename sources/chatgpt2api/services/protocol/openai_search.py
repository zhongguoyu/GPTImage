from __future__ import annotations

from services.account_service import account_service
from services.openai_backend_api import OpenAIBackendAPI, SEARCH_MODEL
from services.scheduler.context import current_execution_context

MODEL = SEARCH_MODEL


def handle(body: dict[str, object]) -> dict[str, object]:
    from services import sub2_gateway
    if sub2_gateway.enabled():
        return sub2_gateway.search(str(body["prompt"]))
    context = current_execution_context()
    token = (
        context.lease.access_token
        if context is not None and context.lease is not None
        else account_service.get_text_access_token()
    )
    account = account_service.get_account(token) or {}
    backend = OpenAIBackendAPI(token)
    try:
        result = backend.search(str(body["prompt"]))
    finally:
        backend.close()
    if context is None:
        account_service.mark_text_used(token)
    result["_account_email"] = str(account.get("email") or "")
    return result
