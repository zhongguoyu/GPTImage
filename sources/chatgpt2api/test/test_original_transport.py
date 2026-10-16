import unittest
from unittest.mock import patch,Mock
from services import original_web_backend as original
from services.openai_backend_api import OpenAIBackendAPI,InvalidAccessTokenError
from services import sub2_gateway

class OriginalTransportTests(unittest.TestCase):
    def test_original_session_and_text_endpoint(self):
        with patch.object(sub2_gateway,"enabled",return_value=False),patch.object(original.account_service,"get_account",return_value={}),patch.object(original.requests,"Session") as session:
            session.return_value.headers={}
            backend=OpenAIBackendAPI("test-token")
            self.assertEqual(session.call_args.kwargs["impersonate"],"chrome110")
            self.assertEqual(backend._chat_target()[0],"/backend-api/conversation")
            self.assertIn("Edg/143",backend.user_agent)
            backend.close()
    def test_original_image_protocol_and_comments(self):
        from types import SimpleNamespace
        backend=OpenAIBackendAPI.__new__(OpenAIBackendAPI)
        with patch.object(original.OpenAIBackendAPI,"stream_conversation",return_value=iter([])) as run:
            list(backend.stream_conversation(prompt="edit",images=["image"],system_hints=["picture_v2"],comments=[SimpleNamespace(image_index=0,x=.25,y=.5,text="make red")],markup=True))
        self.assertIn("point (25.0%, 50.0%): make red",run.call_args.kwargs["prompt"])
        self.assertEqual(run.call_args.kwargs["images"],["image"])
        self.assertIs(OpenAIBackendAPI._start_image_generation,original.OpenAIBackendAPI._start_image_generation)
    def test_original_exception_matches_existing_handlers(self):
        backend=OpenAIBackendAPI.__new__(OpenAIBackendAPI)
        with self.assertRaises(InvalidAccessTokenError):
            backend._raise_on_error(Mock(status_code=401),"/backend-api/me")
    def test_gateway_still_blocks_web_constructor(self):
        from fastapi import HTTPException
        with patch.object(sub2_gateway,"enabled",return_value=True):
            with self.assertRaises(HTTPException):OpenAIBackendAPI("test-token")
