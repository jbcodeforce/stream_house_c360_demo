"""FastAPI router for the /chat endpoints (Assistant).

Step 1 stub — ``GET /chat/welcome`` proves config propagation on first
connection; ``POST /chat`` returns a canned reply. See ``chat.service``.
"""

from __future__ import annotations

from fastapi import APIRouter

from chat import service
from chat.models import ChatRequest, ChatResponse, WelcomeResponse

router = APIRouter(prefix="/chat", tags=["chat"])


@router.get("/welcome", response_model=WelcomeResponse, status_code=200)
def get_welcome() -> WelcomeResponse:
    """The empty-state greeting, sourced from ``CHAT_FIRST_PROMPT``."""
    return service.get_welcome()


@router.post("", response_model=ChatResponse, status_code=200)
def post_chat(request: ChatRequest) -> ChatResponse:
    """Answer a user question. Step 1 returns stub text."""
    return service.get_reply(request)
