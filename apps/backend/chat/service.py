"""Orchestration for the Assistant chat.

Step 1 is a deliberate stub: the welcome prompt is read from config so the
frontend can verify end-to-end backend propagation on first connection, and
chat replies are canned echoes. Step 2 swaps ``get_reply`` for a local agent
that queries ``fct_c360_profile`` via the RTCE Lightning Query API — see
``analytics.lightning_client.LightningClient``.

Config is read via a late ``import config`` (not ``from config import settings``)
so tests that reload config with patched env take effect here.
"""

from __future__ import annotations

import config
from chat.models import ChatRequest, ChatResponse, WelcomeResponse

SOURCE_MODE = "stub"


def get_welcome() -> WelcomeResponse:
    """Return the empty-state greeting from ``CHAT_FIRST_PROMPT``."""
    return WelcomeResponse(prompt=config.settings.CHAT_FIRST_PROMPT, source_mode=SOURCE_MODE)


def get_reply(request: ChatRequest) -> ChatResponse:
    """Return a canned reply.

    Step 2 replaces this body with a local agent + RTCE query; the signature and
    response shape stay the same so the frontend contract does not change.
    """
    message = request.message.strip()
    reply = (
        f'You asked: "{message}". The assistant is not connected yet — the '
        "real-time context engine over fct_c360_profile is wired up in the next step."
    )
    return ChatResponse(reply=reply, source_mode=SOURCE_MODE)
