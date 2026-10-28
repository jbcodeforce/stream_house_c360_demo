"""Orchestration for the Assistant chat.

Two modes, selected by ``CHAT_DATA_MODE``:

* **stub** (default) — a canned reply, no LLM. Keeps the no-infra test suite and
  a laptop without the MLX server working.
* **live** — an agno agent (``chat.agent``) backed by a local LLM + the RTCE
  context-engine MCP server.

Robustness mirrors the analytics resource: never raise to the caller. A "live"
request with missing LLM/MCP config degrades to the stub reply; an agent failure
returns a friendly error reply. ``source_mode`` labels which path answered
("stub" | "live" | "error") so the UI/tests can tell.

Config is read via a late ``import config`` so tests that patch settings take
effect here.
"""

from __future__ import annotations

import logging

import config
from chat.models import ChatRequest, ChatResponse, WelcomeResponse

logger = logging.getLogger("c360.chat")


def get_welcome() -> WelcomeResponse:
    """Return the empty-state greeting from ``CHAT_FIRST_PROMPT``."""
    return WelcomeResponse(prompt=config.settings.CHAT_FIRST_PROMPT, source_mode="stub")


def _live_ready() -> bool:
    """True when live mode is selected and the LLM + MCP config is complete."""
    s = config.settings
    return bool(
        s.CHAT_DATA_MODE == "live"
        and s.LLM_BASE_URL
        and s.LLM_MODEL
        and s.rtce_mcp_url()
        and s.CONFLUENT_GLOBAL_KEY
        and s.CONFLUENT_GLOBAL_SECRET
    )


def _stub_reply(message: str) -> str:
    return (
        f'You asked: "{message}". The assistant is not connected yet — the '
        "real-time context engine over fct_c360_profile is wired up in the next step."
    )


async def get_reply(request: ChatRequest) -> ChatResponse:
    """Answer a user question, degrading gracefully instead of raising."""
    message = request.message.strip()
    session_id = request.session_id

    if not _live_ready():
        return ChatResponse(reply=_stub_reply(message), source_mode="stub", session_id=session_id)

    try:
        from chat import agent  # late import: only load agno on the live path

        reply = await agent.run_agent(message, request.history, session_id)
        return ChatResponse(reply=reply or "(no answer)", source_mode="live", session_id=session_id)
    except Exception:
        logger.exception("chat agent failed")
        return ChatResponse(
            reply="Sorry — I couldn't reach the assistant backend just now. Please try again.",
            source_mode="error",
            session_id=session_id,
        )


async def reset_session(session_id: str) -> bool:
    """Close and forget a cached session. Returns whether one existed."""
    try:
        from chat import agent

        return await agent.reset_session(session_id)
    except Exception:
        logger.exception("chat session reset failed")
        return False
