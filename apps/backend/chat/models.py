"""Pydantic models for the Assistant chat.

The Assistant is a natural-language chat over the C360 data. Step 1 is a stub:
the welcome prompt comes from config (``CHAT_FIRST_PROMPT``) and replies are
canned. ``ChatRequest.history`` carries prior turns so step 2's local agent has
conversation context when it queries ``fct_c360_profile`` via RTCE.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel


class ChatMessage(BaseModel):
    """One turn in the conversation."""

    role: Literal["user", "assistant"]
    content: str


class ChatRequest(BaseModel):
    """A user question plus the prior turns of the conversation."""

    message: str
    history: list[ChatMessage] = []


class ChatResponse(BaseModel):
    """The assistant's reply.

    ``source_mode`` is "stub" until step 2 wires the local agent + RTCE, when it
    becomes "live" so the UI can label where the answer came from.
    """

    reply: str
    source_mode: str = "stub"


class WelcomeResponse(BaseModel):
    """The greeting shown in the chat's empty state, from ``CHAT_FIRST_PROMPT``."""

    prompt: str
    source_mode: str = "stub"
