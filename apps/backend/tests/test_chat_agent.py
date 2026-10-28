"""Unit tests for chat.agent — stateless retry, prompt/error helpers, and the
session cache (reuse / reset / reconnect).

The agno pieces (`_run_stateless`, `_connect`) are monkeypatched, so these run
with no LLM or MCP infrastructure.
"""

from __future__ import annotations

import asyncio

import pytest

from chat import agent
from chat.models import ChatMessage


async def _noop_sleep(*_args, **_kwargs):
    return None


@pytest.fixture(autouse=True)
def _clear_sessions_and_sleep(monkeypatch):
    """Isolate each test: empty the session cache and skip retry backoff."""
    agent._sessions.clear()
    monkeypatch.setattr(agent.asyncio, "sleep", _noop_sleep)
    yield
    agent._sessions.clear()


# --------------------------------------------------------------------------- #
# Stateless path (no session_id)
# --------------------------------------------------------------------------- #

def test_run_agent_retries_then_succeeds(monkeypatch):
    calls = {"n": 0}

    async def flaky(prompt):
        calls["n"] += 1
        if calls["n"] < 2:
            raise RuntimeError("transient server blip")
        return "the answer"

    monkeypatch.setattr(agent, "_run_stateless", flaky)
    out = asyncio.run(agent.run_agent("who is 42", []))
    assert out == "the answer"
    assert calls["n"] == 2


def test_run_agent_raises_after_max_attempts(monkeypatch):
    calls = {"n": 0}

    async def always_fail(prompt):
        calls["n"] += 1
        raise RuntimeError("server down")

    monkeypatch.setattr(agent, "_run_stateless", always_fail)
    with pytest.raises(RuntimeError, match="server down"):
        asyncio.run(agent.run_agent("who is 42", []))
    assert calls["n"] == agent._MAX_ATTEMPTS


def test_run_agent_builds_prompt_with_history(monkeypatch):
    captured = {}

    async def capture(prompt):
        captured["prompt"] = prompt
        return "ok"

    monkeypatch.setattr(agent, "_run_stateless", capture)
    asyncio.run(agent.run_agent(
        "and their balance?",
        [ChatMessage(role="user", content="who is 42"),
         ChatMessage(role="assistant", content="Jane Doe")],
    ))
    assert "Conversation so far:" in captured["prompt"]
    assert "user: who is 42" in captured["prompt"]
    assert "assistant: Jane Doe" in captured["prompt"]
    assert captured["prompt"].rstrip().endswith("User: and their balance?")


def test_error_detail_surfaces_code_and_message():
    class FakeMCPError(Exception):
        code = -32603
        message = "Server returned an error response"
        data = None

    detail = agent._error_detail(FakeMCPError("boom"))
    assert "FakeMCPError" in detail
    assert "code=-32603" in detail
    assert "message=Server returned an error response" in detail


# --------------------------------------------------------------------------- #
# Session cache (with session_id)
# --------------------------------------------------------------------------- #

class _FakeTools:
    def __init__(self):
        self.closed = False

    async def close(self):
        self.closed = True


class _FakeAgent:
    def __init__(self, session_id, tools):
        self.session_id = session_id
        self.tools = tools
        self.runs = 0

    async def arun(self, input, session_id=None):  # noqa: A002 — mirror agno
        self.runs += 1
        return type("R", (), {"content": f"answer-{self.runs}"})()


def _fake_connect_factory():
    connects = {"n": 0, "agents": []}

    async def fake_connect(session_id):
        connects["n"] += 1
        tools = _FakeTools()
        ag = _FakeAgent(session_id, tools)
        connects["agents"].append(ag)
        return ag, tools

    return fake_connect, connects


def test_session_reuses_one_connection_across_turns(monkeypatch):
    fake_connect, connects = _fake_connect_factory()
    monkeypatch.setattr(agent, "_connect", fake_connect)

    r1 = asyncio.run(agent.run_agent("first", [], session_id="s1"))
    r2 = asyncio.run(agent.run_agent("second", [], session_id="s1"))

    assert r1 == "answer-1"
    assert r2 == "answer-2"           # same cached agent, run counter advanced
    assert connects["n"] == 1         # connected only once
    assert connects["agents"][0].runs == 2


def test_distinct_sessions_get_distinct_connections(monkeypatch):
    fake_connect, connects = _fake_connect_factory()
    monkeypatch.setattr(agent, "_connect", fake_connect)

    asyncio.run(agent.run_agent("hi", [], session_id="s1"))
    asyncio.run(agent.run_agent("hi", [], session_id="s2"))
    assert connects["n"] == 2
    assert set(agent._sessions) == {"s1", "s2"}


def test_reset_closes_and_reconnects_next_turn(monkeypatch):
    fake_connect, connects = _fake_connect_factory()
    monkeypatch.setattr(agent, "_connect", fake_connect)

    asyncio.run(agent.run_agent("hi", [], session_id="s1"))
    first_tools = connects["agents"][0].tools

    existed = asyncio.run(agent.reset_session("s1"))
    assert existed is True
    assert first_tools.closed is True
    assert "s1" not in agent._sessions

    asyncio.run(agent.run_agent("again", [], session_id="s1"))
    assert connects["n"] == 2          # reconnected after reset


def test_reset_unknown_session_is_false(monkeypatch):
    fake_connect, _ = _fake_connect_factory()
    monkeypatch.setattr(agent, "_connect", fake_connect)
    assert asyncio.run(agent.reset_session("nope")) is False


def test_failed_turn_evicts_cached_session(monkeypatch):
    connects = {"n": 0}

    async def fake_connect(session_id):
        connects["n"] += 1
        tools = _FakeTools()

        class _BadAgent:
            async def arun(self, input, session_id=None):  # noqa: A002
                raise RuntimeError("connection dropped")

        return _BadAgent(), tools

    monkeypatch.setattr(agent, "_connect", fake_connect)
    with pytest.raises(RuntimeError, match="connection dropped"):
        asyncio.run(agent.run_agent("hi", [], session_id="s1"))
    # each failed attempt evicts, so the session is not left cached
    assert "s1" not in agent._sessions
    assert connects["n"] == agent._MAX_ATTEMPTS  # reconnected each attempt


def test_shutdown_closes_all_sessions(monkeypatch):
    fake_connect, connects = _fake_connect_factory()
    monkeypatch.setattr(agent, "_connect", fake_connect)

    asyncio.run(agent.run_agent("hi", [], session_id="s1"))
    asyncio.run(agent.run_agent("hi", [], session_id="s2"))
    tools = [ag.tools for ag in connects["agents"]]

    asyncio.run(agent.shutdown())
    assert agent._sessions == {}
    assert all(t.closed for t in tools)
