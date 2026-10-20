#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Test authorized A2A SSE forwarding and stream lifecycle.
Location: ./tests/unit/mcpgateway/services/test_a2a_streaming.py
Copyright contributors to the MCP-CONTEXT-FORGE project
SPDX-License-Identifier: Apache-2.0
"""

# Standard
import asyncio
from contextlib import asynccontextmanager
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

# Third-Party
import httpx
import pytest
from sqlalchemy.orm import Session

# First-Party
from mcpgateway.db import A2AAgent
from mcpgateway.services.a2a_service import A2AAgentError, A2AAgentNotFoundError, A2AAgentService


REQUEST = {"jsonrpc": "2.0", "id": "stream-1", "method": "message/stream", "params": {"message": {"messageId": "m1", "role": "user", "parts": [{"kind": "text", "text": "Hello"}]}}}
FIRST = b': heartbeat\r\nid: 1\r\ndata: {"jsonrpc":"2.0",\r\ndata: "result":{"taskId":"task-1","status":{"state":"working"}}}\r\n\r\n'
FINAL = b'data: {"jsonrpc":"2.0","result":{"taskId":"task-1","status":{"state":"completed"},"final":true}}\n\n'


class GatedStream(httpx.AsyncByteStream):
    """Emit one event before waiting for the test to release completion."""

    def __init__(self, error=False):
        """Create stream gates and a closure signal."""
        self.release = asyncio.Event()
        self.closed = asyncio.Event()
        self.error = error

    async def __aiter__(self):
        """Yield two events or fail after the first event."""
        yield FIRST
        await self.release.wait()
        if self.error:
            raise httpx.ReadError("upstream transport failed")
        yield FINAL

    async def aclose(self):
        """Signal HTTPX stream closure."""
        self.closed.set()


@pytest.fixture
def invocation(monkeypatch):
    """Use the real invocation service with isolated HTTP and persistence doubles."""
    service = A2AAgentService()
    db = MagicMock(spec=Session)
    agent = A2AAgent(
        id="agent-1", name="pilot", endpoint_url="https://agent.example/a2a", agent_type="generic", protocol_version="0.3", visibility="private", owner_email="owner@example.com", enabled=True
    )
    monkeypatch.setattr("mcpgateway.services.a2a_service.get_for_update", lambda *_args: agent)
    monkeypatch.setattr("mcpgateway.services.a2a_service.is_user_admin", lambda _db, email: email == "owner@example.com")
    monkeypatch.setattr(service, "_get_plugin_manager", AsyncMock(return_value=None))
    monkeypatch.setattr(service, "upsert_task", MagicMock())
    monkeypatch.setattr("mcpgateway.services.a2a_service.structured_logger", MagicMock())
    monkeypatch.setattr("mcpgateway.services.a2a_service.fresh_db_session", MagicMock())
    metrics = MagicMock()
    monkeypatch.setattr("mcpgateway.services.metrics_buffer_service.get_metrics_buffer_service", lambda: metrics)
    pinned = SimpleNamespace(endpoint_url="https://203.0.113.10/a2a", headers={"Host": "agent.example"}, extensions={"sni_hostname": "agent.example"})
    pinning = AsyncMock(return_value=pinned)
    monkeypatch.setattr("mcpgateway.services.a2a_service.prepare_pinned_a2a_invocation", pinning)
    return SimpleNamespace(service=service, db=db, agent=agent, metrics=metrics, pinning=pinning)


def install_transport(monkeypatch, response):
    """Install an isolated HTTPX transport and return its request history."""
    requests = []

    async def handle(request):
        requests.append(request)
        return response

    @asynccontextmanager
    async def client(**kwargs):
        assert kwargs == {"follow_redirects": False}
        async with httpx.AsyncClient(transport=httpx.MockTransport(handle), follow_redirects=False) as transport:
            yield transport

    monkeypatch.setattr("mcpgateway.services.a2a_service.get_isolated_http_client", client)
    return requests


async def read_event(events):
    """Read through the first event delimiter without waiting for completion."""
    parts = []
    async with asyncio.timeout(2):
        async for part in events:
            parts.append(part)
            if part == b"\n":
                return b"".join(parts)
    pytest.fail("No complete SSE event")


async def test_stream_arrives_before_completion_and_persists_final_status(invocation, monkeypatch):
    stream = GatedStream()
    requests = install_transport(monkeypatch, httpx.Response(200, headers={"content-type": "text/event-stream; charset=utf-8"}, stream=stream))
    events = await invocation.service.stream_agent(invocation.db, "pilot", REQUEST, user_email="owner@example.com", token_teams=None)
    assert await read_event(events) == FIRST.replace(b"\r\n", b"\n")
    assert not stream.release.is_set()
    invocation.metrics.record_a2a_agent_metric_with_duration.assert_not_called()
    stream.release.set()
    assert b"".join([part async for part in events]) == FINAL
    assert stream.closed.is_set()
    assert requests[0].url.host == "203.0.113.10"
    assert requests[0].headers["host"] == "agent.example"
    assert requests[0].extensions["sni_hostname"] == "agent.example"
    assert json.loads(requests[0].content)["method"] == "message/stream"
    assert invocation.service.upsert_task.call_args.args[2:4] == ("task-1", "completed")
    assert invocation.metrics.record_a2a_agent_metric_with_duration.call_args.kwargs["success"] is True


async def test_downstream_close_cancels_upstream_and_records_failure(invocation, monkeypatch):
    stream = GatedStream()
    install_transport(monkeypatch, httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream))
    events = await invocation.service.stream_agent(invocation.db, "pilot", REQUEST, user_email="owner@example.com", token_teams=None)
    await read_event(events)
    await events.aclose()
    assert stream.closed.is_set()
    assert invocation.metrics.record_a2a_agent_metric_with_duration.call_args.kwargs["success"] is False


async def test_close_before_first_iteration_releases_upstream(invocation, monkeypatch):
    stream = GatedStream()
    install_transport(monkeypatch, httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream))
    events = await invocation.service.stream_agent(invocation.db, "pilot", REQUEST, user_email="owner@example.com", token_teams=None)
    await events.aclose()
    assert stream.closed.is_set()


async def test_midstream_failure_returns_jsonrpc_error_and_closes_upstream(invocation, monkeypatch):
    stream = GatedStream(error=True)
    install_transport(monkeypatch, httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream))
    events = await invocation.service.stream_agent(invocation.db, "pilot", REQUEST, user_email="owner@example.com", token_teams=None)
    await read_event(events)
    stream.release.set()
    remaining = b"".join([part async for part in events])
    error = json.loads(remaining.removeprefix(b"data: ").strip())
    assert error["id"] == REQUEST["id"]
    assert error["error"]["code"] == -32603
    assert "upstream transport failed" not in remaining.decode()
    assert stream.closed.is_set()


@pytest.mark.parametrize("status", [401, 403, 500])
async def test_upstream_http_errors_fail_before_stream_starts(invocation, monkeypatch, status):
    install_transport(monkeypatch, httpx.Response(status, text="upstream unavailable"))
    with pytest.raises(A2AAgentError, match=f"HTTP {status}"):
        await invocation.service.stream_agent(invocation.db, "pilot", REQUEST, user_email="owner@example.com", token_teams=None)


async def test_streaming_method_can_receive_jsonrpc_error_response(invocation, monkeypatch):
    response = {"jsonrpc": "2.0", "id": REQUEST["id"], "error": {"code": -32601, "message": "Unsupported method"}}
    install_transport(monkeypatch, httpx.Response(200, json=response))
    assert await invocation.service.stream_agent(invocation.db, "pilot", REQUEST, user_email="owner@example.com", token_teams=None) == response


@pytest.mark.parametrize("email,teams", [(None, []), ("other@example.com", ["other-team"]), ("owner@example.com", [])])
async def test_streaming_preserves_private_access_denials(invocation, email, teams):
    with pytest.raises(A2AAgentNotFoundError):
        await invocation.service.stream_agent(invocation.db, "pilot", REQUEST, user_email=email, token_teams=teams)
    invocation.pinning.assert_not_called()


async def test_disabled_agent_cannot_start_stream(invocation):
    invocation.agent.enabled = False
    with pytest.raises(A2AAgentError, match="disabled"):
        await invocation.service.stream_agent(invocation.db, "pilot", REQUEST, user_email="owner@example.com", token_teams=None)
    invocation.pinning.assert_not_called()
