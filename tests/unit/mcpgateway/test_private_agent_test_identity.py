#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Test owner identity and deny paths for private agent test buttons.
Location: ./tests/unit/mcpgateway/test_private_agent_test_identity.py
Copyright contributors to the MCP-CONTEXT-FORGE project
SPDX-License-Identifier: Apache-2.0
"""

# Standard
from unittest.mock import AsyncMock, MagicMock

# Third-Party
from fastapi import HTTPException, Request
import pytest
from sqlalchemy.orm import Session

# First-Party
from mcpgateway import admin, main
from mcpgateway.db import A2AAgent
from mcpgateway.services.a2a_service import A2AAgentNotFoundError, A2AAgentService
from mcpgateway.services.tool_service import ToolNotFoundError, ToolService


def scoped_request(email, teams):
    """Create request context with verified API-token visibility scope."""
    request = Request(
        {"type": "http", "method": "POST", "path": "/rpc", "headers": [], "state": {"token_teams": teams, "_jwt_verified_payload": ("verified-test-token", {"is_admin": True, "teams": teams})}}
    )
    request._body = b'{"query":"Hello"}'
    user = {"email": email, "is_admin": True}
    return request, user


@pytest.mark.parametrize("email,teams,allowed", [("owner@example.com", None, True), ("other@example.com", None, False), ("owner@example.com", [], False), ("other@example.com", ["other-team"], False)])
async def test_private_agent_test_uses_owner_and_token_scope(monkeypatch, email, teams, allowed):
    db = MagicMock(spec=Session)
    request, user = scoped_request(email, teams)
    user["db"] = db
    service = A2AAgentService()
    agent = A2AAgent(id="pilot-id", name="pilot", endpoint_url="https://agent.example/a2a", agent_type="generic", visibility="private", owner_email="owner@example.com", enabled=True)
    monkeypatch.setattr("mcpgateway.services.a2a_service.is_user_admin", lambda *_args: True)

    async def get_agent(_db, _agent_id, *, user_email, token_teams):
        if not await service._check_agent_access(_db, agent, user_email, token_teams):
            raise A2AAgentNotFoundError("Agent not found")
        return agent

    monkeypatch.setattr(service, "get_agent", AsyncMock(side_effect=get_agent))
    monkeypatch.setattr(service, "invoke_agent", AsyncMock(return_value={"response": "OK"}))
    monkeypatch.setattr(admin, "a2a_service", service)
    monkeypatch.setattr(admin.settings, "mcpgateway_a2a_enabled", True)
    response = await admin.admin_test_a2a_agent("pilot-id", request, db, user=user)
    assert response.status_code == (200 if allowed else 404)
    service.get_agent.assert_awaited_once_with(db, "pilot-id", user_email=email, token_teams=teams)
    if allowed:
        assert service.invoke_agent.call_args.kwargs["user_email"] == email
        assert service.invoke_agent.call_args.kwargs["token_teams"] is None
    else:
        service.invoke_agent.assert_not_called()


@pytest.mark.parametrize("email,teams,allowed", [("owner@example.com", None, True), ("other@example.com", None, False), ("owner@example.com", [], False), ("other@example.com", ["other-team"], False)])
async def test_private_generated_tool_test_uses_owner_and_token_scope(monkeypatch, email, teams, allowed):
    db = MagicMock(spec=Session)
    request, user = scoped_request(email, teams)
    service = ToolService()
    tool = {"visibility": "private", "owner_email": "owner@example.com", "team_id": "pilot-team"}
    monkeypatch.setattr("mcpgateway.services.tool_service.is_user_admin", lambda *_args: True)
    monkeypatch.setattr(main.settings, "mcpgateway_tool_cancellation_enabled", False)

    async def invoke_tool(**kwargs):
        if not await service._check_tool_access(db, tool, kwargs["user_email"], kwargs["token_teams"]):
            raise ToolNotFoundError("Tool not found")
        return {"content": [{"type": "text", "text": "OK"}]}

    invoke = AsyncMock(side_effect=invoke_tool)
    monkeypatch.setattr(main.tool_service, "invoke_tool", invoke)
    call = main._execute_rpc_tools_call(request, db, user, req_id=1, params={"name": "a2a-pilot", "arguments": {"query": "Hello"}}, lowered_request_headers={}, server_id=None)
    if allowed:
        assert (await call)["content"][0]["text"] == "OK"
    else:
        with pytest.raises(main.JSONRPCError, match="Tool not found"):
            await call
    assert invoke.call_args.kwargs["user_email"] == email
    assert invoke.call_args.kwargs["token_teams"] == teams


async def test_private_agent_test_requires_invoke_permission(monkeypatch, mock_permission_service):
    request, user = scoped_request("owner@example.com", None)
    db = MagicMock(spec=Session)
    user["db"] = db
    mock_permission_service.check_permission.return_value = False
    service = MagicMock()
    monkeypatch.setattr(admin, "a2a_service", service)
    with pytest.raises(HTTPException) as error:
        await admin.admin_test_a2a_agent("pilot-id", request, db, user=user)
    assert error.value.status_code == 403
    service.get_agent.assert_not_called()
