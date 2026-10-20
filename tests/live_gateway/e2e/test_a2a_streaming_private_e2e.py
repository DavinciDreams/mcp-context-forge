#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Verify private agent tests and SSE through a running gateway.
Location: ./tests/live_gateway/e2e/test_a2a_streaming_private_e2e.py
Copyright contributors to the MCP-CONTEXT-FORGE project
SPDX-License-Identifier: Apache-2.0
"""

# Standard
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import threading
import time

# Third-Party
import httpx
import pytest

# First-Party
from tests.helpers.auth import make_auth_headers, make_test_jwt


class PilotHandler(BaseHTTPRequestHandler):
    """Serve a deterministic agent with a completion gate."""

    release = threading.Event()

    def do_GET(self):
        """Return an agent card for the health probe."""
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"name":"pilot","capabilities":{"streaming":true}}')

    def do_POST(self):
        """Stream a working event before the test releases completion."""
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        streaming = body.get("method") == "message/stream"
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream" if streaming else "application/json")
        self.end_headers()
        if streaming:
            self.wfile.write(b'data: {"jsonrpc":"2.0","id":1,"result":{"taskId":"task-1","status":{"state":"working"}}}\n\n')
            self.wfile.flush()
            assert self.release.wait(15)
            self.wfile.write(b'data: {"jsonrpc":"2.0","id":1,"result":{"taskId":"task-1","status":{"state":"completed"},"final":true}}\n\n')
        else:
            self.wfile.write(json.dumps({"jsonrpc": "2.0", "id": body.get("id"), "result": {"message": "PRIVATE_PILOT_OK"}}).encode())

    def log_message(self, *_args):
        """Suppress the disposable upstream access log."""


@pytest.fixture
def live_pilot(tmp_path):
    """Start an isolated gateway and loopback-only upstream."""
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), PilotHandler)
    threading.Thread(target=upstream.serve_forever, daemon=True).start()
    PilotHandler.release.clear()
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    signing_key = secrets.token_urlsafe(48)
    env = {
        "PATH": os.environ["PATH"],
        "PYTHONPATH": str(Path(__file__).resolve().parents[3]),
        "DATABASE_URL": f"sqlite:///{tmp_path / 'gateway.db'}",
        "CACHE_TYPE": "memory",
        "REDIS_URL": "",
        "JWT_SECRET_KEY": signing_key,
        "AUTH_ENCRYPTION_SECRET": secrets.token_urlsafe(48),
        "PLATFORM_ADMIN_PASSWORD": secrets.token_urlsafe(32),
        "DEFAULT_USER_PASSWORD": secrets.token_urlsafe(32),
        "PLATFORM_ADMIN_EMAIL": "admin@example.com",
        "AUTH_REQUIRED": "true",
        "REQUIRE_USER_IN_DB": "false",
        "MCPGATEWAY_ADMIN_API_ENABLED": "true",
        "MCPGATEWAY_UI_ENABLED": "false",
        "MCPGATEWAY_A2A_ENABLED": "true",
        "SSRF_ALLOW_LOCALHOST": "true",
        "SSRF_ALLOW_PRIVATE_NETWORKS": "true",
        "PLUGINS_ENABLED": "false",
        "LOG_LEVEL": "ERROR",
    }
    env.update({name: os.environ[name] for name in ("SYSTEMROOT", "WINDIR", "TEMP", "TMP") if name in os.environ})
    owner = make_test_jwt("admin@example.com", is_admin=True, teams=None, secret=signing_key, algorithm="HS256")
    other = make_test_jwt("other@example.com", is_admin=True, teams=None, secret=signing_key, algorithm="HS256")
    log_path = tmp_path / "gateway.log"
    with log_path.open("w") as log_file:
        process = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "mcpgateway.main:app", "--host", "127.0.0.1", "--port", str(port)], cwd=tmp_path, env=env, stdout=log_file, stderr=subprocess.STDOUT
        )
        try:
            with httpx.Client(base_url=f"http://127.0.0.1:{port}", headers=make_auth_headers(owner), timeout=20, trust_env=False) as client:
                deadline = time.monotonic() + 60
                while time.monotonic() < deadline:
                    assert process.poll() is None, log_path.read_text()[-5000:]
                    try:
                        if client.get("/health").status_code == 200:
                            break
                    except httpx.TransportError:
                        pass
                    time.sleep(0.1)
                else:
                    pytest.fail(log_path.read_text()[-5000:])
                yield client, other, upstream.server_port
        finally:
            PilotHandler.release.set()
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            upstream.shutdown()
            upstream.server_close()


def test_private_buttons_and_incremental_stream(live_pilot):
    """Verify both private test routes and incremental SSE without service mocks."""
    client, other, upstream_port = live_pilot
    created = client.post(
        "/a2a", json={"agent": {"name": "pilot", "endpoint_url": f"http://127.0.0.1:{upstream_port}/a2a", "agent_type": "generic", "protocol_version": "0.3"}, "visibility": "private"}
    )
    assert created.status_code == 201, created.text
    agent = created.json()
    tested = client.post(f"/admin/a2a/{agent['id']}/test", json={"query": "Hello"})
    assert tested.status_code == 200, tested.text
    assert tested.json()["success"] is True
    assert "PRIVATE_PILOT_OK" in tested.text
    tools_response = client.get("/tools")
    assert tools_response.status_code == 200, tools_response.text
    tools = tools_response.json()
    if isinstance(tools, dict):
        tools = tools["tools"]
    tool = next(tool for tool in tools if tool.get("annotations", {}).get("a2a_agent_id") == agent["id"])
    rpc = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": tool["name"], "arguments": {"query": "Hello"}}}
    tested = client.post("/rpc", json=rpc)
    assert tested.status_code == 200, tested.text
    assert "PRIVATE_PILOT_OK" in tested.text and "error" not in tested.json()
    denied = client.post(f"/admin/a2a/{agent['id']}/test", json={"query": "Hello"}, headers=make_auth_headers(other))
    assert denied.status_code in (403, 404), denied.text
    body = {"jsonrpc": "2.0", "method": "message/stream", "id": 1, "params": {"message": {"messageId": "m1", "role": "user", "parts": [{"kind": "text", "text": "Hello"}]}}}
    with client.stream("POST", "/a2a/pilot/jsonrpc", json=body) as response:
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        lines = response.iter_lines()
        assert '"working"' in next(line for line in lines if line.startswith("data:"))
        assert not PilotHandler.release.is_set()
        PilotHandler.release.set()
        assert any('"completed"' in line for line in lines)
