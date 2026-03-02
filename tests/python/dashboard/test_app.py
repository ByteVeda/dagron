"""Tests for the Rust-backed dashboard routes."""

import json
import urllib.error
import urllib.request

import pytest

try:
    from dagron._internal import RustDashboardServer
except ImportError:
    pytest.skip(
        "Dashboard feature not compiled (build with --features dashboard)",
        allow_module_level=True,
    )


@pytest.fixture
def server():
    srv = RustDashboardServer("127.0.0.1", 0)
    yield srv
    srv.stop()


def _get(server, path):
    url = f"http://127.0.0.1:{server.port}{path}"
    req = urllib.request.Request(url)
    return urllib.request.urlopen(req)


def _post(server, path, body=None):
    url = f"http://127.0.0.1:{server.port}{path}"
    data = json.dumps(body).encode() if body else None
    headers = {"Content-Type": "application/json"} if data else {}
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        return urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        return e


class TestIndexRoute:
    def test_get_index_returns_html(self, server):
        resp = _get(server, "/")
        assert resp.status == 200
        ct = resp.headers.get("content-type", "")
        assert "text/html" in ct
        body = resp.read().decode()
        assert "dagron" in body


class TestAPIState:
    def test_get_state_returns_json(self, server):
        resp = _get(server, "/api/state")
        assert resp.status == 200
        data = json.loads(resp.read())
        assert "dag_dot" in data
        assert "nodes" in data
        assert "is_running" in data

    def test_get_state_after_reset(self, server):
        server.reset("digraph { x -> y }", ["x", "y"], [("x", "y")])
        resp = _get(server, "/api/state")
        data = json.loads(resp.read())
        assert data["is_running"] is True
        assert set(data["dag_nodes"]) == {"x", "y"}
        assert len(data["nodes"]) == 2


class TestAPIProfile:
    def test_profile_204_before_execution(self, server):
        resp = _get(server, "/api/profile")
        assert resp.status == 204

    def test_profile_200_after_execution(self, server):
        server.execution_finished(0.5, 1, 0)
        resp = _get(server, "/api/profile")
        assert resp.status == 200
        data = json.loads(resp.read())
        assert data["succeeded"] == 1


class TestGateRoutes:
    def test_approve_unknown_gate_returns_404(self, server):
        resp = _post(server, "/api/gates/nope/approve")
        assert resp.status == 404

    def test_reject_unknown_gate_returns_404(self, server):
        resp = _post(server, "/api/gates/nope/reject")
        assert resp.status == 404

    def test_approve_existing_gate(self, server):
        from dagron.execution.gates import ApprovalGate, GateController

        gc = GateController({"deploy": ApprovalGate()})
        server.set_gate_callback(gc.approve, gc.reject, gc.has_gate)

        resp = _post(server, "/api/gates/deploy/approve")
        assert resp.status == 200
        data = json.loads(resp.read())
        assert data["ok"] is True

    def test_reject_existing_gate_with_reason(self, server):
        from dagron.execution.gates import ApprovalGate, GateController

        gc = GateController({"deploy": ApprovalGate()})
        server.set_gate_callback(gc.approve, gc.reject, gc.has_gate)

        resp = _post(server, "/api/gates/deploy/reject", {"reason": "not ready"})
        assert resp.status == 200
        data = json.loads(resp.read())
        assert data["ok"] is True
