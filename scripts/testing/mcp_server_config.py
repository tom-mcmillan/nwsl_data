# Minimal shim used by tests
from __future__ import annotations

try:
    from src.server import mcp  # the FastMCP instance
except Exception:
    mcp = None  # tests may only import; handle absence gracefully


def create_mcp_server():
    """Return the MCP server instance (or None if unavailable)."""
    return mcp


def handle_tool_call(*_args, **_kwargs):
    """No-op placeholder used by tests."""
    return None
