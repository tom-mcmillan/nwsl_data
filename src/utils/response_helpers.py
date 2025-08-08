#!/usr/bin/env python3
"""
Response Utilities
==================

Shared utilities for MCP server responses and error handling.
"""

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def safe_json_response(data: Any) -> str:
    """Safely convert data to JSON string with error handling"""
    try:
        return json.dumps(data, indent=2, default=str)
    except Exception as e:
        logger.error(f"JSON serialization failed: {e}")
        return json.dumps({"error": f"JSON serialization failed: {str(e)}"})
