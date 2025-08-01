#!/bin/bash
"""
Start NWSL MCP Server
Use this to replace your existing server
"""

# Navigate to project directory
cd "$(dirname "$0")"

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
    echo "✅ Virtual environment activated"
fi

# Check database exists
if [ ! -f "data/processed/nwsldata.db" ]; then
    echo "❌ Database not found at data/processed/nwsldata.db"
    exit 1
fi

# Install dependencies if needed
python3 -c "import mcp" 2>/dev/null || {
    echo "📦 Installing MCP dependencies..."
    pip install mcp pandas
}

# Start the server
echo "🚀 Starting NWSL MCP Server..."
python3 nwsl_mcp_server.py