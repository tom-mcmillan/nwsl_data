FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code and database
COPY mcp_server.py .
COPY data/processed/nwsldata.db ./data/processed/

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app
USER app

# Expose port
EXPOSE 8000

# Health check - check if FastMCP streamable HTTP server is responding
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.post('http://localhost:8000/mcp', json={'jsonrpc': '2.0', 'method': 'initialize', 'id': 1}, timeout=10)" || exit 1

# Run the server with proper Cloud Run configuration
CMD ["python", "mcp_server.py"]