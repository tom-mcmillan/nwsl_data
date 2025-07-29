# NWSL MCP Server - Deployment & OpenAI Playground Setup

## 🚀 Quick Test (Local)

The FastMCP server is working! Test it locally first:

```bash
# 1. Install dependencies
pip install fastapi uvicorn fastmcp pydantic

# 2. Run the server
python mcp_server.py

# 3. Test endpoints
curl http://localhost:8000/
```

## 🌐 Deploy to Google Cloud Platform

### Option 1: Google App Engine (Recommended)

```bash
# 1. Install Google Cloud CLI
# Download from: https://cloud.google.com/sdk/docs/install

# 2. Initialize and authenticate
gcloud init
gcloud auth login

# 3. Create a new project (or use existing)
gcloud projects create nwsl-mcp-server-123
gcloud config set project nwsl-mcp-server-123

# 4. Enable App Engine
gcloud app create --region=us-central1

# 5. Deploy the application
gcloud app deploy app.yaml

# 6. Get your deployment URL
gcloud app browse
```

Your server will be available at: `https://nwsl-mcp-server-123.uc.r.appspot.com`

### Option 2: Google Cloud Run (Alternative)

```bash
# 1. Build and push Docker image
gcloud builds submit --tag gcr.io/nwsl-mcp-server-123/nwsl-mcp

# 2. Deploy to Cloud Run
gcloud run deploy nwsl-mcp-server \
  --image gcr.io/nwsl-mcp-server-123/nwsl-mcp \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --port 8000

# 3. Get service URL
gcloud run services describe nwsl-mcp-server \
  --platform managed \
  --region us-central1 \
  --format 'value(status.url)'
```

## 🤖 OpenAI Playground Setup

Once deployed, add your server to OpenAI Playground:

### Step 1: Get Your Server URL
After deployment, you'll have a URL like:
- App Engine: `https://nwsl-mcp-server-123.uc.r.appspot.com`
- Cloud Run: `https://nwsl-mcp-server-xyz.run.app`

### Step 2: Configure in OpenAI Playground

In the OpenAI Playground MCP connection form, enter:

**URL:** `https://your-deployed-server.com/mcp`
**Label:** `NWSL Data Server`
**Description:** `Query NWSL database (2013-2025) for player stats, team performance, and match results`
**Authentication:** Leave as "Access token / API key" but leave token field empty (no auth required)

### Step 3: Test the Connection

Click "Connect" and test with these example queries:

```
Search for players named Morgan
```

```
Show me Portland Thorns performance in 2024
```

```
Get the 2024 NWSL season overview
```

## 🛠 Available Tools

Your deployed server provides these 6 MCP tools:

1. **`search_players`** - Find players by name
2. **`query_player_stats`** - Get detailed player statistics
3. **`query_team_performance`** - Team performance across seasons
4. **`query_match_results`** - Match results and head-to-head data
5. **`get_league_standings`** - Season standings by expected goals
6. **`get_season_overview`** - High-level season statistics

## 🔧 Troubleshooting

### Server Issues
- Check logs: `gcloud app logs tail -s default`
- Verify database file is included in deployment
- Test locally first: `python mcp_fastapi_server.py`

### OpenAI Playground Issues
- Verify URL includes `/mcp` path
- Check server is publicly accessible
- Test with curl: `curl https://your-server.com/mcp`

### Database Issues
- Ensure `nwsldata.db` is in `data/processed/` directory
- Check file permissions and size
- Verify SQLite queries work locally

## 💰 Cost Estimates

**Google App Engine:**
- ~$0.05/hour for F1 instance (minimal usage)
- Free tier: 28 instance hours/day

**Google Cloud Run:**
- ~$0.000024/request + $0.000009/GB-second
- Free tier: 2 million requests/month

For light usage, both should stay within free tiers.

## 🔄 Updates

To update your deployment:

```bash
# Make changes to code
# Then redeploy
gcloud app deploy app.yaml
```

## 📊 Example Usage

Once deployed, users can ask questions like:

- "Who scored the most goals in 2024?"
- "How did Angel City FC perform in their first season?"
- "Show me head-to-head results between Portland and Seattle"
- "What were the top teams by expected goals in 2023?"
- "Find all players with 'Smith' in their name"

Your NWSL MCP server is now ready for production use! 🏆