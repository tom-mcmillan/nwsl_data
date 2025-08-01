#!/usr/bin/env python3
"""
Complete NWSL MCP Server Implementation
Fixes the issues: hardcoded 2020, missing Courage data, fake statistics
"""

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

from mcp.server.models import InitializationOptions
from mcp.server import NotificationOptions, Server
from mcp.types import (
    CallToolRequest,
    CallToolResult,
    ListToolsRequest,
    ListToolsResult,
    Tool,
    TextContent,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import our database tools
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scripts.database_context_tool import DatabaseContextTool
from scripts.team_performance_analyzer import TeamPerformanceAnalyzer
from scripts.enhanced_match_analyzer import MatchAnalyzer
from scripts.player_stats_analyzer import PlayerStatsAnalyzer

# Initialize tools with correct database path
DB_PATH = "data/processed/nwsldata.db"
db_context = DatabaseContextTool(DB_PATH)
team_analyzer = TeamPerformanceAnalyzer(DB_PATH)
match_analyzer = MatchAnalyzer(DB_PATH)
player_analyzer = PlayerStatsAnalyzer(DB_PATH)

# Create server
server = Server("nwsl-analytics-server")

def safe_json_response(data: Any) -> str:
    """Safely convert data to JSON string"""
    try:
        return json.dumps(data, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": f"JSON serialization failed: {str(e)}"})

@server.list_tools()
async def handle_list_tools() -> list[Tool]:
    """List all available NWSL database tools"""
    return [
        Tool(
            name="get_database_overview",
            description="Get comprehensive overview of NWSL database (seasons 2013-2025, teams, matches, data quality)",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
        Tool(
            name="search_teams",
            description="Search for NWSL teams by name (handles partial matches like 'Courage' -> 'North Carolina Courage')",
            inputSchema={
                "type": "object",
                "properties": {
                    "search_term": {"type": "string", "description": "Team name or partial name"}
                },
                "required": ["search_term"]
            }
        ),
        Tool(
            name="get_season_summary", 
            description="Get comprehensive season summary (matches, teams, goals, dates, top performers)",
            inputSchema={
                "type": "object",
                "properties": {
                    "season_id": {"type": "integer", "description": "Season year (2013-2025)"}
                },
                "required": ["season_id"]
            }
        ),
        Tool(
            name="analyze_team_season",
            description="Analyze team's complete season performance (record, stats, best matches)",
            inputSchema={
                "type": "object", 
                "properties": {
                    "team_name": {"type": "string", "description": "Team name (full or partial)"},
                    "season_id": {"type": "integer", "description": "Season year"}
                },
                "required": ["team_name", "season_id"]
            }
        ),
        Tool(
            name="compare_teams",
            description="Compare two teams' performance in a season side-by-side",
            inputSchema={
                "type": "object",
                "properties": {
                    "team1_name": {"type": "string", "description": "First team name"},
                    "team2_name": {"type": "string", "description": "Second team name"},  
                    "season_id": {"type": "integer", "description": "Season year"}
                },
                "required": ["team1_name", "team2_name", "season_id"]
            }
        ),
        Tool(
            name="get_head_to_head_record",
            description="Get historical head-to-head record between two teams across all seasons",
            inputSchema={
                "type": "object",
                "properties": {
                    "team1_name": {"type": "string", "description": "First team name"},
                    "team2_name": {"type": "string", "description": "Second team name"}
                },
                "required": ["team1_name", "team2_name"]
            }
        ),
        Tool(
            name="analyze_match",
            description="Deep analysis of specific match (team stats, goalscorers, top performers)",
            inputSchema={
                "type": "object",
                "properties": {
                    "match_id": {"type": "string", "description": "Match ID"}
                },
                "required": ["match_id"]
            }
        ),
        Tool(
            name="get_player_season_stats",
            description="Get player's season statistics (goals, assists, matches, best performances)",
            inputSchema={
                "type": "object",
                "properties": {
                    "player_name": {"type": "string", "description": "Player name"},
                    "season_id": {"type": "integer", "description": "Season year"}
                },
                "required": ["player_name", "season_id"]
            }
        ),
        Tool(
            name="get_season_top_scorers",
            description="Get top goal scorers for a season with statistics",
            inputSchema={
                "type": "object",
                "properties": {
                    "season_id": {"type": "integer", "description": "Season year"},
                    "limit": {"type": "integer", "description": "Number of players to return", "default": 10}
                },
                "required": ["season_id"]
            }
        ),
        Tool(
            name="validate_query",
            description="Validate if team/season exists before running analysis (use when queries might fail)",
            inputSchema={
                "type": "object",
                "properties": {
                    "team_name": {"type": "string", "description": "Team name to validate"},
                    "season_id": {"type": "integer", "description": "Season to validate"}
                },
                "required": []
            }
        )
    ]

@server.call_tool()
async def handle_call_tool(request: CallToolRequest) -> CallToolResult:
    """Handle all tool calls with proper error handling"""
    
    try:
        tool_name = request.params.name
        args = request.params.arguments or {}
        
        logger.info(f"Calling tool: {tool_name} with args: {args}")
        
        # Route to appropriate tool function
        if tool_name == "get_database_overview":
            result = db_context.get_database_overview()
            
        elif tool_name == "search_teams":
            result = db_context.search_team_names(args["search_term"])
            
        elif tool_name == "get_season_summary":
            result = db_context.get_season_summary(args["season_id"])
            
        elif tool_name == "analyze_team_season":
            result = team_analyzer.get_team_season_summary(args["team_name"], args["season_id"])
            
        elif tool_name == "compare_teams":
            result = team_analyzer.compare_teams_in_season(
                args["team1_name"], args["team2_name"], args["season_id"]
            )
            
        elif tool_name == "get_head_to_head_record":
            result = team_analyzer.get_head_to_head_record(args["team1_name"], args["team2_name"])
            
        elif tool_name == "analyze_match":
            result = match_analyzer.analyze_match_completely(args["match_id"])
            
        elif tool_name == "get_player_season_stats":
            result = player_analyzer.get_player_season_stats(args["player_name"], args["season_id"])
            
        elif tool_name == "get_season_top_scorers":
            limit = args.get("limit", 10)
            result = player_analyzer.get_top_scorers_in_season(args["season_id"], limit)
            
        elif tool_name == "validate_query":
            team_name = args.get("team_name")
            season_id = args.get("season_id") 
            result = db_context.validate_user_query(team_name, season_id)
            
        else:
            result = {"error": f"Unknown tool: {tool_name}"}
        
        # Convert result to JSON string
        json_result = safe_json_response(result)
        logger.info(f"Tool {tool_name} completed successfully")
        
        return CallToolResult(
            content=[TextContent(type="text", text=json_result)]
        )
        
    except Exception as e:
        error_msg = f"Tool execution failed: {str(e)}"
        logger.error(error_msg)
        return CallToolResult(
            content=[TextContent(type="text", text=safe_json_response({"error": error_msg}))]
        )

async def main():
    """Run the NWSL MCP server"""
    logger.info("🚀 Starting NWSL Analytics MCP Server...")
    
    # Verify database exists
    if not Path(DB_PATH).exists():
        logger.error(f"❌ Database not found at {DB_PATH}")
        return
    
    # Test database connection
    try:
        overview = db_context.get_database_overview()
        logger.info(f"✅ Database connected: {overview['total_seasons']} seasons, {overview['total_matches']} matches")
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return
    
    # Run server
    async with server.run_stdio() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="nwsl-analytics-server",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())