#!/usr/bin/env python3
"""
NWSL MCP Server - HTTP Version
Complete NWSL analytics server with FastMCP HTTP for Cloud Run deployment
Converted from stdio to HTTP while preserving all superior tools and database integration
"""

import json
import logging
import os
from pathlib import Path
from typing import Any, Optional
import sys

# Add project root to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Configure logging for debugging startup issues (FIRST!)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("nwsl-mcp-server")

from mcp.server.fastmcp import FastMCP
import uvicorn

# Import our database tools
logger.info("🟡 Importing database tools...")
try:
    from src.analyzers.database_context_tool import DatabaseContextTool
    logger.info("✅ DatabaseContextTool imported")
    
    from src.analyzers.team_performance_analyzer import TeamPerformanceAnalyzer
    logger.info("✅ TeamPerformanceAnalyzer imported")
    
    from src.analyzers.enhanced_match_analyzer import MatchAnalyzer
    logger.info("✅ MatchAnalyzer imported")
    
    from src.analyzers.player_stats_analyzer import PlayerStatsAnalyzer
    logger.info("✅ PlayerStatsAnalyzer imported")
    
except ImportError as e:
    logger.error(f"❌ Failed to import analyzer: {e}")
    raise

logger.info("🟡 Starting server initialization...")
logger.info("🟡 Python path setup complete")

# Database configuration
DB_PATH = Path(__file__).parent / "data" / "processed" / "nwsldata.db"
logger.info(f"🟡 Database path configured: {DB_PATH}")
logger.info(f"🟡 Database exists: {DB_PATH.exists()}")

# Initialize tools with correct database path
logger.info("🟡 Initializing database context tool...")
db_context = DatabaseContextTool(str(DB_PATH))
logger.info("✅ Database context tool initialized")

logger.info("🟡 Initializing team performance analyzer...")
team_analyzer = TeamPerformanceAnalyzer(str(DB_PATH))
logger.info("✅ Team performance analyzer initialized")

logger.info("🟡 Initializing match analyzer...")
match_analyzer = MatchAnalyzer(str(DB_PATH))
logger.info("✅ Match analyzer initialized")

logger.info("🟡 Initializing player stats analyzer...")
player_analyzer = PlayerStatsAnalyzer(str(DB_PATH))
logger.info("✅ Player stats analyzer initialized")

def safe_json_response(data: Any) -> str:
    """Safely convert data to JSON string"""
    try:
        return json.dumps(data, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": f"JSON serialization failed: {str(e)}"})

# Initialize FastMCP server
logger.info("🟡 Initializing FastMCP server...")
mcp = FastMCP("NWSL Analytics Server")
logger.info("✅ FastMCP server initialized")

@mcp.tool()
def get_database_overview() -> str:
    """Get comprehensive overview of NWSL database (seasons 2013-2025, teams, matches, data quality).
    
    Returns:
        Comprehensive database overview with season coverage, data quality metrics
    """
    try:
        result = db_context.get_database_overview()
        return safe_json_response(result)
    except Exception as e:
        logger.error(f"Error in get_database_overview: {e}")
        return safe_json_response({"error": f"Database overview failed: {str(e)}"})

@mcp.tool()
def search_team_names(search_term: str) -> str:
    """Search for NWSL teams by name (handles partial matches like 'Courage' -> 'North Carolina Courage').
    
    Args:
        search_term: Team name or partial name to search for
        
    Returns:
        List of matching teams with variations and aliases
    """
    try:
        result = db_context.search_team_names(search_term)
        return safe_json_response(result)
    except Exception as e:
        logger.error(f"Error in search_team_names: {e}")
        return safe_json_response({"error": f"Team search failed: {str(e)}"})

@mcp.tool()
def get_season_summary(season_id: int) -> str:
    """Get comprehensive season summary (matches, teams, goals, dates, top performers).
    
    Args:
        season_id: Season year (2013-2025)
        
    Returns:
        Complete season overview with key statistics and context
    """
    try:
        result = db_context.get_season_summary(season_id)
        return safe_json_response(result)
    except Exception as e:
        logger.error(f"Error in get_season_summary: {e}")
        return safe_json_response({"error": f"Season summary failed: {str(e)}"})

@mcp.tool()
def analyze_team_season(team_name: str, season_id: int) -> str:
    """Analyze team's complete season performance (record, stats, best matches).
    
    Args:
        team_name: Team name (full or partial)
        season_id: Season year
        
    Returns:
        Comprehensive team season analysis with performance metrics
    """
    try:
        result = team_analyzer.analyze_team_season(team_name, season_id)
        return safe_json_response(result)
    except Exception as e:
        logger.error(f"Error in analyze_team_season: {e}")
        return safe_json_response({"error": f"Team season analysis failed: {str(e)}"})

@mcp.tool()
def compare_teams(team1_name: str, team2_name: str, season_id: int) -> str:
    """Compare two teams' performance in a season side-by-side.
    
    Args:
        team1_name: First team name
        team2_name: Second team name
        season_id: Season year
        
    Returns:
        Detailed side-by-side team comparison with key metrics
    """
    try:
        result = team_analyzer.compare_teams(team1_name, team2_name, season_id)
        return safe_json_response(result)
    except Exception as e:
        logger.error(f"Error in compare_teams: {e}")
        return safe_json_response({"error": f"Team comparison failed: {str(e)}"})

@mcp.tool()
def get_head_to_head_record(team1_name: str, team2_name: str) -> str:
    """Get historical head-to-head record between two teams across all seasons.
    
    Args:
        team1_name: First team name
        team2_name: Second team name
        
    Returns:
        Complete historical record between the two teams
    """
    try:
        result = team_analyzer.get_head_to_head_record(team1_name, team2_name)
        return safe_json_response(result)
    except Exception as e:
        logger.error(f"Error in get_head_to_head_record: {e}")
        return safe_json_response({"error": f"Head-to-head analysis failed: {str(e)}"})

@mcp.tool()
def analyze_match(match_id: str) -> str:
    """Deep analysis of specific match (team stats, goalscorers, top performers).
    
    Args:
        match_id: Unique match identifier
        
    Returns:
        Comprehensive match analysis with detailed statistics
    """
    try:
        result = match_analyzer.analyze_match(match_id)
        return safe_json_response(result)
    except Exception as e:
        logger.error(f"Error in analyze_match: {e}")
        return safe_json_response({"error": f"Match analysis failed: {str(e)}"})

@mcp.tool()
def get_player_season_stats(player_name: str, season_id: int) -> str:
    """Get player's season statistics (goals, assists, matches, best performances).
    
    Args:
        player_name: Full or partial player name
        season_id: Season year
        
    Returns:
        Complete player season statistics and performance metrics
    """
    try:
        result = player_analyzer.get_player_season_stats(player_name, season_id)
        return safe_json_response(result)
    except Exception as e:
        logger.error(f"Error in get_player_season_stats: {e}")
        return safe_json_response({"error": f"Player stats analysis failed: {str(e)}"})

@mcp.tool()
def get_season_top_scorers(season_id: int, limit: int = 10) -> str:
    """Get top goal scorers for a season with statistics.
    
    Args:
        season_id: Season year
        limit: Number of players to return (default: 10)
        
    Returns:
        Top goal scorers with detailed statistics
    """
    try:
        result = player_analyzer.get_season_top_scorers(season_id, limit)
        return safe_json_response(result)
    except Exception as e:
        logger.error(f"Error in get_season_top_scorers: {e}")
        return safe_json_response({"error": f"Top scorers analysis failed: {str(e)}"})

@mcp.tool()
def validate_query(team_name: Optional[str] = None, season_id: Optional[int] = None) -> str:
    """Validate if team/season exists before running analysis (use when queries might fail).
    
    Args:
        team_name: Team name to validate (optional)
        season_id: Season to validate (optional)
        
    Returns:
        Validation results and suggestions for valid queries
    """
    try:
        result = db_context.validate_query(team_name, season_id)
        return safe_json_response(result)
    except Exception as e:
        logger.error(f"Error in validate_query: {e}")
        return safe_json_response({"error": f"Query validation failed: {str(e)}"})

if __name__ == "__main__":
    logger.info("🟡 Entering main execution block...")
    
    # Run as HTTP server for remote MCP access (Cloud Run deployment)
    port = int(os.environ.get("PORT", 8000))
    logger.info(f"🟡 Port configured: {port}")
    
    try:
        logger.info("🟡 Starting database verification...")
        
        # Verify database exists
        if not DB_PATH.exists():
            logger.error(f"❌ Database not found at {DB_PATH}")
            raise FileNotFoundError(f"Database not found at {DB_PATH}")
        
        logger.info("✅ Database file exists")
        
        # Test database connection
        logger.info("🟡 Testing database connection...")
        try:
            overview = db_context.get_database_overview()
            logger.info(f"✅ Database connected: {overview.get('total_seasons', 'unknown')} seasons, {overview.get('total_matches', 'unknown')} matches")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise
        
        logger.info(f"🟡 Creating FastMCP app...")
        # Get FastMCP streamable HTTP app for Cloud Run deployment
        app = mcp.streamable_http_app()
        logger.info("✅ FastMCP app created")
        
        logger.info(f"🚀 Starting NWSL MCP Server on port {port}")
        
        # Run with uvicorn for Cloud Run
        uvicorn.run(
            app,
            host="0.0.0.0",  # Required for Cloud Run
            port=port,
            log_level="info"  # Show startup logs for debugging
        )
        
    except Exception as e:
        logger.error(f"❌ Failed to start NWSL MCP Server: {e}")
        logger.exception("Full traceback:")
        raise