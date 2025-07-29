#!/usr/bin/env python3
"""
NWSL Data MCP Server
Official MCP server implementation following FastMCP best practices
Provides NWSL data access for players, coaches, and fans (2013-2025)
"""

import sqlite3
from typing import Optional
from pathlib import Path
import logging
from contextlib import contextmanager

from mcp.server.fastmcp import FastMCP
import uvicorn

# Configure logging for MCP (stderr only, no stdout)
logging.basicConfig(level=logging.ERROR, format='%(levelname)s: %(message)s')
logger = logging.getLogger("nwsl-mcp-server")

# Database configuration
DB_PATH = Path(__file__).parent / "data" / "processed" / "nwsldata.db"

@contextmanager
def get_db_connection():
    """Get database connection with proper cleanup and error handling."""
    conn = None
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row  # Enable column access by name
        yield conn
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()

# Initialize FastMCP server
mcp = FastMCP("NWSL Data Server")

@mcp.tool()
def search_players(search_term: str, limit: int = 10) -> str:
    """Search for NWSL players by name or partial name.
    
    Args:
        search_term: Player name or partial name to search for
        limit: Number of results to return (default: 10)
    
    Returns:
        Formatted string with player names, seasons, and teams
    """
    try:
        with get_db_connection() as conn:
            query = """
            SELECT DISTINCT p.player_name, 
                   GROUP_CONCAT(DISTINCT ps.season_id ORDER BY ps.season_id) as seasons,
                   GROUP_CONCAT(DISTINCT ps.squad ORDER BY ps.squad) as teams
            FROM player p
            LEFT JOIN player_season ps ON p.player_id = ps.player_id
            WHERE p.player_name LIKE ? COLLATE NOCASE
            GROUP BY p.player_id, p.player_name
            ORDER BY p.player_name
            LIMIT ?
            """
            
            cursor = conn.execute(query, (f"%{search_term}%", limit))
            results = cursor.fetchall()
            
            if not results:
                return f"No players found matching '{search_term}'"
            
            response = f"Players matching '{search_term}':\n\n"
            for row in results:
                response += f"• {row['player_name']}\n"
                if row['seasons']:
                    response += f"  Seasons: {row['seasons']}\n"
                if row['teams']:
                    response += f"  Teams: {row['teams']}\n"
                response += "\n"
            
            return response
            
    except Exception as e:
        logger.error(f"Error in search_players: {e}")
        return f"Error searching for players: {str(e)}"

@mcp.tool()
def get_player_stats(player_name: str, season: Optional[int] = None) -> str:
    """Get detailed statistics for a specific NWSL player.
    
    Args:
        player_name: Name of the player to query
        season: Optional season year (e.g., 2024). If not provided, returns all seasons
    
    Returns:
        Formatted string with player statistics by season
    """
    try:
        with get_db_connection() as conn:
            if season:
                query = """
                SELECT DISTINCT p.player_name, ps.season_id, ps.squad,
                       ps.position, ps.mp, ps.goals, ps.assists, ps.minutes
                FROM player p
                JOIN player_season ps ON p.player_id = ps.player_id
                WHERE p.player_name LIKE ? COLLATE NOCASE AND ps.season_id = ?
                ORDER BY ps.season_id DESC
                """
                cursor = conn.execute(query, (f"%{player_name}%", season))
            else:
                query = """
                SELECT DISTINCT p.player_name, ps.season_id, ps.squad,
                       ps.position, ps.mp, ps.goals, ps.assists, ps.minutes
                FROM player p
                JOIN player_season ps ON p.player_id = ps.player_id
                WHERE p.player_name LIKE ? COLLATE NOCASE
                ORDER BY ps.season_id DESC
                LIMIT 20
                """
                cursor = conn.execute(query, (f"%{player_name}%",))
            
            results = cursor.fetchall()
            
            if not results:
                return f"No player found matching '{player_name}'"
            
            response = f"Player Statistics for '{player_name}':\n\n"
            for row in results:
                response += f"Season {row['season_id']} - {row['squad'] or 'Unknown Team'}\n"
                response += f"  Position: {row['position'] or 'Unknown'}\n"
                response += f"  Games: {row['mp'] or 0}, Goals: {row['goals'] or 0}, Assists: {row['assists'] or 0}\n"
                if row['minutes']:
                    response += f"  Minutes: {row['minutes']}\n"
                response += "\n"
            
            return response
            
    except Exception as e:
        logger.error(f"Error in get_player_stats: {e}")
        return f"Error getting player stats: {str(e)}"

@mcp.tool()
def get_team_performance(team_name: str, season: Optional[int] = None) -> str:
    """Get team performance data and expected goals statistics.
    
    Args:
        team_name: Name of the team to query (e.g., "Portland Thorns", "Angel City")
        season: Optional season year. If not provided, returns last 5 seasons
    
    Returns:
        Formatted string with team performance metrics
    """
    try:
        with get_db_connection() as conn:
            # Find team by name (case insensitive)
            team_query = """
            SELECT team_id, team_name, team_name_short
            FROM team
            WHERE team_name LIKE ? COLLATE NOCASE 
               OR team_name_short LIKE ? COLLATE NOCASE
            """
            cursor = conn.execute(team_query, (f"%{team_name}%", f"%{team_name}%"))
            team_row = cursor.fetchone()
            
            if not team_row:
                return f"No team found matching '{team_name}'"
            
            team_id = team_row['team_id']
            team_full_name = team_row['team_name']
            
            # Get performance data
            if season:
                match_query = """
                SELECT COUNT(*) as games_played,
                       SUM(CASE WHEN (home_team_id = ? AND xg_home > xg_away) 
                                  OR (away_team_id = ? AND xg_away > xg_home) 
                                THEN 1 ELSE 0 END) as wins,
                       AVG(CASE WHEN home_team_id = ? THEN xg_home 
                               WHEN away_team_id = ? THEN xg_away END) as avg_xg,
                       AVG(CASE WHEN home_team_id = ? THEN xg_away 
                               WHEN away_team_id = ? THEN xg_home END) as avg_xga
                FROM match
                WHERE (home_team_id = ? OR away_team_id = ?) 
                  AND season_id = ?
                  AND match_type_name = 'Regular Season'
                """
                cursor = conn.execute(match_query, 
                    (team_id, team_id, team_id, team_id, team_id, team_id, team_id, team_id, season))
            else:
                match_query = """
                SELECT season_id,
                       COUNT(*) as games_played,
                       SUM(CASE WHEN (home_team_id = ? AND xg_home > xg_away) 
                                  OR (away_team_id = ? AND xg_away > xg_home) 
                                THEN 1 ELSE 0 END) as wins,
                       AVG(CASE WHEN home_team_id = ? THEN xg_home 
                               WHEN away_team_id = ? THEN xg_away END) as avg_xg,
                       AVG(CASE WHEN home_team_id = ? THEN xg_away 
                               WHEN away_team_id = ? THEN xg_home END) as avg_xga
                FROM match
                WHERE (home_team_id = ? OR away_team_id = ?)
                  AND match_type_name = 'Regular Season'
                GROUP BY season_id
                ORDER BY season_id DESC
                LIMIT 5
                """
                cursor = conn.execute(match_query, 
                    (team_id, team_id, team_id, team_id, team_id, team_id, team_id, team_id))
            
            results = cursor.fetchall()
            
            response = f"Team Performance: {team_full_name}\n\n"
            
            if season:
                if results and results[0]['games_played'] > 0:
                    row = results[0]
                    win_pct = (row['wins'] / row['games_played']) * 100 if row['games_played'] > 0 else 0
                    response += f"Season {season}:\n"
                    response += f"  Games Played: {row['games_played']}\n"
                    response += f"  Wins: {row['wins']} ({win_pct:.1f}%)\n"
                    response += f"  Average xG: {row['avg_xg']:.2f}\n"
                    response += f"  Average xGA: {row['avg_xga']:.2f}\n"
                    response += f"  xG Differential: {(row['avg_xg'] - row['avg_xga']):.2f}\n"
                else:
                    response += f"No data found for {team_full_name} in {season} season."
            else:
                if results:
                    for row in results:
                        win_pct = (row['wins'] / row['games_played']) * 100 if row['games_played'] > 0 else 0
                        xg_diff = (row['avg_xg'] - row['avg_xga']) if row['avg_xg'] and row['avg_xga'] else 0
                        response += f"Season {row['season_id']}:\n"
                        response += f"  Games: {row['games_played']}, Wins: {row['wins']} ({win_pct:.1f}%)\n"
                        response += f"  Avg xG: {row['avg_xg']:.2f}, Avg xGA: {row['avg_xga']:.2f} (Diff: {xg_diff:+.2f})\n\n"
                else:
                    response += f"No performance data found for {team_full_name}."
            
            return response
            
    except Exception as e:
        logger.error(f"Error in get_team_performance: {e}")
        return f"Error getting team performance: {str(e)}"

@mcp.tool()
def get_league_standings(season: int, match_type: str = "Regular Season") -> str:
    """Get NWSL league standings ranked by expected goals differential.
    
    Args:
        season: Season year (e.g., 2024)
        match_type: Match type filter (default: "Regular Season")
    
    Returns:
        Formatted string with team rankings by xG differential
    """
    try:
        with get_db_connection() as conn:
            query = """
            SELECT team_name,
                   SUM(games) as total_games,
                   SUM(xg_for) as total_xg,
                   SUM(xg_against) as total_xg_against,
                   SUM(wins) as total_wins
            FROM (
                SELECT home_team_name as team_name, 
                       COUNT(*) as games,
                       SUM(xg_home) as xg_for,
                       SUM(xg_away) as xg_against,
                       SUM(CASE WHEN xg_home > xg_away THEN 1 ELSE 0 END) as wins
                FROM match 
                WHERE season_id = ? AND match_type_name = ?
                  AND xg_home IS NOT NULL AND xg_away IS NOT NULL
                GROUP BY home_team_name
                
                UNION ALL
                
                SELECT away_team_name as team_name,
                       COUNT(*) as games,
                       SUM(xg_away) as xg_for,
                       SUM(xg_home) as xg_against,
                       SUM(CASE WHEN xg_away > xg_home THEN 1 ELSE 0 END) as wins
                FROM match 
                WHERE season_id = ? AND match_type_name = ?
                  AND xg_home IS NOT NULL AND xg_away IS NOT NULL
                GROUP BY away_team_name
            )
            GROUP BY team_name
            ORDER BY (total_xg - total_xg_against) DESC
            """
            
            cursor = conn.execute(query, (season, match_type, season, match_type))
            results = cursor.fetchall()
            
            if not results:
                return f"No standings data found for {season} season"
            
            response = f"{season} NWSL {match_type} - Team Rankings (by xG differential):\n\n"
            for i, row in enumerate(results, 1):
                xg_diff = row['total_xg'] - row['total_xg_against']
                win_pct = (row['total_wins'] / row['total_games']) * 100 if row['total_games'] > 0 else 0
                response += f"{i:2d}. {row['team_name']}\n"
                response += f"     Games: {row['total_games']}, Wins: {row['total_wins']} ({win_pct:.1f}%)\n"
                response += f"     xG: {row['total_xg']:.1f}, xGA: {row['total_xg_against']:.1f} (Diff: {xg_diff:+.1f})\n\n"
            
            return response
            
    except Exception as e:
        logger.error(f"Error in get_league_standings: {e}")
        return f"Error getting league standings: {str(e)}"

@mcp.tool()
def get_season_overview(season: int) -> str:
    """Get comprehensive overview statistics for a specific NWSL season.
    
    Args:
        season: Season year (e.g., 2024)
    
    Returns:
        Formatted string with season overview statistics
    """
    try:
        with get_db_connection() as conn:
            overview_query = """
            SELECT 
                COUNT(DISTINCT CASE WHEN match_type_name = 'Regular Season' THEN match_id END) as regular_season_games,
                COUNT(DISTINCT CASE WHEN match_type_name != 'Regular Season' THEN match_id END) as other_games,
                COUNT(DISTINCT home_team_name) as teams,
                AVG(CASE WHEN xg_home IS NOT NULL AND xg_away IS NOT NULL 
                         THEN xg_home + xg_away END) as avg_total_xg,
                AVG(CASE WHEN home_goals IS NOT NULL AND away_goals IS NOT NULL 
                         THEN home_goals + away_goals END) as avg_total_goals
            FROM match
            WHERE season_id = ?
            """
            
            cursor = conn.execute(overview_query, (season,))
            overview = cursor.fetchone()
            
            if not overview or overview['regular_season_games'] == 0:
                return f"No data found for {season} season"
            
            response = f"{season} NWSL Season Overview:\n\n"
            response += f"• Regular Season Games: {overview['regular_season_games']}\n"
            response += f"• Other Games (Playoffs/Cups): {overview['other_games']}\n"  
            response += f"• Number of Teams: {overview['teams']}\n"
            if overview['avg_total_goals']:
                response += f"• Average Goals per Game: {overview['avg_total_goals']:.2f}\n"
            if overview['avg_total_xg']:
                response += f"• Average Expected Goals per Game: {overview['avg_total_xg']:.2f}\n"
            
            return response
            
    except Exception as e:
        logger.error(f"Error in get_season_overview: {e}")
        return f"Error getting season overview: {str(e)}"

@mcp.tool()
def get_match_results(team1: str, team2: Optional[str] = None, season: Optional[int] = None, limit: int = 10) -> str:
    """Get detailed match information and results.
    
    Args:
        team1: First team name (required)
        team2: Second team name for head-to-head matchups (optional)
        season: Season year filter (optional)
        limit: Number of matches to return (default: 10)
    
    Returns:
        Formatted string with match results and details
    """
    try:
        with get_db_connection() as conn:
            if team2:
                # Head-to-head matches
                query = """
                SELECT m.match_date, m.home_team_name, m.away_team_name,
                       m.home_goals, m.away_goals, m.xg_home, m.xg_away, 
                       m.season_id, m.match_type_name, m.venue
                FROM match m
                WHERE ((m.home_team_name LIKE ? COLLATE NOCASE AND m.away_team_name LIKE ? COLLATE NOCASE)
                    OR (m.home_team_name LIKE ? COLLATE NOCASE AND m.away_team_name LIKE ? COLLATE NOCASE))
                """
                params = [f"%{team1}%", f"%{team2}%", f"%{team2}%", f"%{team1}%"]
                
                if season:
                    query += " AND m.season_id = ?"
                    params.append(season)
                
                query += " ORDER BY m.match_date DESC LIMIT ?"
                params.append(limit)
            else:
                # All matches for one team
                query = """
                SELECT m.match_date, m.home_team_name, m.away_team_name,
                       m.home_goals, m.away_goals, m.xg_home, m.xg_away,
                       m.season_id, m.match_type_name, m.venue
                FROM match m
                WHERE (m.home_team_name LIKE ? COLLATE NOCASE OR m.away_team_name LIKE ? COLLATE NOCASE)
                """
                params = [f"%{team1}%", f"%{team1}%"]
                
                if season:
                    query += " AND m.season_id = ?"
                    params.append(season)
                
                query += " ORDER BY m.match_date DESC LIMIT ?"
                params.append(limit)
            
            cursor = conn.execute(query, params)
            results = cursor.fetchall()
            
            if not results:
                return f"No matches found for the specified criteria"
            
            response = f"Match Results:\n\n"
            for row in results:
                response += f"{row['match_date']} - {row['match_type_name']}\n"
                response += f"  {row['home_team_name']} vs {row['away_team_name']}\n"
                if row['home_goals'] is not None and row['away_goals'] is not None:
                    response += f"  Score: {row['home_goals']}-{row['away_goals']}\n"
                if row['xg_home'] and row['xg_away']:
                    response += f"  Expected Goals: {row['xg_home']:.2f} - {row['xg_away']:.2f}\n"
                if row['venue']:
                    response += f"  Venue: {row['venue']}\n"
                response += f"  Season: {row['season_id']}\n\n"
            
            return response
            
    except Exception as e:
        logger.error(f"Error in get_match_results: {e}")
        return f"Error getting match results: {str(e)}"

if __name__ == "__main__":
    # Run as HTTP server for remote MCP access
    import os
    port = int(os.environ.get("PORT", 8000))
    
    try:
        # Verify database exists
        if not DB_PATH.exists():
            logger.error(f"Database not found at {DB_PATH}")
            raise FileNotFoundError(f"Database not found at {DB_PATH}")
        
        # Get the FastMCP HTTP app
        app = mcp.http_app()
        
        # Add a simple health check endpoint
        @app.get("/health")
        def health_check():
            return {"status": "healthy", "service": "NWSL MCP Server"}
        
        # Run with uvicorn
        uvicorn.run(
            app, 
            host="0.0.0.0", 
            port=port,
            log_level="error"  # Keep logs minimal for MCP
        )
        
    except Exception as e:
        logger.error(f"Failed to start NWSL MCP Server: {e}")
        raise