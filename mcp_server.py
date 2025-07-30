#!/usr/bin/env python3
"""
NWSL Data MCP Server
Official MCP server implementation following FastMCP best practices
Provides NWSL data access for players, coaches, and fans (2013-2025)
"""

import sqlite3
from typing import Optional, Dict, Any, List
from pathlib import Path
import logging
from contextlib import contextmanager
from functools import lru_cache
import json

from mcp.server.fastmcp import FastMCP
import uvicorn

# Configure logging for MCP (stderr only, no stdout)
logging.basicConfig(level=logging.ERROR, format='%(levelname)s: %(message)s')
logger = logging.getLogger("nwsl-mcp-server")

# Database configuration
DB_PATH = Path(__file__).parent / "data" / "processed" / "nwsldata.db"

# Team name mapping cache for improved matching
_TEAM_NAME_CACHE = {}

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

def _build_team_name_cache():
    """Build cache of team name variations for better matching."""
    global _TEAM_NAME_CACHE
    if _TEAM_NAME_CACHE:
        return _TEAM_NAME_CACHE
    
    try:
        with get_db_connection() as conn:
            # Get team names from team table
            team_query = "SELECT team_id, team_name_1, team_name_2, team_name_3, team_name_4 FROM team"
            cursor = conn.execute(team_query)
            team_rows = cursor.fetchall()
            
            # Get team names from match data (2025 uses different format)
            match_teams_query = "SELECT DISTINCT home_team_name FROM match WHERE season_id = 2025"
            cursor = conn.execute(match_teams_query)
            match_teams = [row[0] for row in cursor.fetchall()]
            
            # Build comprehensive mapping
            for row in team_rows:
                team_id = row['team_id']
                names = [row['team_name_1'], row['team_name_2'], row['team_name_3'], row['team_name_4']]
                names = [name for name in names if name]  # Remove None values
                
                for name in names:
                    if name:
                        _TEAM_NAME_CACHE[name.lower()] = {
                            'team_id': team_id,
                            'full_name': row['team_name_1'],
                            'variations': names
                        }
            
            # Add 2025 match team names
            for team_name in match_teams:
                if team_name and team_name.lower() not in _TEAM_NAME_CACHE:
                    # Try to match with existing teams
                    matched = False
                    for cached_name, data in _TEAM_NAME_CACHE.items():
                        if team_name.lower() in cached_name or cached_name in team_name.lower():
                            _TEAM_NAME_CACHE[team_name.lower()] = data
                            matched = True
                            break
                    
                    if not matched:
                        # Create new entry for unmatched teams
                        _TEAM_NAME_CACHE[team_name.lower()] = {
                            'team_id': team_name.lower().replace(' ', '_'),
                            'full_name': team_name,
                            'variations': [team_name]
                        }
                        
    except Exception as e:
        logger.error(f"Error building team name cache: {e}")
    
    return _TEAM_NAME_CACHE

def find_team_info(team_name: str) -> Optional[Dict[str, Any]]:
    """Find team information using cached team name mapping."""
    cache = _build_team_name_cache()
    team_lower = team_name.lower()
    
    # Direct match
    if team_lower in cache:
        return cache[team_lower]
    
    # Partial match
    for cached_name, data in cache.items():
        if team_lower in cached_name or cached_name in team_lower:
            return data
    
    return None

def validate_season(season: Optional[int]) -> str:
    """Validate season parameter and return error message if invalid."""
    if season is not None:
        if not isinstance(season, int) or season < 2013 or season > 2025:
            return f"Invalid season {season}. Valid seasons are 2013-2025."
    return ""

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
        team_name: Name of the team to query (e.g., "Courage", "Angel City", "Portland Thorns")
        season: Optional season year. If not provided, returns last 5 seasons
    
    Returns:
        Formatted string with team performance metrics
    """
    # Validate inputs
    error_msg = validate_season(season)
    if error_msg:
        return error_msg
    
    try:
        team_info = find_team_info(team_name)
        if not team_info:
            return f"No team found matching '{team_name}'. Try: Courage, Angel City, Thorns, etc."
        
        with get_db_connection() as conn:
            # Use team name from match data for 2025, team_id for older seasons
            if season and season >= 2025:
                # For 2025+, use team names directly from match table
                team_match_name = team_info['full_name']
                if season:
                    match_query = """
                    SELECT COUNT(*) as games_played,
                           SUM(CASE WHEN (home_team_name = ? AND xg_home > xg_away) 
                                      OR (away_team_name = ? AND xg_away > xg_home) 
                                    THEN 1 ELSE 0 END) as wins,
                           AVG(CASE WHEN home_team_name = ? THEN xg_home 
                                   WHEN away_team_name = ? THEN xg_away END) as avg_xg,
                           AVG(CASE WHEN home_team_name = ? THEN xg_away 
                                   WHEN away_team_name = ? THEN xg_home END) as avg_xga
                    FROM match
                    WHERE (home_team_name = ? OR away_team_name = ?) 
                      AND season_id = ?
                      AND match_type_name = 'Regular Season'
                    """
                    cursor = conn.execute(match_query, 
                        (team_match_name, team_match_name, team_match_name, team_match_name, 
                         team_match_name, team_match_name, team_match_name, team_match_name, season))
                    results = cursor.fetchall()
                else:
                    match_query = """
                    SELECT season_id,
                           COUNT(*) as games_played,
                           SUM(CASE WHEN (home_team_name = ? AND xg_home > xg_away) 
                                      OR (away_team_name = ? AND xg_away > xg_home) 
                                    THEN 1 ELSE 0 END) as wins,
                           AVG(CASE WHEN home_team_name = ? THEN xg_home 
                                   WHEN away_team_name = ? THEN xg_away END) as avg_xg,
                           AVG(CASE WHEN home_team_name = ? THEN xg_away 
                                   WHEN away_team_name = ? THEN xg_home END) as avg_xga
                    FROM match
                    WHERE (home_team_name = ? OR away_team_name = ?)
                      AND match_type_name = 'Regular Season'
                    GROUP BY season_id
                    ORDER BY season_id DESC
                    LIMIT 5
                    """
                    cursor = conn.execute(match_query, 
                        (team_match_name, team_match_name, team_match_name, team_match_name, 
                         team_match_name, team_match_name, team_match_name, team_match_name))
                    results = cursor.fetchall()
            else:
                # For older seasons, use team_id
                team_id = team_info['team_id']
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
                    results = cursor.fetchall()
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
            
            response = f"Team Performance: {team_info['full_name']}\n\n"
            
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
                    response += f"No data found for {team_info['full_name']} in {season} season."
            else:
                if results:
                    for row in results:
                        win_pct = (row['wins'] / row['games_played']) * 100 if row['games_played'] > 0 else 0
                        xg_diff = (row['avg_xg'] - row['avg_xga']) if row['avg_xg'] and row['avg_xga'] else 0
                        response += f"Season {row['season_id']}:\n"
                        response += f"  Games: {row['games_played']}, Wins: {row['wins']} ({win_pct:.1f}%)\n"
                        response += f"  Avg xG: {row['avg_xg']:.2f}, Avg xGA: {row['avg_xga']:.2f} (Diff: {xg_diff:+.2f})\n\n"
                else:
                    response += f"No performance data found for {team_info['full_name']}."
            
            return response
            
    except Exception as e:
        logger.error(f"Error in get_team_performance: {e}")
        return f"Error getting team performance: {str(e)}"

@mcp.tool()
def get_league_standings(season: int) -> str:
    """Get NWSL league standings ranked by points and goal differential.
    
    Args:
        season: Season year (e.g., 2024)
    
    Returns:
        Formatted string with official league standings by points
    """
    try:
        with get_db_connection() as conn:
            query = """
            SELECT position, team_name, matches_played, wins, losses, draws, 
                   points, goals_for, goals_against, goal_differential,
                   xg_for, xg_against, xg_differential
            FROM league_standings 
            WHERE season_id = ?
            ORDER BY position
            """
            
            cursor = conn.execute(query, (season,))
            results = cursor.fetchall()
            
            if not results:
                return f"No standings data found for {season} season"
            
            response = f"{season} NWSL Regular Season Standings:\n\n"
            for row in results:
                win_pct = (row['wins'] / row['matches_played']) * 100 if row['matches_played'] > 0 else 0
                response += f"{row['position']:2d}. {row['team_name']}\n"
                response += f"     GP: {row['matches_played']}, W: {row['wins']}, L: {row['losses']}, D: {row['draws']}, Pts: {row['points']}\n"
                response += f"     GF: {row['goals_for']}, GA: {row['goals_against']}, GD: {row['goal_differential']:+d}\n"
                if row['xg_for'] and row['xg_against']:
                    response += f"     xG: {row['xg_for']:.1f}, xGA: {row['xg_against']:.1f}, xGD: {row['xg_differential']:+.1f}\n"
                response += "\n"
            
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
    # Validate inputs
    error_msg = validate_season(season)
    if error_msg:
        return error_msg
    
    if limit <= 0 or limit > 50:
        return "Limit must be between 1 and 50"
        
    try:
        team1_info = find_team_info(team1)
        if not team1_info:
            return f"No team found matching '{team1}'. Try: Courage, Angel City, Thorns, etc."
        
        team2_info = None
        if team2:
            team2_info = find_team_info(team2)
            if not team2_info:
                return f"No team found matching '{team2}'. Try: Courage, Angel City, Thorns, etc."
        
        with get_db_connection() as conn:
            team1_name = team1_info['full_name']
            
            if team2_info:
                team2_name = team2_info['full_name']
                # Head-to-head matches
                query = """
                SELECT m.match_date, m.home_team_name, m.away_team_name,
                       m.home_goals, m.away_goals, m.xg_home, m.xg_away, 
                       m.season_id, m.match_type_name
                FROM match m
                WHERE ((m.home_team_name = ? AND m.away_team_name = ?)
                    OR (m.home_team_name = ? AND m.away_team_name = ?))
                """
                params = [team1_name, team2_name, team2_name, team1_name]
                
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
                       m.season_id, m.match_type_name
                FROM match m
                WHERE (m.home_team_name = ? OR m.away_team_name = ?)
                """
                params = [team1_name, team1_name]
                
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
                response += f"  Season: {row['season_id']}\n\n"
            
            return response
            
    except Exception as e:
        logger.error(f"Error in get_match_results: {e}")
        return f"Error getting match results: {str(e)}"

@mcp.tool()
def get_top_performers(season: int, category: str = "goals", limit: int = 10) -> str:
    """Get top performing players by various statistical categories.
    
    Args:
        season: Season year (e.g., 2025)
        category: Statistical category - 'goals', 'assists', 'minutes', or 'games'
        limit: Number of players to return (default: 10)
    
    Returns:
        Formatted string with top performers in the specified category
    """
    # Validate inputs
    error_msg = validate_season(season)
    if error_msg:
        return error_msg
    
    valid_categories = ['goals', 'assists', 'minutes', 'games']
    if category not in valid_categories:
        return f"Invalid category '{category}'. Valid options: {', '.join(valid_categories)}"
    
    if limit <= 0 or limit > 50:
        return "Limit must be between 1 and 50"
    
    try:
        with get_db_connection() as conn:
            # Map category to database column
            column_map = {
                'goals': 'goals',
                'assists': 'assists', 
                'minutes': 'minutes',
                'games': 'mp'
            }
            
            column = column_map[category]
            
            query = f"""
            SELECT p.player_name, ps.squad, ps.{column} as stat_value,
                   ps.position, ps.goals, ps.assists, ps.mp, ps.minutes
            FROM player p
            JOIN player_season ps ON p.player_id = ps.player_id
            WHERE ps.season_id = ? AND ps.{column} IS NOT NULL AND ps.{column} > 0
            ORDER BY ps.{column} DESC
            LIMIT ?
            """
            
            cursor = conn.execute(query, (season, limit))
            results = cursor.fetchall()
            
            if not results:
                return f"No player statistics found for {season} season in category '{category}'"
            
            response = f"{season} NWSL Top {category.title()} Leaders:\n\n"
            
            for i, row in enumerate(results, 1):
                response += f"{i:2d}. {row['player_name']} ({row['squad'] or 'Unknown Team'})\n"
                response += f"    {category.title()}: {row['stat_value'] or 0}"
                
                # Add context stats
                if category != 'goals' and row['goals']:
                    response += f", Goals: {row['goals']}"
                if category != 'assists' and row['assists']:
                    response += f", Assists: {row['assists']}"
                if category != 'games' and row['mp']:
                    response += f", Games: {row['mp']}"
                    
                response += f"\n    Position: {row['position'] or 'Unknown'}\n\n"
            
            return response
            
    except Exception as e:
        logger.error(f"Error in get_top_performers: {e}")
        return f"Error getting top performers: {str(e)}"

@mcp.tool()
def get_team_roster(team_name: str, season: int) -> str:
    """Get current roster for a specific team and season.
    
    Args:
        team_name: Name of the team
        season: Season year
    
    Returns:
        Formatted string with team roster information
    """
    # Validate inputs
    error_msg = validate_season(season)
    if error_msg:
        return error_msg
    
    team_info = find_team_info(team_name)
    if not team_info:
        return f"No team found matching '{team_name}'. Try: Courage, Angel City, Thorns, etc."
    
    try:
        with get_db_connection() as conn:
            query = """
            SELECT DISTINCT p.player_name, ps.position, ps.mp, ps.goals, ps.assists, ps.minutes
            FROM player p
            JOIN player_season ps ON p.player_id = ps.player_id
            WHERE ps.squad = ? AND ps.season_id = ?
            ORDER BY ps.position, p.player_name
            """
            
            cursor = conn.execute(query, (team_info['full_name'], season))
            results = cursor.fetchall()
            
            if not results:
                return f"No roster data found for {team_info['full_name']} in {season}"
            
            response = f"{team_info['full_name']} {season} Roster:\n\n"
            
            # Group by position
            positions = {}
            for row in results:
                pos = row['position'] or 'Unknown'
                if pos not in positions:
                    positions[pos] = []
                positions[pos].append(row)
            
            for position in sorted(positions.keys()):
                response += f"{position}:\n"
                for player in positions[position]:
                    response += f"  • {player['player_name']}"
                    if player['mp'] or player['goals'] or player['assists']:
                        stats = []
                        if player['mp']: stats.append(f"Games: {player['mp']}")
                        if player['goals']: stats.append(f"Goals: {player['goals']}")
                        if player['assists']: stats.append(f"Assists: {player['assists']}")
                        response += f" ({', '.join(stats)})"
                    response += "\n"
                response += "\n"
            
            return response
            
    except Exception as e:
        logger.error(f"Error in get_team_roster: {e}")
        return f"Error getting team roster: {str(e)}"

@mcp.tool()
def list_teams(season: Optional[int] = None) -> str:
    """List all teams that participated in a given season.
    
    Args:
        season: Season year. If not provided, shows current season (2025)
    
    Returns:
        Formatted string with list of teams
    """
    if season is None:
        season = 2025
        
    error_msg = validate_season(season)
    if error_msg:
        return error_msg
    
    try:
        with get_db_connection() as conn:
            if season >= 2025:
                # For 2025+, get team names from match data
                query = """
                SELECT DISTINCT home_team_name as team_name
                FROM match
                WHERE season_id = ?
                ORDER BY home_team_name
                """
            else:
                # For older seasons, use team table with season data
                query = """
                SELECT DISTINCT t.team_name_1 as team_name
                FROM team t
                JOIN match m ON (t.team_id = m.home_team_id OR t.team_id = m.away_team_id)
                WHERE m.season_id = ?
                ORDER BY t.team_name_1
                """
            
            cursor = conn.execute(query, (season,))
            results = cursor.fetchall()
            
            if not results:
                return f"No teams found for {season} season"
            
            response = f"{season} NWSL Teams ({len(results)} total):\n\n"
            
            for i, row in enumerate(results, 1):
                response += f"{i:2d}. {row['team_name']}\n"
            
            return response
            
    except Exception as e:
        logger.error(f"Error in list_teams: {e}")
        return f"Error getting teams list: {str(e)}"

if __name__ == "__main__":
    # Run as HTTP server for remote MCP access
    import os
    port = int(os.environ.get("PORT", 8000))
    
    try:
        # Verify database exists
        if not DB_PATH.exists():
            logger.error(f"Database not found at {DB_PATH}")
            raise FileNotFoundError(f"Database not found at {DB_PATH}")
        
        logger.info(f"🚀 Starting NWSL MCP Server on port {port}")
        
        # Get FastMCP streamable HTTP app for Cloud Run deployment
        app = mcp.streamable_http_app()
        
        # Run with uvicorn for Cloud Run
        uvicorn.run(
            app,
            host="0.0.0.0",  # Required for Cloud Run
            port=port,
            log_level="error"  # Keep logs minimal for MCP
        )
        
    except Exception as e:
        logger.error(f"Failed to start NWSL MCP Server: {e}")
        raise