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

@lru_cache(maxsize=16)
def _calculate_standings_from_matches(season: int) -> str:
    """Calculate standings from raw match data as fallback."""
    with get_db_connection() as conn:
        # Get all regular season matches for the season
        matches_query = """
        SELECT home_team_name, away_team_name, home_goals, away_goals, xg_home, xg_away
        FROM match
        WHERE season_id = ? AND match_type_name = 'Regular Season'
        AND home_goals IS NOT NULL AND away_goals IS NOT NULL
        ORDER BY match_date
        """
        
        cursor = conn.execute(matches_query, (season,))
        matches = cursor.fetchall()
        
        if not matches:
            return f"No match data found for {season} regular season"
        
        # Calculate standings
        teams = {}
        
        for match in matches:
            home_team = match['home_team_name']
            away_team = match['away_team_name']
            home_goals = match['home_goals']
            away_goals = match['away_goals']
            
            # Initialize teams
            for team in [home_team, away_team]:
                if team not in teams:
                    teams[team] = {
                        'games': 0, 'wins': 0, 'draws': 0, 'losses': 0,
                        'goals_for': 0, 'goals_against': 0, 'points': 0,
                        'xg_for': 0.0, 'xg_against': 0.0
                    }
            
            # Update stats
            teams[home_team]['games'] += 1
            teams[away_team]['games'] += 1
            teams[home_team]['goals_for'] += home_goals
            teams[home_team]['goals_against'] += away_goals
            teams[away_team]['goals_for'] += away_goals
            teams[away_team]['goals_against'] += home_goals
            
            # Add xG if available
            if match['xg_home'] and match['xg_away']:
                teams[home_team]['xg_for'] += match['xg_home']
                teams[home_team]['xg_against'] += match['xg_away']
                teams[away_team]['xg_for'] += match['xg_away']
                teams[away_team]['xg_against'] += match['xg_home']
            
            # Determine result
            if home_goals > away_goals:
                teams[home_team]['wins'] += 1
                teams[home_team]['points'] += 3
                teams[away_team]['losses'] += 1
            elif home_goals < away_goals:
                teams[away_team]['wins'] += 1
                teams[away_team]['points'] += 3
                teams[home_team]['losses'] += 1
            else:
                teams[home_team]['draws'] += 1
                teams[away_team]['draws'] += 1
                teams[home_team]['points'] += 1
                teams[away_team]['points'] += 1
        
        # Sort by points, then goal differential
        sorted_teams = sorted(teams.items(), key=lambda x: (
            -x[1]['points'],  # Points (descending)
            -(x[1]['goals_for'] - x[1]['goals_against']),  # Goal diff (descending)
            -x[1]['goals_for']  # Goals for (descending)
        ))
        
        # Format response
        response = f"{season} NWSL Regular Season Standings (Live Calculation):\n\n"
        
        for pos, (team_name, stats) in enumerate(sorted_teams, 1):
            gd = stats['goals_for'] - stats['goals_against']
            response += f"{pos:2d}. {team_name}\n"
            response += f"     GP: {stats['games']}, W: {stats['wins']}, L: {stats['losses']}, D: {stats['draws']}, Pts: {stats['points']}\n"
            response += f"     GF: {stats['goals_for']}, GA: {stats['goals_against']}, GD: {gd:+d}\n"
            
            if stats['xg_for'] > 0:
                xgd = stats['xg_for'] - stats['xg_against']
                response += f"     xG: {stats['xg_for']:.1f}, xGA: {stats['xg_against']:.1f}, xGD: {xgd:+.1f}\n"
            
            response += "\n"
        
        return response

@mcp.tool()
def get_league_standings(season: int) -> str:
    """Get NWSL league standings ranked by points and goal differential.
    
    Args:
        season: Season year (e.g., 2024)
    
    Returns:
        Formatted string with official league standings by points
    """
    error_msg = validate_season(season)
    if error_msg:
        return error_msg
        
    try:
        # Try pre-calculated standings first
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
            
            if results:
                response = f"{season} NWSL Regular Season Standings:\n\n"
                for row in results:
                    response += f"{row['position']:2d}. {row['team_name']}\n"
                    response += f"     GP: {row['matches_played']}, W: {row['wins']}, L: {row['losses']}, D: {row['draws']}, Pts: {row['points']}\n"
                    response += f"     GF: {row['goals_for']}, GA: {row['goals_against']}, GD: {row['goal_differential']:+d}\n"
                    if row['xg_for'] and row['xg_against']:
                        response += f"     xG: {row['xg_for']:.1f}, xGA: {row['xg_against']:.1f}, xGD: {row['xg_differential']:+.1f}\n"
                    response += "\n"
                return response
            else:
                # Fallback: calculate from match data
                return _calculate_standings_from_matches(season)
            
    except Exception as e:
        logger.error(f"Error in get_league_standings: {e}")
        # Final fallback
        try:
            return _calculate_standings_from_matches(season)
        except:
            return f"Unable to get standings for {season}. Try specific team queries instead."

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

@mcp.tool()
def analyze_season(season: int) -> str:
    """Comprehensive season analysis with narrative insights and context.
    
    Args:
        season: Season year (2013-2025)
    
    Returns:
        Rich narrative analysis of the season with key insights
    """
    error_msg = validate_season(season)
    if error_msg:
        return error_msg
    
    try:
        with get_db_connection() as conn:
            # Get comprehensive season data
            overview_query = """
            SELECT 
                COUNT(DISTINCT CASE WHEN match_type_name = 'Regular Season' THEN match_id END) as regular_games,
                COUNT(DISTINCT CASE WHEN match_type_name != 'Regular Season' THEN match_id END) as other_games,
                COUNT(DISTINCT home_team_name) as teams,
                AVG(CASE WHEN xg_home IS NOT NULL AND xg_away IS NOT NULL 
                         THEN xg_home + xg_away END) as avg_total_xg,
                AVG(CASE WHEN home_goals IS NOT NULL AND away_goals IS NOT NULL 
                         THEN home_goals + away_goals END) as avg_total_goals,
                AVG(ABS(home_goals - away_goals)) as avg_goal_diff,
                COUNT(CASE WHEN home_goals = away_goals THEN 1 END) * 100.0 / 
                COUNT(CASE WHEN home_goals IS NOT NULL AND away_goals IS NOT NULL THEN 1 END) as draw_pct
            FROM match
            WHERE season_id = ?
            """
            
            cursor = conn.execute(overview_query, (season,))
            overview = cursor.fetchone()
            
            if not overview or overview['regular_games'] == 0:
                return f"No data available for {season} season. Try seasons 2013-2024 for complete data."
            
            # Get top and bottom teams if standings available
            standings_query = """
            SELECT team_name, points, wins, losses, draws, goal_differential, position
            FROM league_standings 
            WHERE season_id = ?
            ORDER BY position
            LIMIT 3
            """
            
            cursor = conn.execute(standings_query, (season,))
            top_teams = cursor.fetchall()
            
            # Get bottom teams
            bottom_standings_query = """
            SELECT team_name, points, wins, losses, draws, goal_differential, position
            FROM league_standings 
            WHERE season_id = ?
            ORDER BY position DESC
            LIMIT 3
            """
            cursor = conn.execute(bottom_standings_query, (season,))
            bottom_teams = cursor.fetchall()
            
            # Build narrative analysis
            response = f"📊 **{season} NWSL Season Analysis**\n\n"
            
            # League structure insights
            response += f"**League Overview:**\n"
            response += f"• {overview['teams']} teams competed across {overview['regular_games']} regular season matches\n"
            
            if overview['other_games'] > 0:
                response += f"• {overview['other_games']} playoff/cup matches were also played\n"
            
            # Scoring analysis
            goals_per_game = overview['avg_total_goals'] or 0
            xg_per_game = overview['avg_total_xg'] or 0
            
            response += f"\n**Scoring Trends:**\n"
            response += f"• Average of {goals_per_game:.2f} goals per match"
            
            if goals_per_game < 2.3:
                response += f" (defensive season)\n"
            elif goals_per_game > 2.8:
                response += f" (high-scoring season)\n"
            else:
                response += f" (balanced scoring)\n"
            
            if xg_per_game > 0:
                finishing_efficiency = (goals_per_game / xg_per_game) * 100
                response += f"• Expected goals: {xg_per_game:.2f} per match"
                if finishing_efficiency < 85:
                    response += f" (poor finishing, {finishing_efficiency:.1f}% conversion)\n"
                elif finishing_efficiency > 105:
                    response += f" (clinical finishing, {finishing_efficiency:.1f}% conversion)\n"
                else:
                    response += f" (normal finishing, {finishing_efficiency:.1f}% conversion)\n"
            
            # Competitiveness
            draw_pct = overview['draw_pct'] or 0
            avg_margin = overview['avg_goal_diff'] or 0
            
            response += f"\n**Competitiveness:**\n"
            response += f"• {draw_pct:.1f}% of matches ended in draws"
            if draw_pct < 15:
                response += f" (decisive outcomes)\n"
            elif draw_pct > 25:
                response += f" (very tight competition)\n"
            else:
                response += f" (typical parity)\n"
                
            response += f"• Average winning margin: {avg_margin:.1f} goals"
            if avg_margin < 1.3:
                response += f" (very competitive)\n"
            elif avg_margin > 1.8:
                response += f" (some blowouts)\n"
            else:
                response += f" (balanced competition)\n"
            
            # Standings insights
            if top_teams:
                response += f"\n**Standout Performers:**\n"
                winner = top_teams[0]
                response += f"• **{winner['team_name']}** dominated with {winner['points']} points"
                if winner['wins'] >= 15:
                    response += f" and {winner['wins']} wins (excellent season)\n"
                else:
                    response += f" ({winner['wins']}W-{winner['losses']}L-{winner['draws']}D)\n"
                    
                if len(top_teams) >= 2:
                    second = top_teams[1]
                    response += f"• **{second['team_name']}** finished strong in 2nd with {second['points']} points\n"
                    
                if bottom_teams:
                    worst = bottom_teams[0]
                    response += f"• **{worst['team_name']}** struggled, finishing last with {worst['points']} points\n"
            
            # Data availability note
            if season >= 2025:
                response += f"\n**Note:** Individual player statistics for {season} are still being compiled. Match-level data is complete.\n"
            
            # Suggestions for deeper analysis
            response += f"\n**💡 Want to explore more?**\n"
            response += f"• Ask about specific teams: 'How did [team] perform in {season}?'\n"
            if season > 2013:
                response += f"• Compare seasons: 'Compare {season} to {season-1}'\n"
            response += f"• Get current standings: 'Show me {season} league standings'\n"
            
            return response
            
    except Exception as e:
        logger.error(f"Error in analyze_season: {e}")
        return f"Unable to analyze {season} season. Try asking about specific teams or matches instead."

@mcp.tool()
def compare_seasons(season1: int, season2: int) -> str:
    """Compare two NWSL seasons with key differences highlighted.
    
    Args:
        season1: First season year (2013-2025)
        season2: Second season year (2013-2025)
    
    Returns:
        Detailed comparison highlighting key differences and trends
    """
    # Validate both seasons
    for season in [season1, season2]:
        error_msg = validate_season(season)
        if error_msg:
            return error_msg.replace(f"Invalid season {season}", f"Invalid season {season} in comparison")
    
    if season1 == season2:
        return "Please provide two different seasons to compare."
    
    # Ensure season1 is the earlier year for logical comparison
    if season1 > season2:
        season1, season2 = season2, season1
    
    try:
        with get_db_connection() as conn:
            # Get data for both seasons
            comparison_query = """
            SELECT 
                season_id,
                COUNT(DISTINCT CASE WHEN match_type_name = 'Regular Season' THEN match_id END) as regular_games,
                COUNT(DISTINCT home_team_name) as teams,
                AVG(CASE WHEN home_goals IS NOT NULL AND away_goals IS NOT NULL 
                         THEN home_goals + away_goals END) as avg_goals,
                AVG(CASE WHEN xg_home IS NOT NULL AND xg_away IS NOT NULL 
                         THEN xg_home + xg_away END) as avg_xg,
                COUNT(CASE WHEN home_goals = away_goals THEN 1 END) * 100.0 / 
                COUNT(CASE WHEN home_goals IS NOT NULL AND away_goals IS NOT NULL THEN 1 END) as draw_pct
            FROM match
            WHERE season_id IN (?, ?)
            GROUP BY season_id
            ORDER BY season_id
            """
            
            cursor = conn.execute(comparison_query, (season1, season2))
            results = cursor.fetchall()
            
            if len(results) < 2:
                missing_seasons = []
                found_seasons = [row['season_id'] for row in results]
                if season1 not in found_seasons: missing_seasons.append(season1)
                if season2 not in found_seasons: missing_seasons.append(season2)
                return f"No data available for season(s): {', '.join(map(str, missing_seasons))}"
            
            s1_data = results[0] if results[0]['season_id'] == season1 else results[1]
            s2_data = results[1] if results[1]['season_id'] == season2 else results[0]
            
            response = f"⚖️ **Season Comparison: {season1} vs {season2}**\n\n"
            
            # League growth
            team_change = s2_data['teams'] - s1_data['teams']
            if team_change > 0:
                response += f"📈 **League Growth:** Added {team_change} team(s) ({s1_data['teams']} → {s2_data['teams']})\n"
            elif team_change < 0:
                response += f"📉 **League Contraction:** Lost {abs(team_change)} team(s) ({s1_data['teams']} → {s2_data['teams']})\n"
            else:
                response += f"🔄 **League Size:** Stable at {s1_data['teams']} teams\n"
            
            # Scoring evolution
            goals_change = (s2_data['avg_goals'] or 0) - (s1_data['avg_goals'] or 0)
            response += f"\n⚽ **Scoring Evolution:**\n"
            response += f"• {season1}: {s1_data['avg_goals']:.2f} goals/game\n"
            response += f"• {season2}: {s2_data['avg_goals']:.2f} goals/game\n"
            
            if abs(goals_change) > 0.2:
                if goals_change > 0:
                    response += f"• **Trend:** More attacking (+{goals_change:.2f} goals/game)\n"
                else:
                    response += f"• **Trend:** More defensive ({goals_change:.2f} goals/game)\n"
            else:
                response += f"• **Trend:** Stable scoring levels\n"
            
            # Expected goals comparison
            if s1_data['avg_xg'] and s2_data['avg_xg']:
                xg_change = s2_data['avg_xg'] - s1_data['avg_xg']
                response += f"\n🎯 **Chance Quality:**\n"
                response += f"• {season1}: {s1_data['avg_xg']:.2f} xG/game\n"
                response += f"• {season2}: {s2_data['avg_xg']:.2f} xG/game\n"
                
                if abs(xg_change) > 0.15:
                    if xg_change > 0:
                        response += f"• **Trend:** Better chances created (+{xg_change:.2f} xG/game)\n"
                    else:
                        response += f"• **Trend:** Fewer quality chances ({xg_change:.2f} xG/game)\n"
            
            # Competitiveness
            draw_change = (s2_data['draw_pct'] or 0) - (s1_data['draw_pct'] or 0)
            response += f"\n🏆 **Competitiveness:**\n"
            response += f"• {season1}: {s1_data['draw_pct']:.1f}% draws\n"
            response += f"• {season2}: {s2_data['draw_pct']:.1f}% draws\n"
            
            if abs(draw_change) > 3:
                if draw_change > 0:
                    response += f"• **Trend:** Tighter competition (+{draw_change:.1f}% more draws)\n"
                else:
                    response += f"• **Trend:** More decisive outcomes ({draw_change:.1f}% fewer draws)\n"
            
            # Historical context
            years_apart = season2 - season1
            response += f"\n📅 **Context:**\n"
            if years_apart == 1:
                response += f"• Year-over-year comparison showing immediate changes\n"
            else:
                response += f"• {years_apart}-year gap showing longer-term evolution\n"
            
            # Suggestions
            response += f"\n**💡 Explore Further:**\n"
            response += f"• Compare specific teams across these seasons\n"
            response += f"• Look at individual season analyses: 'analyze {season1}' or 'analyze {season2}'\n"
            response += f"• Check standings for both seasons\n"
            
            return response
            
    except Exception as e:
        logger.error(f"Error in compare_seasons: {e}")
        return f"Unable to compare {season1} and {season2}. Try analyzing individual seasons instead."

@mcp.tool()
def get_data_status() -> str:
    """Show what NWSL data is available and suggest optimal queries.
    
    Returns:
        Overview of data availability and recommended queries
    """
    try:
        with get_db_connection() as conn:
            # Check data availability by season
            availability_query = """
            SELECT 
                s.season_year,
                COUNT(DISTINCT m.match_id) as matches,
                COUNT(DISTINCT ps.player_id) as players_with_stats,
                COUNT(DISTINCT ls.team_name) as teams_in_standings
            FROM season s
            LEFT JOIN match m ON s.season_year = m.season_id
            LEFT JOIN player_season ps ON s.season_year = ps.season_id
            LEFT JOIN league_standings ls ON s.season_year = ls.season_id
            GROUP BY s.season_year
            ORDER BY s.season_year DESC
            """
            
            cursor = conn.execute(availability_query)
            seasons = cursor.fetchall()
            
            response = "📋 **NWSL Data Availability Status**\n\n"
            
            # Current season status
            current_season = 2025
            current_data = next((s for s in seasons if s['season_year'] == current_season), None)
            
            if current_data:
                response += f"🏃 **Current Season ({current_season}):**\n"
                response += f"• ✅ Match data: {current_data['matches']} games available\n"
                response += f"• ✅ Team standings: Available\n"
                if current_data['players_with_stats'] > 0:
                    response += f"• ✅ Player stats: {current_data['players_with_stats']} players\n"
                else:
                    response += f"• ⏳ Player stats: In progress (match data available)\n"
                response += f"\n**Best queries for {current_season}:**\n"
                response += f"• 'analyze {current_season}' - Full season overview\n"
                response += f"• 'get league standings {current_season}' - Current table\n"
                response += f"• 'get team performance [team name] {current_season}' - Team analysis\n"
                response += f"• 'get match results [team name] {current_season}' - Recent games\n\n"
            
            # Historical data summary
            complete_seasons = [s for s in seasons if s['players_with_stats'] > 100 and s['matches'] > 50]
            response += f"📚 **Historical Data ({len(complete_seasons)} complete seasons):**\n"
            
            if complete_seasons:
                earliest = min(s['season_year'] for s in complete_seasons)
                latest = max(s['season_year'] for s in complete_seasons if s['season_year'] < current_season)
                response += f"• ✅ Complete data: {earliest}-{latest}\n"
                response += f"• ✅ Player statistics, match results, standings all available\n\n"
                
                response += f"**Best historical queries:**\n"
                response += f"• 'compare {latest} to {current_season}' - Recent changes\n"
                response += f"• 'analyze {latest}' - Last complete season\n"
                response += f"• 'get top performers {latest} goals' - Historical leaders\n\n"
            
            # Data gaps or limitations
            response += f"⚠️ **Current Limitations:**\n"
            if current_data and current_data['players_with_stats'] == 0:
                response += f"• {current_season} individual player stats not yet available\n"
                response += f"• Use team-level and match-level queries for current season\n"
            
            # Usage tips
            response += f"\n💡 **Pro Tips:**\n"
            response += f"• Start broad: 'analyze [season]' or 'compare [year1] to [year2]'\n"
            response += f"• Then drill down: team performance, player stats, match results\n"
            response += f"• Use partial team names: 'Courage', 'Thorns', 'Angel City'\n"
            response += f"• All tools include helpful suggestions for follow-up queries\n"
            
            return response
            
    except Exception as e:
        logger.error(f"Error in get_data_status: {e}")
        return "Unable to check data status. Basic query tools should still work for most seasons."

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