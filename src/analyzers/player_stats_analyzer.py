#!/usr/bin/env python3
"""
Player Statistics Analyzer - Query nwsldata.db for player-focused analysis
Provides insights into individual player performance, career stats, and comparisons
"""

import sqlite3
import pandas as pd
from typing import Dict, List, Optional, Tuple
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PlayerStatsAnalyzer:
    """
    Tool for analyzing player statistics from nwsldata.db
    """
    
    def __init__(self, db_path: str = "data/processed/nwsldata.db"):
        self.db_path = db_path
        
    def get_player_season_stats(self, player_name: str, season_id: int) -> Dict:
        """Get comprehensive season statistics for a player"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get player season aggregated stats
                season_stats = pd.read_sql_query("""
                    SELECT 
                        mp.player_name,
                        COUNT(DISTINCT mp.match_id) as matches_played,
                        SUM(mp.minutes_played) as total_minutes,
                        AVG(mp.minutes_played) as avg_minutes_per_match,
                        SUM(mp.summary_perf_gls) as total_goals,
                        SUM(mp.summary_perf_ast) as total_assists,
                        SUM(mp.summary_perf_sh) as total_shots,
                        SUM(mp.summary_perf_sot) as total_shots_on_target,
                        AVG(mp.summary_perf_gls) as goals_per_match,
                        AVG(mp.summary_perf_ast) as assists_per_match,
                        SUM(mp.summary_perf_touches) as total_touches,
                        SUM(mp.summary_perf_tkl) as total_tackles,
                        SUM(mp.summary_perf_int) as total_interceptions,
                        SUM(mp.summary_perf_crdy) as total_yellow_cards,
                        SUM(mp.summary_perf_crdr) as total_red_cards,
                        t.team_name_1 as team,
                        mp.position
                    FROM match_player mp
                    JOIN match m ON mp.match_id = m.match_id
                    JOIN team t ON mp.team_id = t.team_id
                    WHERE mp.player_name = ? AND m.season_id = ?
                    GROUP BY mp.player_name, t.team_name_1, mp.position
                    """, conn, params=[player_name, season_id])
                
                if season_stats.empty:
                    return {"error": f"No data found for {player_name} in {season_id} season"}
                
                # Get best individual match performances
                best_matches = pd.read_sql_query("""
                    SELECT 
                        m.match_date,
                        mp.summary_perf_gls as goals,
                        mp.summary_perf_ast as assists,
                        mp.summary_perf_sh as shots,
                        mp.summary_perf_touches as touches,
                        mp.minutes_played,
                        mp.match_id
                    FROM match_player mp
                    JOIN match m ON mp.match_id = m.match_id
                    WHERE mp.player_name = ? AND m.season_id = ?
                    ORDER BY (mp.summary_perf_gls + mp.summary_perf_ast) DESC, mp.summary_perf_gls DESC
                    LIMIT 5
                    """, conn, params=[player_name, season_id])
                
                return {
                    "player": player_name,
                    "season": season_id,
                    "season_totals": season_stats.iloc[0].to_dict(),
                    "best_performances": best_matches.to_dict('records')
                }
                
        except Exception as e:
            logger.error(f"❌ Error getting player season stats: {str(e)}")
            return {"error": str(e)}
    
    def get_top_scorers_in_season(self, season_id: int, limit: int = 10) -> Dict:
        """Get top goal scorers in a season"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                top_scorers = pd.read_sql_query("""
                    SELECT 
                        mp.player_name,
                        t.team_name_1 as team,
                        SUM(mp.summary_perf_gls) as total_goals,
                        SUM(mp.summary_perf_ast) as total_assists,
                        COUNT(DISTINCT mp.match_id) as matches_played,
                        SUM(mp.summary_perf_sh) as total_shots,
                        ROUND(AVG(mp.summary_perf_gls), 2) as goals_per_match,
                        mp.position
                    FROM match_player mp
                    JOIN match m ON mp.match_id = m.match_id
                    JOIN team t ON mp.team_id = t.team_id
                    WHERE m.season_id = ?
                    GROUP BY mp.player_name, t.team_name_1, mp.position
                    HAVING SUM(mp.summary_perf_gls) > 0
                    ORDER BY total_goals DESC, total_assists DESC
                    LIMIT ?
                    """, conn, params=[season_id, limit])
                
                return {
                    "season": season_id,
                    "top_scorers": top_scorers.to_dict('records')
                }
                
        except Exception as e:
            logger.error(f"❌ Error getting top scorers: {str(e)}")
            return {"error": str(e)}
    
    def get_top_assists_in_season(self, season_id: int, limit: int = 10) -> Dict:
        """Get top assist providers in a season"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                top_assists = pd.read_sql_query("""
                    SELECT 
                        mp.player_name,
                        t.team_name_1 as team,
                        SUM(mp.summary_perf_ast) as total_assists,
                        SUM(mp.summary_perf_gls) as total_goals,
                        COUNT(DISTINCT mp.match_id) as matches_played,
                        ROUND(AVG(mp.summary_perf_ast), 2) as assists_per_match,
                        mp.position
                    FROM match_player mp
                    JOIN match m ON mp.match_id = m.match_id
                    JOIN team t ON mp.team_id = t.team_id
                    WHERE m.season_id = ?
                    GROUP BY mp.player_name, t.team_name_1, mp.position
                    HAVING SUM(mp.summary_perf_ast) > 0
                    ORDER BY total_assists DESC, total_goals DESC
                    LIMIT ?
                    """, conn, params=[season_id, limit])
                
                return {
                    "season": season_id,
                    "top_assist_providers": top_assists.to_dict('records')
                }
                
        except Exception as e:
            logger.error(f"❌ Error getting top assists: {str(e)}")
            return {"error": str(e)}
    
    def compare_players_in_season(self, player1_name: str, player2_name: str, season_id: int) -> Dict:
        """Compare two players' performance in a season"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                comparison_stats = pd.read_sql_query("""
                    SELECT 
                        mp.player_name,
                        t.team_name_1 as team,
                        COUNT(DISTINCT mp.match_id) as matches_played,
                        SUM(mp.minutes_played) as total_minutes,
                        SUM(mp.summary_perf_gls) as total_goals,
                        SUM(mp.summary_perf_ast) as total_assists,
                        SUM(mp.summary_perf_sh) as total_shots,
                        SUM(mp.summary_perf_sot) as shots_on_target,
                        ROUND(AVG(mp.summary_perf_gls), 2) as goals_per_match,
                        ROUND(AVG(mp.summary_perf_ast), 2) as assists_per_match,
                        SUM(mp.summary_perf_touches) as total_touches,
                        SUM(mp.summary_perf_tkl) as total_tackles,
                        SUM(mp.summary_perf_int) as total_interceptions,
                        mp.position
                    FROM match_player mp
                    JOIN match m ON mp.match_id = m.match_id
                    JOIN team t ON mp.team_id = t.team_id
                    WHERE mp.player_name IN (?, ?) AND m.season_id = ?
                    GROUP BY mp.player_name, t.team_name_1, mp.position
                    ORDER BY mp.player_name
                    """, conn, params=[player1_name, player2_name, season_id])
                
                if len(comparison_stats) < 2:
                    return {"error": "Could not find both players in the specified season"}
                
                player1_stats = comparison_stats[comparison_stats['player_name'] == player1_name].iloc[0].to_dict()
                player2_stats = comparison_stats[comparison_stats['player_name'] == player2_name].iloc[0].to_dict()
                
                return {
                    "season": season_id,
                    "player_comparison": {
                        player1_name: player1_stats,
                        player2_name: player2_stats
                    }
                }
                
        except Exception as e:
            logger.error(f"❌ Error comparing players: {str(e)}")
            return {"error": str(e)}
    
    def get_player_career_summary(self, player_name: str) -> Dict:
        """Get career summary across all seasons for a player"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Career totals
                career_totals = pd.read_sql_query("""
                    SELECT 
                        mp.player_name,
                        COUNT(DISTINCT m.season_id) as seasons_played,
                        COUNT(DISTINCT mp.match_id) as total_matches,
                        SUM(mp.minutes_played) as total_minutes,
                        SUM(mp.summary_perf_gls) as career_goals,
                        SUM(mp.summary_perf_ast) as career_assists,
                        SUM(mp.summary_perf_sh) as career_shots,
                        ROUND(AVG(mp.summary_perf_gls), 2) as avg_goals_per_match,
                        ROUND(AVG(mp.summary_perf_ast), 2) as avg_assists_per_match
                    FROM match_player mp
                    JOIN match m ON mp.match_id = m.match_id
                    WHERE mp.player_name = ?
                    GROUP BY mp.player_name
                    """, conn, params=[player_name])
                
                # Season by season breakdown
                season_breakdown = pd.read_sql_query("""
                    SELECT 
                        m.season_id,
                        t.team_name_1 as team,
                        COUNT(DISTINCT mp.match_id) as matches_played,
                        SUM(mp.summary_perf_gls) as goals,
                        SUM(mp.summary_perf_ast) as assists,
                        SUM(mp.minutes_played) as minutes
                    FROM match_player mp
                    JOIN match m ON mp.match_id = m.match_id
                    JOIN team t ON mp.team_id = t.team_id
                    WHERE mp.player_name = ?
                    GROUP BY m.season_id, t.team_name_1
                    ORDER BY m.season_id DESC
                    """, conn, params=[player_name])
                
                if career_totals.empty:
                    return {"error": f"No career data found for {player_name}"}
                
                return {
                    "player": player_name,
                    "career_totals": career_totals.iloc[0].to_dict(),
                    "season_by_season": season_breakdown.to_dict('records')
                }
                
        except Exception as e:
            logger.error(f"❌ Error getting player career summary: {str(e)}")
            return {"error": str(e)}
    
    def get_position_leaders(self, position: str, season_id: int, stat: str = 'goals') -> Dict:
        """Get top performers by position"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                stat_column_map = {
                    'goals': 'summary_perf_gls',
                    'assists': 'summary_perf_ast',
                    'shots': 'summary_perf_sh',
                    'touches': 'summary_perf_touches',
                    'tackles': 'summary_perf_tkl',
                    'interceptions': 'summary_perf_int'
                }
                
                if stat not in stat_column_map:
                    return {"error": f"Invalid stat '{stat}'. Choose from: {list(stat_column_map.keys())}"}
                
                stat_column = stat_column_map[stat]
                
                position_leaders = pd.read_sql_query(f"""
                    SELECT 
                        mp.player_name,
                        t.team_name_1 as team,
                        mp.position,
                        SUM(mp.{stat_column}) as total_{stat},
                        COUNT(DISTINCT mp.match_id) as matches_played,
                        ROUND(AVG(mp.{stat_column}), 2) as {stat}_per_match
                    FROM match_player mp
                    JOIN match m ON mp.match_id = m.match_id
                    JOIN team t ON mp.team_id = t.team_id
                    WHERE mp.position = ? AND m.season_id = ?
                    GROUP BY mp.player_name, t.team_name_1, mp.position
                    HAVING SUM(mp.{stat_column}) > 0
                    ORDER BY total_{stat} DESC
                    LIMIT 10
                    """, conn, params=[position, season_id])
                
                return {
                    "position": position,
                    "season": season_id,
                    "stat": stat,
                    "leaders": position_leaders.to_dict('records')
                }
                
        except Exception as e:
            logger.error(f"❌ Error getting position leaders: {str(e)}")
            return {"error": str(e)}

# Usage functions
def get_player_stats(player_name: str, season_id: int) -> Dict:
    """Get comprehensive player season statistics"""
    analyzer = PlayerStatsAnalyzer()
    return analyzer.get_player_season_stats(player_name, season_id)

def compare_players(player1_name: str, player2_name: str, season_id: int) -> Dict:
    """Compare two players in a season"""
    analyzer = PlayerStatsAnalyzer()
    return analyzer.compare_players_in_season(player1_name, player2_name, season_id)

def get_season_leaders(season_id: int, stat: str = 'goals') -> Dict:
    """Get top performers in a season"""
    analyzer = PlayerStatsAnalyzer()
    if stat == 'goals':
        return analyzer.get_top_scorers_in_season(season_id)
    elif stat == 'assists':
        return analyzer.get_top_assists_in_season(season_id)
    else:
        return {"error": f"Stat '{stat}' not supported for season leaders"}

logger.info("⚽ Player Statistics Analyzer ready!")
logger.info("📖 Usage: get_player_stats('Barbra Banda', 2025) or compare_players('Barbra Banda', 'Marta', 2025)")