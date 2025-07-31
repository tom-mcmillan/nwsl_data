#!/usr/bin/env python3
"""
Enhanced Match Analyzer - Query nwsldata.db for comprehensive match analysis
Provides deep insights into matches, teams, and players from the database
"""

import sqlite3
import pandas as pd
from typing import Dict, List, Optional, Tuple
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MatchAnalyzer:
    """
    Enhanced tool for analyzing match data from nwsldata.db
    """
    
    def __init__(self, db_path: str = "data/processed/nwsldata.db"):
        self.db_path = db_path
        
    def get_match_overview(self, match_id: str) -> Dict:
        """Get comprehensive match overview including teams, score, and key stats"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get basic match info
                match_query = """
                SELECT m.match_id, m.match_date, m.season_id,
                       mt1.team_id as home_team_id, t1.team_name_1 as home_team,
                       mt2.team_id as away_team_id, t2.team_name_1 as away_team,
                       mt1.goals as home_goals, mt2.goals as away_goals,
                       mt1.possession_pct as home_possession, mt2.possession_pct as away_possession,
                       mt1.passing_acc_pct as home_passing_acc, mt2.passing_acc_pct as away_passing_acc
                FROM match m
                JOIN match_team mt1 ON m.match_id = mt1.match_id
                JOIN match_team mt2 ON m.match_id = mt2.match_id AND mt1.team_id != mt2.team_id
                JOIN team t1 ON mt1.team_id = t1.team_id
                JOIN team t2 ON mt2.team_id = t2.team_id
                WHERE m.match_id = ?
                LIMIT 1
                """
                
                match_data = pd.read_sql_query(match_query, conn, params=[match_id])
                
                if match_data.empty:
                    return {"error": f"Match {match_id} not found"}
                
                match_info = match_data.iloc[0]
                
                return {
                    "match_id": match_id,
                    "date": match_info['match_date'],
                    "season": match_info['season_id'],
                    "home_team": {
                        "name": match_info['home_team'],
                        "goals": match_info['home_goals'],
                        "possession": match_info['home_possession'],
                        "passing_accuracy": match_info['home_passing_acc']
                    },
                    "away_team": {
                        "name": match_info['away_team'], 
                        "goals": match_info['away_goals'],
                        "possession": match_info['away_possession'],
                        "passing_accuracy": match_info['away_passing_acc']
                    },
                    "result": f"{match_info['home_goals']}-{match_info['away_goals']}"
                }
                
        except Exception as e:
            logger.error(f"❌ Error getting match overview: {str(e)}")
            return {"error": str(e)}
    
    def get_match_goalscorers(self, match_id: str) -> Dict:
        """Get all goalscorers and assists for a match"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get players who scored goals
                goalscorers_query = """
                SELECT mp.player_name, mp.summary_perf_gls as goals, 
                       mp.summary_perf_ast as assists, t.team_name_1 as team,
                       mp.minutes_played, mp.position
                FROM match_player mp
                JOIN team t ON mp.team_id = t.team_id
                WHERE mp.match_id = ? AND mp.summary_perf_gls > 0
                ORDER BY mp.summary_perf_gls DESC, mp.player_name
                """
                
                goalscorers = pd.read_sql_query(goalscorers_query, conn, params=[match_id])
                
                # Get players who made assists
                assists_query = """
                SELECT mp.player_name, mp.summary_perf_ast as assists,
                       t.team_name_1 as team, mp.position
                FROM match_player mp
                JOIN team t ON mp.team_id = t.team_id
                WHERE mp.match_id = ? AND mp.summary_perf_ast > 0
                ORDER BY mp.summary_perf_ast DESC, mp.player_name
                """
                
                assists = pd.read_sql_query(assists_query, conn, params=[match_id])
                
                return {
                    "goalscorers": goalscorers.to_dict('records') if not goalscorers.empty else [],
                    "assists": assists.to_dict('records') if not assists.empty else []
                }
                
        except Exception as e:
            logger.error(f"❌ Error getting goalscorers: {str(e)}")
            return {"error": str(e)}
    
    def get_team_performance_comparison(self, match_id: str) -> Dict:
        """Compare detailed team performance stats for a match"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                team_stats_query = """
                SELECT t.team_name_1 as team_name,
                       mt.goals,
                       mt.possession_pct,
                       mt.passing_acc_pct,
                       mt.SoT_pct,
                       mt.saves_pct,
                       mt.fouls,
                       mt.corners,
                       mt.crosses,
                       mt.touches,
                       mt.tackles,
                       mt.interceptions,
                       mt.aerials_won,
                       mt.clearances,
                       mt.offsides
                FROM match_team mt
                JOIN team t ON mt.team_id = t.team_id
                WHERE mt.match_id = ?
                ORDER BY mt.goals DESC
                """
                
                stats = pd.read_sql_query(team_stats_query, conn, params=[match_id])
                
                if stats.empty:
                    return {"error": f"No team stats found for match {match_id}"}
                
                return {
                    "teams": stats.to_dict('records')
                }
                
        except Exception as e:
            logger.error(f"❌ Error getting team performance: {str(e)}")
            return {"error": str(e)}
    
    def get_top_performers(self, match_id: str) -> Dict:
        """Get top performing players in various categories for a match"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Top scorers
                top_scorers = pd.read_sql_query("""
                    SELECT mp.player_name, t.team_name_1 as team, mp.summary_perf_gls as goals
                    FROM match_player mp
                    JOIN team t ON mp.team_id = t.team_id
                    WHERE mp.match_id = ? AND mp.summary_perf_gls > 0
                    ORDER BY mp.summary_perf_gls DESC
                    LIMIT 5
                    """, conn, params=[match_id])
                
                # Most assists
                top_assists = pd.read_sql_query("""
                    SELECT mp.player_name, t.team_name_1 as team, mp.summary_perf_ast as assists
                    FROM match_player mp
                    JOIN team t ON mp.team_id = t.team_id
                    WHERE mp.match_id = ? AND mp.summary_perf_ast > 0
                    ORDER BY mp.summary_perf_ast DESC
                    LIMIT 5
                    """, conn, params=[match_id])
                
                # Most shots
                top_shots = pd.read_sql_query("""
                    SELECT mp.player_name, t.team_name_1 as team, mp.summary_perf_sh as shots
                    FROM match_player mp
                    JOIN team t ON mp.team_id = t.team_id
                    WHERE mp.match_id = ? AND mp.summary_perf_sh > 0
                    ORDER BY mp.summary_perf_sh DESC
                    LIMIT 5
                    """, conn, params=[match_id])
                
                # Most touches
                top_touches = pd.read_sql_query("""
                    SELECT mp.player_name, t.team_name_1 as team, mp.summary_perf_touches as touches
                    FROM match_player mp
                    JOIN team t ON mp.team_id = t.team_id
                    WHERE mp.match_id = ? AND mp.summary_perf_touches > 0
                    ORDER BY mp.summary_perf_touches DESC
                    LIMIT 5
                    """, conn, params=[match_id])
                
                return {
                    "top_scorers": top_scorers.to_dict('records'),
                    "top_assists": top_assists.to_dict('records'),
                    "most_shots": top_shots.to_dict('records'),
                    "most_touches": top_touches.to_dict('records')
                }
                
        except Exception as e:
            logger.error(f"❌ Error getting top performers: {str(e)}")
            return {"error": str(e)}
    
    def get_season_context(self, match_id: str) -> Dict:
        """Get context about where this match fits in the season"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get match info
                match_info = pd.read_sql_query("""
                    SELECT m.season_id, m.match_date,
                           mt1.team_id as team1_id, t1.team_name_1 as team1_name,
                           mt2.team_id as team2_id, t2.team_name_1 as team2_name
                    FROM match m
                    JOIN match_team mt1 ON m.match_id = mt1.match_id
                    JOIN match_team mt2 ON m.match_id = mt2.match_id AND mt1.team_id != mt2.team_id
                    JOIN team t1 ON mt1.team_id = t1.team_id
                    JOIN team t2 ON mt2.team_id = t2.team_id
                    WHERE m.match_id = ?
                    LIMIT 1
                    """, conn, params=[match_id])
                
                if match_info.empty:
                    return {"error": "Match not found"}
                
                match_data = match_info.iloc[0]
                season_id = match_data['season_id']
                
                # Get season stats for both teams
                season_stats = pd.read_sql_query("""
                    SELECT t.team_name_1 as team,
                           COUNT(*) as matches_played,
                           SUM(mt.goals) as total_goals,
                           AVG(mt.goals) as avg_goals_per_match,
                           AVG(mt.possession_pct) as avg_possession
                    FROM match_team mt
                    JOIN team t ON mt.team_id = t.team_id
                    JOIN match m ON mt.match_id = m.match_id
                    WHERE m.season_id = ? AND mt.team_id IN (?, ?)
                    GROUP BY mt.team_id, t.team_name_1
                    """, conn, params=[season_id, match_data['team1_id'], match_data['team2_id']])
                
                return {
                    "season": season_id,
                    "match_date": match_data['match_date'],
                    "teams": {
                        match_data['team1_name']: season_stats[season_stats['team'] == match_data['team1_name']].to_dict('records')[0] if not season_stats.empty else {},
                        match_data['team2_name']: season_stats[season_stats['team'] == match_data['team2_name']].to_dict('records')[0] if not season_stats.empty else {}
                    }
                }
                
        except Exception as e:
            logger.error(f"❌ Error getting season context: {str(e)}")
            return {"error": str(e)}
    
    def analyze_match_completely(self, match_id: str) -> Dict:
        """Get complete match analysis combining all aspects"""
        logger.info(f"🔍 Analyzing match {match_id} completely...")
        
        analysis = {
            "match_overview": self.get_match_overview(match_id),
            "goalscorers": self.get_match_goalscorers(match_id),
            "team_comparison": self.get_team_performance_comparison(match_id),
            "top_performers": self.get_top_performers(match_id),
            "season_context": self.get_season_context(match_id)
        }
        
        logger.info(f"✅ Complete analysis ready for match {match_id}")
        return analysis

# Usage functions
def analyze_match(match_id: str) -> Dict:
    """
    Main function to get comprehensive match analysis
    """
    analyzer = MatchAnalyzer()
    return analyzer.analyze_match_completely(match_id)

def get_match_summary(match_id: str) -> Dict:
    """Get quick match summary"""
    analyzer = MatchAnalyzer()
    return analyzer.get_match_overview(match_id)

logger.info("🔍 Enhanced Match Analyzer ready!")
logger.info("📖 Usage: analyze_match('match_id') for complete analysis")