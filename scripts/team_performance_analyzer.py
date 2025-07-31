#!/usr/bin/env python3
"""
Team Performance Analyzer - Query nwsldata.db for team-focused analysis
Provides insights into team performance trends, head-to-head records, and season stats
"""

import sqlite3
import pandas as pd
from typing import Dict, List, Optional, Tuple
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TeamPerformanceAnalyzer:
    """
    Tool for analyzing team performance from nwsldata.db
    """
    
    def __init__(self, db_path: str = "data/processed/nwsldata.db"):
        self.db_path = db_path
        
    def get_team_season_summary(self, team_name: str, season_id: int) -> Dict:
        """Get comprehensive season summary for a team"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get team ID
                team_id_query = """
                SELECT team_id FROM team 
                WHERE team_name_1 = ? OR team_name_2 = ? OR team_name_3 = ? OR team_name_4 = ?
                """
                team_result = pd.read_sql_query(team_id_query, conn, params=[team_name]*4)
                
                if team_result.empty:
                    return {"error": f"Team '{team_name}' not found"}
                
                team_id = team_result.iloc[0]['team_id']
                
                # Get season statistics
                season_stats = pd.read_sql_query("""
                    SELECT 
                        COUNT(*) as matches_played,
                        SUM(mt.goals) as total_goals_scored,
                        AVG(mt.goals) as avg_goals_per_match,
                        AVG(mt.possession_pct) as avg_possession,
                        AVG(mt.passing_acc_pct) as avg_passing_accuracy,
                        AVG(mt.SoT_pct) as avg_shots_on_target_pct,
                        SUM(mt.fouls) as total_fouls,
                        SUM(mt.corners) as total_corners,
                        SUM(mt.tackles) as total_tackles,
                        SUM(mt.interceptions) as total_interceptions
                    FROM match_team mt
                    JOIN match m ON mt.match_id = m.match_id
                    WHERE mt.team_id = ? AND m.season_id = ?
                    """, conn, params=[team_id, season_id])
                
                # Get wins/losses/draws
                results = pd.read_sql_query("""
                    SELECT 
                        mt.result,
                        COUNT(*) as count
                    FROM match_team mt
                    JOIN match m ON mt.match_id = m.match_id
                    WHERE mt.team_id = ? AND m.season_id = ?
                    GROUP BY mt.result
                    """, conn, params=[team_id, season_id])
                
                # Get top performances
                best_matches = pd.read_sql_query("""
                    SELECT 
                        m.match_date,
                        mt.goals as goals_scored,
                        mt.possession_pct,
                        mt.passing_acc_pct,
                        mt.match_id
                    FROM match_team mt
                    JOIN match m ON mt.match_id = m.match_id
                    WHERE mt.team_id = ? AND m.season_id = ?
                    ORDER BY mt.goals DESC, mt.possession_pct DESC
                    LIMIT 5
                    """, conn, params=[team_id, season_id])
                
                results_summary = {}
                for _, row in results.iterrows():
                    results_summary[row['result']] = row['count']
                
                return {
                    "team": team_name,
                    "season": season_id,
                    "overall_stats": season_stats.iloc[0].to_dict() if not season_stats.empty else {},
                    "record": results_summary,
                    "best_performances": best_matches.to_dict('records')
                }
                
        except Exception as e:
            logger.error(f"❌ Error getting team season summary: {str(e)}")
            return {"error": str(e)}
    
    def get_head_to_head_record(self, team1_name: str, team2_name: str) -> Dict:
        """Get head-to-head record between two teams"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get team IDs
                team_ids = {}
                for team_name in [team1_name, team2_name]:
                    team_result = pd.read_sql_query("""
                        SELECT team_id FROM team 
                        WHERE team_name_1 = ? OR team_name_2 = ? OR team_name_3 = ? OR team_name_4 = ?
                        """, conn, params=[team_name]*4)
                    
                    if team_result.empty:
                        return {"error": f"Team '{team_name}' not found"}
                    
                    team_ids[team_name] = team_result.iloc[0]['team_id']
                
                # Get all matches between these teams
                h2h_matches = pd.read_sql_query("""
                    SELECT 
                        m.match_id,
                        m.match_date,
                        m.season_id,
                        mt1.goals as team1_goals,
                        mt2.goals as team2_goals,
                        mt1.possession_pct as team1_possession,
                        mt2.possession_pct as team2_possession
                    FROM match m
                    JOIN match_team mt1 ON m.match_id = mt1.match_id AND mt1.team_id = ?
                    JOIN match_team mt2 ON m.match_id = mt2.match_id AND mt2.team_id = ?
                    ORDER BY m.match_date DESC
                    """, conn, params=[team_ids[team1_name], team_ids[team2_name]])
                
                if h2h_matches.empty:
                    return {"teams": [team1_name, team2_name], "matches": 0, "record": "No matches found"}
                
                # Calculate record
                team1_wins = len(h2h_matches[h2h_matches['team1_goals'] > h2h_matches['team2_goals']])
                team2_wins = len(h2h_matches[h2h_matches['team2_goals'] > h2h_matches['team1_goals']])
                draws = len(h2h_matches[h2h_matches['team1_goals'] == h2h_matches['team2_goals']])
                
                # Recent form (last 5 matches)
                recent_matches = h2h_matches.head(5)
                
                return {
                    "teams": [team1_name, team2_name],
                    "total_matches": len(h2h_matches),
                    "record": {
                        team1_name: {"wins": team1_wins, "goals_for": h2h_matches['team1_goals'].sum(), "goals_against": h2h_matches['team2_goals'].sum()},
                        team2_name: {"wins": team2_wins, "goals_for": h2h_matches['team2_goals'].sum(), "goals_against": h2h_matches['team1_goals'].sum()},
                        "draws": draws
                    },
                    "recent_matches": recent_matches.to_dict('records'),
                    "all_matches": h2h_matches.to_dict('records')
                }
                
        except Exception as e:
            logger.error(f"❌ Error getting head-to-head record: {str(e)}")
            return {"error": str(e)}
    
    def get_team_top_scorers(self, team_name: str, season_id: int, limit: int = 10) -> Dict:
        """Get top scorers for a team in a season"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get team ID
                team_result = pd.read_sql_query("""
                    SELECT team_id FROM team 
                    WHERE team_name_1 = ? OR team_name_2 = ? OR team_name_3 = ? OR team_name_4 = ?
                    """, conn, params=[team_name]*4)
                
                if team_result.empty:
                    return {"error": f"Team '{team_name}' not found"}
                
                team_id = team_result.iloc[0]['team_id']
                
                # Get top scorers
                top_scorers = pd.read_sql_query("""
                    SELECT 
                        mp.player_name,
                        SUM(mp.summary_perf_gls) as total_goals,
                        SUM(mp.summary_perf_ast) as total_assists,
                        COUNT(DISTINCT mp.match_id) as matches_played,
                        AVG(mp.summary_perf_gls) as goals_per_match,
                        SUM(mp.summary_perf_sh) as total_shots
                    FROM match_player mp
                    JOIN match m ON mp.match_id = m.match_id
                    WHERE mp.team_id = ? AND m.season_id = ?
                    GROUP BY mp.player_name
                    HAVING SUM(mp.summary_perf_gls) > 0
                    ORDER BY total_goals DESC, total_assists DESC
                    LIMIT ?
                    """, conn, params=[team_id, season_id, limit])
                
                return {
                    "team": team_name,
                    "season": season_id,
                    "top_scorers": top_scorers.to_dict('records')
                }
                
        except Exception as e:
            logger.error(f"❌ Error getting top scorers: {str(e)}")
            return {"error": str(e)}
    
    def get_team_form_analysis(self, team_name: str, season_id: int, last_n_matches: int = 5) -> Dict:
        """Analyze team's recent form"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get team ID
                team_result = pd.read_sql_query("""
                    SELECT team_id FROM team 
                    WHERE team_name_1 = ? OR team_name_2 = ? OR team_name_3 = ? OR team_name_4 = ?
                    """, conn, params=[team_name]*4)
                
                if team_result.empty:
                    return {"error": f"Team '{team_name}' not found"}
                
                team_id = team_result.iloc[0]['team_id']
                
                # Get recent matches
                recent_matches = pd.read_sql_query("""
                    SELECT 
                        m.match_date,
                        mt.goals as goals_for,
                        mt.result,
                        mt.possession_pct,
                        mt.passing_acc_pct,
                        mt.match_id
                    FROM match_team mt
                    JOIN match m ON mt.match_id = m.match_id
                    WHERE mt.team_id = ? AND m.season_id = ?
                    ORDER BY m.match_date DESC
                    LIMIT ?
                    """, conn, params=[team_id, season_id, last_n_matches])
                
                if recent_matches.empty:
                    return {"error": "No recent matches found"}
                
                # Calculate form metrics
                wins = len(recent_matches[recent_matches['result'] == 'W'])
                draws = len(recent_matches[recent_matches['result'] == 'D'])
                losses = len(recent_matches[recent_matches['result'] == 'L'])
                
                total_goals = recent_matches['goals_for'].sum()
                avg_possession = recent_matches['possession_pct'].mean()
                avg_passing_acc = recent_matches['passing_acc_pct'].mean()
                
                # Form string (W/D/L for last matches)
                form_string = ''.join(recent_matches['result'].tolist())
                
                return {
                    "team": team_name,
                    "season": season_id,
                    "last_n_matches": last_n_matches,
                    "form": {
                        "wins": wins,
                        "draws": draws,
                        "losses": losses,
                        "form_string": form_string,
                        "points": wins * 3 + draws,  # Assuming 3 points for win, 1 for draw
                        "total_goals": int(total_goals),
                        "avg_goals_per_match": round(total_goals / len(recent_matches), 2),
                        "avg_possession": round(avg_possession, 1) if pd.notna(avg_possession) else None,
                        "avg_passing_accuracy": round(avg_passing_acc, 1) if pd.notna(avg_passing_acc) else None
                    },
                    "recent_matches": recent_matches.to_dict('records')
                }
                
        except Exception as e:
            logger.error(f"❌ Error getting team form: {str(e)}")
            return {"error": str(e)}
    
    def compare_teams_in_season(self, team1_name: str, team2_name: str, season_id: int) -> Dict:
        """Compare two teams' performance in a season"""
        logger.info(f"⚖️  Comparing {team1_name} vs {team2_name} in {season_id} season")
        
        team1_summary = self.get_team_season_summary(team1_name, season_id)
        team2_summary = self.get_team_season_summary(team2_name, season_id)
        h2h_record = self.get_head_to_head_record(team1_name, team2_name)
        
        return {
            "season": season_id,
            "team_comparison": {
                team1_name: team1_summary,
                team2_name: team2_summary
            },
            "head_to_head": h2h_record
        }

# Usage functions
def analyze_team_season(team_name: str, season_id: int) -> Dict:
    """Get comprehensive team season analysis"""
    analyzer = TeamPerformanceAnalyzer()
    return analyzer.get_team_season_summary(team_name, season_id)

def compare_teams(team1_name: str, team2_name: str, season_id: int) -> Dict:
    """Compare two teams in a season"""
    analyzer = TeamPerformanceAnalyzer()
    return analyzer.compare_teams_in_season(team1_name, team2_name, season_id)

def get_head_to_head(team1_name: str, team2_name: str) -> Dict:
    """Get head-to-head record between teams"""
    analyzer = TeamPerformanceAnalyzer()
    return analyzer.get_head_to_head_record(team1_name, team2_name)

logger.info("⚖️  Team Performance Analyzer ready!")
logger.info("📖 Usage: analyze_team_season('Orlando Pride', 2025) or compare_teams('Orlando Pride', 'Chicago Stars FC', 2025)")