# mypy: ignore-errors
#!/usr/bin/env python3
"""
Database Context Tool - Provide current database status and available data
Helps the assistant understand what data is available and current context
"""

import logging
import sqlite3
from datetime import datetime

import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class DatabaseContextTool:
    """
    Tool to provide context about what data is available in the database
    """

    def __init__(self, db_path: str = "data/processed/nwsldata.db"):
        self.db_path = db_path

    def get_database_overview(self) -> dict:
        """Get comprehensive overview of database contents"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Get available seasons
                seasons = pd.read_sql_query(
                    """
                    SELECT DISTINCT season_id, COUNT(*) as matches
                    FROM match 
                    GROUP BY season_id 
                    ORDER BY season_id DESC
                    """,
                    conn,
                )

                # Get current/latest season
                latest_season = seasons.iloc[0]["season_id"] if not seasons.empty else None

                # Get total teams
                teams_count = pd.read_sql_query("SELECT COUNT(*) as count FROM team", conn).iloc[0]["count"]

                # Get total matches
                matches_count = pd.read_sql_query("SELECT COUNT(*) as count FROM match", conn).iloc[0]["count"]

                # Get total players
                players_count = pd.read_sql_query("SELECT COUNT(*) as count FROM player", conn).iloc[0]["count"]

                # Get data quality info
                team_stats_coverage = pd.read_sql_query(
                    """
                    SELECT COUNT(*) as with_stats FROM match_team WHERE possession_pct IS NOT NULL
                    """,
                    conn,
                ).iloc[0]["with_stats"]

                player_stats_coverage = pd.read_sql_query(
                    """
                    SELECT COUNT(*) as with_goals FROM match_player_summary WHERE goals IS NOT NULL
                    """,
                    conn,
                ).iloc[0]["with_goals"]

                return {
                    "database_status": "active",
                    "current_year": datetime.now().year,
                    "latest_season_in_db": int(latest_season) if latest_season else None,
                    "total_seasons": len(seasons),
                    "available_seasons": seasons["season_id"].tolist(),
                    "season_match_counts": dict(zip(seasons["season_id"], seasons["matches"], strict=False)),
                    "total_teams": int(teams_count),
                    "total_matches": int(matches_count),
                    "total_players": int(players_count),
                    "data_quality": {
                        "team_stats_coverage": f"{team_stats_coverage}/{matches_count*2} ({team_stats_coverage/(matches_count*2)*100:.1f}%)",
                        "player_goal_stats_coverage": f"{player_stats_coverage} records with goal data",
                    },
                }

        except Exception as e:
            logger.error(f"❌ Error getting database overview: {str(e)}")
            return {"error": str(e)}

    def get_teams_in_season(self, season_id: int) -> dict:
        """Get all teams that played in a specific season"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                teams = pd.read_sql_query(
                    """
                    SELECT DISTINCT t.team_name_1 as team_name, 
                           COUNT(DISTINCT mt.match_id) as matches_played,
                           SUM(mt.goals) as total_goals
                    FROM team t
                    JOIN match_team mt ON t.team_id = mt.team_id
                    JOIN match m ON mt.match_id = m.match_id
                    WHERE m.season_id = ?
                    GROUP BY t.team_id, t.team_name_1
                    ORDER BY t.team_name_1
                    """,
                    conn,
                    params=[season_id],
                )

                return {
                    "season": season_id,
                    "teams_count": len(teams),
                    "teams": teams.to_dict("records"),
                }

        except Exception as e:
            logger.error(f"❌ Error getting teams for season {season_id}: {str(e)}")
            return {"error": str(e)}

    def search_team_names(self, search_term: str) -> dict:
        """Search for teams by partial name match"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                teams = pd.read_sql_query(
                    """
                    SELECT team_id, team_name_1, team_name_2, team_name_3, team_name_4
                    FROM team
                    WHERE team_name_1 LIKE ? OR team_name_2 LIKE ? OR team_name_3 LIKE ? OR team_name_4 LIKE ?
                    ORDER BY team_name_1
                    """,
                    conn,
                    params=[f"%{search_term}%"] * 4,
                )

                # Get seasons for each team
                results = []
                for _, team in teams.iterrows():
                    seasons = pd.read_sql_query(
                        """
                        SELECT DISTINCT m.season_id, COUNT(*) as matches
                        FROM match_team mt
                        JOIN match m ON mt.match_id = m.match_id
                        WHERE mt.team_id = ?
                        GROUP BY m.season_id
                        ORDER BY m.season_id DESC
                        """,
                        conn,
                        params=[team["team_id"]],
                    )

                    results.append(
                        {
                            "team_id": team["team_id"],
                            "primary_name": team["team_name_1"],
                            "all_names": [
                                n
                                for n in [
                                    team["team_name_1"],
                                    team["team_name_2"],
                                    team["team_name_3"],
                                    team["team_name_4"],
                                ]
                                if n
                            ],
                            "seasons_played": seasons["season_id"].tolist() if not seasons.empty else [],
                            "total_matches": int(seasons["matches"].sum()) if not seasons.empty else 0,
                        }
                    )

                return {"search_term": search_term, "matches_found": len(results), "teams": results}

        except Exception as e:
            logger.error(f"❌ Error searching teams: {str(e)}")
            return {"error": str(e)}

    def get_season_summary(self, season_id: int) -> dict:
        """Get comprehensive summary of a specific season"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Basic season info
                season_info = pd.read_sql_query(
                    """
                    SELECT 
                        COUNT(DISTINCT m.match_id) as total_matches,
                        COUNT(DISTINCT mt.team_id) as teams_count,
                        MIN(m.match_date) as season_start,
                        MAX(m.match_date) as season_end,
                        SUM(mt.goals) as total_goals
                    FROM match m
                    JOIN match_team mt ON m.match_id = mt.match_id
                    WHERE m.season_id = ?
                    """,
                    conn,
                    params=[season_id],
                )

                # Top scoring teams
                top_teams = pd.read_sql_query(
                    """
                    SELECT t.team_name_1, SUM(mt.goals) as total_goals, COUNT(*) as matches
                    FROM match_team mt
                    JOIN match m ON mt.match_id = m.match_id
                    JOIN team t ON mt.team_id = t.team_id
                    WHERE m.season_id = ?
                    GROUP BY t.team_id, t.team_name_1
                    ORDER BY total_goals DESC
                    LIMIT 5
                    """,
                    conn,
                    params=[season_id],
                )

                if season_info.empty:
                    return {"error": f"No data found for season {season_id}"}

                info = season_info.iloc[0]

                return {
                    "season": season_id,
                    "total_matches": int(info["total_matches"]),
                    "teams_count": int(info["teams_count"]),
                    "season_dates": {"start": info["season_start"], "end": info["season_end"]},
                    "total_goals": int(info["total_goals"]),
                    "avg_goals_per_match": round(info["total_goals"] / info["total_matches"], 2),
                    "top_scoring_teams": top_teams.to_dict("records"),
                }

        except Exception as e:
            logger.error(f"❌ Error getting season summary: {str(e)}")
            return {"error": str(e)}

    def validate_user_query(self, team_name: str = None, season_id: int = None) -> dict:
        """Validate if user's query parameters exist in database"""
        try:
            validation_results = {"valid": True, "issues": [], "suggestions": []}

            with sqlite3.connect(self.db_path) as conn:
                # Check season
                if season_id:
                    season_exists = (
                        pd.read_sql_query(
                            """
                        SELECT COUNT(*) as count FROM match WHERE season_id = ?
                        """,
                            conn,
                            params=[season_id],
                        ).iloc[0]["count"]
                        > 0
                    )

                    if not season_exists:
                        validation_results["valid"] = False
                        validation_results["issues"].append(f"Season {season_id} not found")

                        # Suggest available seasons
                        available_seasons = pd.read_sql_query(
                            """
                            SELECT DISTINCT season_id FROM match ORDER BY season_id DESC LIMIT 5
                            """,
                            conn,
                        )["season_id"].tolist()
                        validation_results["suggestions"].append(f"Available seasons: {available_seasons}")

                # Check team
                if team_name:
                    team_exists = (
                        pd.read_sql_query(
                            """
                        SELECT COUNT(*) as count FROM team 
                        WHERE team_name_1 LIKE ? OR team_name_2 LIKE ? OR team_name_3 LIKE ? OR team_name_4 LIKE ?
                        """,
                            conn,
                            params=[f"%{team_name}%"] * 4,
                        ).iloc[0]["count"]
                        > 0
                    )

                    if not team_exists:
                        validation_results["valid"] = False
                        validation_results["issues"].append(f"Team '{team_name}' not found")

                        # Suggest similar teams
                        similar_teams = pd.read_sql_query(
                            """
                            SELECT team_name_1 FROM team 
                            WHERE team_name_1 LIKE ? 
                            ORDER BY team_name_1 
                            LIMIT 3
                            """,
                            conn,
                            params=[f"%{team_name.split()[0]}%"],
                        )["team_name_1"].tolist()

                        if similar_teams:
                            validation_results["suggestions"].append(f"Did you mean: {similar_teams}")

            return validation_results

        except Exception as e:
            logger.error(f"❌ Error validating query: {str(e)}")
            return {"error": str(e)}


# Usage functions
def get_db_context() -> dict:
    """Get database overview and context"""
    tool = DatabaseContextTool()
    return tool.get_database_overview()


def find_team(search_term: str) -> dict:
    """Search for teams by name"""
    tool = DatabaseContextTool()
    return tool.search_team_names(search_term)


def get_season_info(season_id: int) -> dict:
    """Get detailed season information"""
    tool = DatabaseContextTool()
    return tool.get_season_summary(season_id)


def validate_query(team_name: str = None, season_id: int = None) -> dict:
    """Validate user query parameters"""
    tool = DatabaseContextTool()
    return tool.validate_user_query(team_name, season_id)


logger.info("🔍 Database Context Tool ready!")
logger.info("📖 Usage: get_db_context() for overview, find_team('Courage') to search teams")
