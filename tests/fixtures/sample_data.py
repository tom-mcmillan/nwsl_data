"""
Sample Test Data Fixtures
=========================

Reusable test data for NWSL analytics testing.
"""

# Sample match data
SAMPLE_MATCHES = [
    {
        "match_id": "sample_match_001",
        "season_id": 2024,
        "home_team": "Portland Thorns FC",
        "away_team": "OL Reign",
        "home_score": 2,
        "away_score": 1,
        "match_date": "2024-05-01",
    },
    {
        "match_id": "sample_match_002",
        "season_id": 2024,
        "home_team": "Kansas City Current",
        "away_team": "Angel City FC",
        "home_score": 1,
        "away_score": 3,
        "match_date": "2024-05-08",
    },
]

# Sample player performance data
SAMPLE_PLAYER_STATS = [
    {
        "match_id": "sample_match_001",
        "player_name": "Sophia Smith",
        "team_name": "Portland Thorns FC",
        "minutes_played": 90,
        "goals": 1,
        "assists": 1,
        "shots": 4,
        "shots_on_target": 2,
    },
    {
        "match_id": "sample_match_001",
        "player_name": "Megan Rapinoe",
        "team_name": "OL Reign",
        "minutes_played": 85,
        "goals": 1,
        "assists": 0,
        "shots": 3,
        "shots_on_target": 2,
    },
]

# Sample NIR breakdown data
SAMPLE_NIR_BREAKDOWN = {
    "attacking_impact": 0.75,
    "defensive_impact": 0.60,
    "progression_impact": 0.80,
    "creative_impact": 0.70,
    "physical_impact": 0.65,
}

# Sample team analytics
SAMPLE_TEAM_ANALYTICS = {
    "team_name": "Portland Thorns FC",
    "season_id": 2024,
    "matches_played": 15,
    "wins": 9,
    "draws": 3,
    "losses": 3,
    "goals_for": 28,
    "goals_against": 18,
    "nir_score": 0.72,
    "nir_breakdown": SAMPLE_NIR_BREAKDOWN,
}

# Sample visualization data
SAMPLE_CHART_DATA = {
    "chart_type": "radar",
    "title": "Player Performance Profile",
    "data": {
        "categories": ["Attacking", "Defending", "Progression", "Creativity", "Physical"],
        "values": [0.75, 0.60, 0.80, 0.70, 0.65],
    },
}

# Sample season summary
SAMPLE_SEASON_SUMMARY = {
    "season_id": 2024,
    "total_matches": 132,
    "total_teams": 12,
    "top_scorer": "Sophia Smith",
    "most_assists": "Megan Rapinoe",
    "average_goals_per_match": 2.8,
    "season_start": "2024-03-16",
    "season_end": "2024-11-23",
}


# Helper function to create test database with sample data
def create_sample_database(db_path: str):
    """Create a test database with sample data."""
    import sqlite3

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create tables
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS match (
            match_id TEXT PRIMARY KEY,
            season_id INTEGER,
            home_team TEXT,
            away_team TEXT,
            home_score INTEGER,
            away_score INTEGER,
            match_date TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS match_player_summary (
            match_id TEXT,
            player_name TEXT,
            team_name TEXT,
            minutes_played INTEGER,
            goals INTEGER,
            assists INTEGER,
            shots INTEGER,
            shots_on_target INTEGER,
            PRIMARY KEY (match_id, player_name)
        )
    """)

    # Insert sample data
    for match in SAMPLE_MATCHES:
        cursor.execute(
            """
            INSERT INTO match VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (
                match["match_id"],
                match["season_id"],
                match["home_team"],
                match["away_team"],
                match["home_score"],
                match["away_score"],
                match["match_date"],
            ),
        )

    for stats in SAMPLE_PLAYER_STATS:
        cursor.execute(
            """
            INSERT INTO match_player_summary VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                stats["match_id"],
                stats["player_name"],
                stats["team_name"],
                stats["minutes_played"],
                stats["goals"],
                stats["assists"],
                stats["shots"],
                stats["shots_on_target"],
            ),
        )

    conn.commit()
    conn.close()
