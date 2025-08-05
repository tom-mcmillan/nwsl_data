"""
Pytest Configuration and Shared Fixtures
=========================================

Central configuration for the NWSL Analytics test suite.
"""

import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Generator

import pytest
import pandas as pd


@pytest.fixture(scope="session")
def test_database() -> Generator[str, None, None]:
    """Create a temporary test database with sample data."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp_db:
        db_path = tmp_db.name
    
    try:
        # Create basic tables for testing
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create sample match table
        cursor.execute("""
            CREATE TABLE match (
                match_id TEXT PRIMARY KEY,
                season_id INTEGER,
                home_team TEXT,
                away_team TEXT,
                home_score INTEGER,
                away_score INTEGER,
                match_date TEXT
            )
        """)
        
        # Create sample player table
        cursor.execute("""
            CREATE TABLE match_player_summary (
                match_id TEXT,
                player_name TEXT,
                team_name TEXT,
                minutes_played INTEGER,
                goals INTEGER,
                assists INTEGER,
                PRIMARY KEY (match_id, player_name)
            )
        """)
        
        # Insert sample data
        cursor.execute("""
            INSERT INTO match VALUES 
            ('test_match_1', 2024, 'Test Team A', 'Test Team B', 2, 1, '2024-05-01'),
            ('test_match_2', 2024, 'Test Team B', 'Test Team C', 0, 3, '2024-05-08')
        """)
        
        cursor.execute("""
            INSERT INTO match_player_summary VALUES
            ('test_match_1', 'Test Player 1', 'Test Team A', 90, 1, 0),
            ('test_match_1', 'Test Player 2', 'Test Team A', 85, 1, 1),
            ('test_match_1', 'Test Player 3', 'Test Team B', 90, 0, 0),
            ('test_match_2', 'Test Player 4', 'Test Team C', 90, 2, 0)
        """)
        
        conn.commit()
        conn.close()
        
        yield db_path
        
    finally:
        # Cleanup
        if os.path.exists(db_path):
            os.unlink(db_path)


@pytest.fixture
def sample_analytics_context():
    """Provide sample analytics context for testing."""
    from src.core.analytics_engine import AnalyticsContext
    return AnalyticsContext(season_id=2024)


@pytest.fixture
def mock_team_data():
    """Sample team data for testing."""
    return {
        "team_name": "Test Team A",
        "season_id": 2024,
        "matches_played": 10,
        "wins": 6,
        "draws": 2,
        "losses": 2,
        "goals_for": 18,
        "goals_against": 12
    }


@pytest.fixture
def mock_player_data():
    """Sample player data for testing."""
    return {
        "player_name": "Test Player 1",
        "team_name": "Test Team A",
        "season_id": 2024,
        "matches_played": 8,
        "minutes_played": 720,
        "goals": 5,
        "assists": 3
    }


@pytest.fixture(scope="session")
def test_config():
    """Test configuration settings."""
    return {
        "database": {
            "timeout": 10,
            "pool_size": 2
        },
        "analytics": {
            "nir_calculation": {
                "enable_caching": False,
                "debug_mode": True
            }
        },
        "tools": {
            "player_search_limit": 5,
            "match_results_limit": 10
        }
    }