"""
Test Utility Functions
======================

Helper functions for testing NWSL analytics components.
"""

import json
import sqlite3
import tempfile
from pathlib import Path
from typing import Dict, Any, Optional


def validate_json_response(response: str) -> Dict[str, Any]:
    """
    Validate that a response is valid JSON and return parsed data.
    
    Args:
        response: String response to validate
        
    Returns:
        Parsed JSON data
        
    Raises:
        AssertionError: If response is not valid JSON
    """
    try:
        data = json.loads(response)
        assert isinstance(data, dict), "Response should be a JSON object"
        return data
    except json.JSONDecodeError as e:
        raise AssertionError(f"Invalid JSON response: {e}")


def assert_nir_score_valid(nir_score: float):
    """
    Assert that a NIR score is within valid range.
    
    Args:
        nir_score: NIR score to validate
    """
    assert isinstance(nir_score, (int, float)), "NIR score should be numeric"
    assert 0.0 <= nir_score <= 1.0, f"NIR score {nir_score} should be between 0.0 and 1.0"


def assert_nir_breakdown_valid(nir_breakdown: Dict[str, float]):
    """
    Assert that NIR breakdown data is valid.
    
    Args:
        nir_breakdown: NIR breakdown dictionary to validate
    """
    expected_components = {
        'attacking_impact', 'defensive_impact', 'progression_impact'
    }
    
    assert isinstance(nir_breakdown, dict), "NIR breakdown should be a dictionary"
    
    # Check that we have at least the core components
    common_components = set(nir_breakdown.keys()) & expected_components
    assert len(common_components) > 0, "NIR breakdown should have at least one core component"
    
    # Validate each component score
    for component, score in nir_breakdown.items():
        assert isinstance(score, (int, float)), f"{component} should be numeric"
        assert 0.0 <= score <= 1.0, f"{component} score {score} should be between 0.0 and 1.0"


def assert_team_analytics_valid(analytics: Dict[str, Any]):
    """
    Assert that team analytics data is valid.
    
    Args:
        analytics: Team analytics dictionary to validate
    """
    required_fields = {'team_name', 'season_id'}
    
    for field in required_fields:
        assert field in analytics, f"Team analytics missing required field: {field}"
    
    if 'nir_score' in analytics:
        assert_nir_score_valid(analytics['nir_score'])
    
    if 'nir_breakdown' in analytics:
        assert_nir_breakdown_valid(analytics['nir_breakdown'])


def assert_player_analytics_valid(analytics: Dict[str, Any]):
    """
    Assert that player analytics data is valid.
    
    Args:
        analytics: Player analytics dictionary to validate
    """
    required_fields = {'player_name', 'season_id'}
    
    for field in required_fields:
        assert field in analytics, f"Player analytics missing required field: {field}"
    
    if 'nir_score' in analytics:
        assert_nir_score_valid(analytics['nir_score'])
    
    if 'nir_breakdown' in analytics:
        assert_nir_breakdown_valid(analytics['nir_breakdown'])


def assert_visualization_valid(visualization: Dict[str, Any]):
    """
    Assert that visualization data is valid.
    
    Args:
        visualization: Visualization dictionary to validate
    """
    # Should have basic visualization metadata
    if 'chart_type' in visualization:
        valid_chart_types = {'radar', 'bar', 'scatter', 'line', 'stacked_bar'}
        assert visualization['chart_type'] in valid_chart_types
    
    # If it has plotly_json, it should be valid JSON
    if 'plotly_json' in visualization:
        try:
            plotly_data = json.loads(visualization['plotly_json'])
            assert isinstance(plotly_data, dict)
        except json.JSONDecodeError:
            raise AssertionError("plotly_json should be valid JSON")


def create_temp_database_with_schema() -> str:
    """
    Create a temporary database with NWSL schema.
    
    Returns:
        Path to temporary database file
    """
    temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    db_path = temp_db.name
    temp_db.close()
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create basic NWSL schema
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
    
    conn.commit()
    conn.close()
    
    return db_path


def cleanup_temp_database(db_path: str):
    """
    Clean up temporary database file.
    
    Args:
        db_path: Path to database file to remove
    """
    try:
        Path(db_path).unlink()
    except FileNotFoundError:
        pass  # Already deleted


class MockMCPResponse:
    """Mock MCP response for testing."""
    
    def __init__(self, data: Dict[str, Any], success: bool = True):
        self.data = data
        self.success = success
    
    def to_json(self) -> str:
        """Convert to JSON string like real MCP responses."""
        return json.dumps(self.data, indent=2)


def mock_database_query_result(query_type: str) -> Dict[str, Any]:
    """
    Generate mock database query results for testing.
    
    Args:
        query_type: Type of query to mock ('team', 'player', 'match', 'season')
        
    Returns:
        Mock query result data
    """
    if query_type == 'team':
        return {
            'team_name': 'Mock Team FC',
            'season_id': 2024,
            'matches_played': 10,
            'wins': 6,
            'draws': 2,
            'losses': 2
        }
    elif query_type == 'player':
        return {
            'player_name': 'Mock Player',
            'team_name': 'Mock Team FC',
            'season_id': 2024,
            'minutes_played': 800,
            'goals': 5,
            'assists': 3
        }
    elif query_type == 'match':
        return {
            'match_id': 'mock_match_001',
            'season_id': 2024,
            'home_team': 'Mock Team A',
            'away_team': 'Mock Team B',
            'home_score': 2,
            'away_score': 1
        }
    elif query_type == 'season':
        return {
            'season_id': 2024,
            'total_matches': 132,
            'total_teams': 12,
            'start_date': '2024-03-16',
            'end_date': '2024-11-23'
        }
    else:
        return {'mock_data': True}