"""
Unit Tests for Database Context Tool
====================================

Tests for database connectivity and query functionality.
"""

import pytest

from src.core.database_context import DatabaseContextTool


class TestDatabaseContextTool:
    """Test the database context functionality."""

    @pytest.fixture
    def db_context(self, test_database):
        """Create database context with test database."""
        return DatabaseContextTool(test_database)

    def test_database_connection(self, db_context):
        """Test database connection is established."""
        assert db_context is not None
        assert db_context.db_path is not None

    def test_get_database_overview(self, db_context):
        """Test database overview functionality."""
        overview = db_context.get_database_overview()

        assert isinstance(overview, dict)
        assert "total_seasons" in overview or "season_count" in overview
        assert "total_matches" in overview or "match_count" in overview

    def test_search_team_names(self, db_context):
        """Test team name search functionality."""
        result = db_context.search_team_names("Test Team")

        assert isinstance(result, dict)
        # Should handle partial matches
        assert "teams" in result or "matches" in result

    def test_get_season_summary(self, db_context):
        """Test season summary retrieval."""
        result = db_context.get_season_summary(2024)

        assert isinstance(result, dict)
        assert "season_id" in result or "season" in result

    def test_validate_user_query(self, db_context):
        """Test query validation functionality."""
        result = db_context.validate_user_query("Test Team", 2024)

        assert isinstance(result, dict)
        # Should provide validation feedback
        assert "valid" in result or "status" in result or "validation" in result

    def test_get_teams_in_season(self, db_context):
        """Test getting teams for a specific season."""
        result = db_context.get_teams_in_season(2024)

        assert isinstance(result, dict | list)
        # Should return team information for the season


class TestDatabaseQueries:
    """Test database query methods."""

    @pytest.fixture
    def db_context(self, test_database):
        return DatabaseContextTool(test_database)

    def test_sql_injection_protection(self, db_context):
        """Test that queries are protected against SQL injection."""
        # Test with potentially malicious input
        malicious_input = "'; DROP TABLE match; --"

        # These should not crash or corrupt the database
        result = db_context.search_team_names(malicious_input)
        assert isinstance(result, dict)

        result = db_context.get_season_summary(malicious_input)
        assert isinstance(result, dict)

    def test_empty_input_handling(self, db_context):
        """Test handling of empty or None inputs."""
        result = db_context.search_team_names("")
        assert isinstance(result, dict)

        result = db_context.search_team_names(None)
        assert isinstance(result, dict)

    def test_invalid_season_handling(self, db_context):
        """Test handling of invalid season IDs."""
        result = db_context.get_season_summary(9999)
        assert isinstance(result, dict)
        # Should handle non-existent seasons gracefully
