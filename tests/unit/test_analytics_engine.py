"""
Unit Tests for Analytics Engine
===============================

Tests for the core NWSL analytics engine functionality.
"""

import pytest
from unittest.mock import Mock, patch

from src.core.analytics_engine import NWSLAnalyticsEngine, EntityType, AnalyticsContext


class TestNWSLAnalyticsEngine:
    """Test the core analytics engine."""
    
    @pytest.fixture
    def analytics_engine(self, test_database):
        """Create analytics engine with test database."""
        return NWSLAnalyticsEngine(test_database)
    
    def test_engine_initialization(self, analytics_engine):
        """Test analytics engine initializes correctly."""
        assert analytics_engine is not None
        assert analytics_engine.db_path is not None
    
    def test_calculate_nir_score_basic(self, analytics_engine, sample_analytics_context):
        """Test basic NIR score calculation."""
        # This would test the NIR calculation logic
        # For now, we'll test that the method exists and handles basic cases
        
        with patch.object(analytics_engine, '_get_player_base_metrics') as mock_metrics:
            mock_metrics.return_value = {
                'goals': 5,
                'assists': 3,
                'minutes_played': 720
            }
            
            result = analytics_engine.calculate_advanced_metrics(
                EntityType.PLAYER, "Test Player", sample_analytics_context
            )
            
            assert 'nir_score' in result
            assert isinstance(result['nir_score'], (int, float))
    
    def test_entity_type_validation(self, analytics_engine, sample_analytics_context):
        """Test that entity types are properly validated."""
        valid_types = [EntityType.PLAYER, EntityType.TEAM, EntityType.MATCH]
        
        for entity_type in valid_types:
            # Should not raise an exception
            result = analytics_engine.calculate_advanced_metrics(
                entity_type, "test_entity", sample_analytics_context
            )
            assert result is not None
    
    def test_analytics_context_usage(self, analytics_engine):
        """Test analytics context is properly used in calculations."""
        context_2024 = AnalyticsContext(season_id=2024)
        context_2023 = AnalyticsContext(season_id=2023)
        
        assert context_2024.season_id == 2024
        assert context_2023.season_id == 2023
        assert context_2024.season_id != context_2023.season_id


class TestAnalyticsContext:
    """Test the analytics context class."""
    
    def test_context_creation(self):
        """Test context creation with different parameters."""
        context = AnalyticsContext(season_id=2024)
        assert context.season_id == 2024
    
    def test_context_defaults(self):
        """Test default context values."""
        context = AnalyticsContext(season_id=2024)
        # Test that context has reasonable defaults
        assert hasattr(context, 'season_id')


class TestEntityType:
    """Test the EntityType enum."""
    
    def test_entity_types_exist(self):
        """Test that all expected entity types exist."""
        assert EntityType.PLAYER is not None
        assert EntityType.TEAM is not None
        assert EntityType.MATCH is not None
    
    def test_entity_type_values(self):
        """Test entity type string values."""
        assert EntityType.PLAYER.value == "player"
        assert EntityType.TEAM.value == "team"
        assert EntityType.MATCH.value == "match"