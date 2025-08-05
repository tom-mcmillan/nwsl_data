"""
Integration Tests for MCP Server
=================================

Tests for the complete MCP server functionality and tool integration.
"""

import pytest
import asyncio
from unittest.mock import Mock, patch

# Import the actual server components
from src.server import mcp


class TestMCPServerIntegration:
    """Integration tests for the MCP server."""
    
    def test_server_initialization(self):
        """Test that the MCP server initializes correctly."""
        assert mcp is not None
        assert hasattr(mcp, 'tool')
    
    @pytest.mark.asyncio
    async def test_get_database_overview_integration(self, test_database):
        """Test database overview tool integration."""
        # This would test the actual MCP tool
        # For now, we'll test that the function exists and returns valid JSON
        
        with patch('src.server.DB_PATH', test_database):
            from src.server import get_database_overview
            result = get_database_overview()
            
            assert isinstance(result, str)  # Should return JSON string
            # Should be valid JSON
            import json
            parsed = json.loads(result)
            assert isinstance(parsed, dict)
    
    @pytest.mark.asyncio  
    async def test_search_team_names_integration(self, test_database):
        """Test team search tool integration."""
        with patch('src.server.DB_PATH', test_database):
            from src.server import search_team_names
            result = search_team_names("Test Team")
            
            assert isinstance(result, str)
            import json
            parsed = json.loads(result)
            assert isinstance(parsed, dict)
    
    @pytest.mark.asyncio
    async def test_analytics_tool_integration(self, test_database):
        """Test analytics tools integration."""
        with patch('src.server.DB_PATH', test_database):
            from src.server import analyze_team_intelligence
            result = analyze_team_intelligence("Test Team A", 2024)
            
            assert isinstance(result, str)
            import json
            parsed = json.loads(result)
            assert isinstance(parsed, dict)
    
    def test_visualization_tools_integration(self, test_database):
        """Test visualization tools integration."""
        with patch('src.server.DB_PATH', test_database):
            from src.server import create_chart
            result = create_chart("test chart request")
            
            assert isinstance(result, str)
            import json
            parsed = json.loads(result)
            assert isinstance(parsed, dict)


class TestMCPToolsEndToEnd:
    """End-to-end tests for MCP tools."""
    
    @pytest.mark.asyncio
    async def test_player_analysis_workflow(self, test_database):
        """Test complete player analysis workflow."""
        with patch('src.server.DB_PATH', test_database):
            # 1. Search for player
            from src.server import search_team_names
            search_result = search_team_names("Test Team")
            
            # 2. Analyze player performance
            from src.server import analyze_player_intelligence  
            analysis_result = analyze_player_intelligence("Test Player 1", 2024)
            
            # 3. Create visualization
            from src.server import create_player_performance_radar
            viz_result = create_player_performance_radar("Test Player 1", 2024)
            
            # All should return valid JSON
            for result in [search_result, analysis_result, viz_result]:
                assert isinstance(result, str)
                import json
                parsed = json.loads(result)
                assert isinstance(parsed, dict)
    
    @pytest.mark.asyncio
    async def test_team_comparison_workflow(self, test_database):
        """Test complete team comparison workflow."""
        with patch('src.server.DB_PATH', test_database):
            from src.server import compare_team_intelligence, create_team_comparison_chart
            
            # Compare teams
            comparison = compare_team_intelligence("Test Team A", "Test Team B", 2024)
            
            # Create comparison chart
            chart = create_team_comparison_chart("Test Team A", "Test Team B", 2024)
            
            for result in [comparison, chart]:
                assert isinstance(result, str)
                import json
                parsed = json.loads(result)
                assert isinstance(parsed, dict)


class TestErrorHandling:
    """Test error handling in integration scenarios."""
    
    def test_invalid_database_path(self):
        """Test handling of invalid database paths."""
        with patch('src.server.DB_PATH', "/nonexistent/path.db"):
            from src.server import get_database_overview
            result = get_database_overview()
            
            import json
            parsed = json.loads(result)
            assert 'error' in parsed
    
    def test_malformed_requests(self, test_database):
        """Test handling of malformed requests."""
        with patch('src.server.DB_PATH', test_database):
            from src.server import analyze_player_intelligence
            
            # Test with invalid inputs
            result = analyze_player_intelligence("", 9999)
            
            import json
            parsed = json.loads(result)
            # Should handle gracefully, either with error or empty results
            assert isinstance(parsed, dict)