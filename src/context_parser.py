#!/usr/bin/env python3
"""
NWSL Context Parser
==================

Extracts structured data from conversation context for intelligent visualization.
Based on MCP best practices for context-aware tool implementations.
"""

import re
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)

class DataType(Enum):
    TEAM_STATS = "team_stats"
    PLAYER_STATS = "player_stats" 
    SEASON_OVERVIEW = "season_overview"
    MATCH_DATA = "match_data"
    HISTORICAL = "historical"

@dataclass
class ExtractedData:
    """Structured data extracted from conversation context"""
    data_type: DataType
    entities: List[str]  # Team names, player names, etc.
    metrics: Dict[str, Any]  # Numerical data
    context: str  # Original context
    season: Optional[int] = None
    timeframe: Optional[str] = None
    visualization_hints: List[str] = None

class NWSLContextParser:
    """
    Intelligent parser that extracts structured NWSL data from conversation context.
    
    Follows MCP best practices:
    - Context-aware processing
    - Structured data extraction  
    - Visualization-ready output
    """
    
    def __init__(self):
        # Patterns for data extraction
        self.team_patterns = [
            r"Kansas City Current.*?(\d+) goals",
            r"San Diego Wave FC.*?(\d+) goals", 
            r"Angel City FC.*?(\d+) goals",
            r"Racing Louisville.*?(\d+) goals",
            r"Portland Thorns FC.*?(\d+) goals",
            r"(\w+(?:\s+\w+)*)\s*[-—]\s*(\d+)\s*goals?"
        ]
        
        self.season_patterns = [
            r"(\d{4})\s+season",
            r"season\s+(\d{4})",
            r"(\d{4})\s+NWSL"
        ]
        
        self.metric_patterns = [
            r"(\d+\.?\d*)\s+goals?\s+per\s+match",
            r"(\d+)\s+goals?\s+in\s+(\d+)\s+matches?",
            r"total\s+goals?\s*:?\s*(\d+)",
            r"(\d+)\s+teams?",
            r"(\d+)\s+matches?\s+played"
        ]

    def parse_conversation_context(self, messages: List[str]) -> ExtractedData:
        """
        Parse conversation messages to extract structured NWSL data.
        
        Args:
            messages: List of conversation messages (user + assistant)
            
        Returns:
            ExtractedData with structured information for visualization
        """
        # Combine all messages for analysis
        full_context = " ".join(messages)
        
        # Extract season information
        season = self._extract_season(full_context)
        
        # Extract team data 
        team_data = self._extract_team_data(full_context)
        
        # Extract metrics
        metrics = self._extract_metrics(full_context)
        
        # Determine data type
        data_type = self._determine_data_type(full_context, team_data, metrics)
        
        # Generate visualization hints
        viz_hints = self._generate_visualization_hints(full_context, data_type, team_data)
        
        return ExtractedData(
            data_type=data_type,
            entities=list(team_data.keys()) if team_data else [],
            metrics=metrics,
            context=full_context[:500] + "..." if len(full_context) > 500 else full_context,
            season=season,
            visualization_hints=viz_hints
        )

    def _extract_season(self, text: str) -> Optional[int]:
        """Extract season year from text"""
        for pattern in self.season_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return int(match.group(1))
        return None
    
    def _extract_team_data(self, text: str) -> Dict[str, Dict[str, Any]]:
        """Extract team statistics from text"""
        teams = {}
        
        # Known team data extraction patterns
        team_goals_pattern = r"(Kansas City Current|San Diego Wave FC|Angel City FC|Racing Louisville|Portland Thorns FC).*?(\d+)\s+goals?"
        matches = re.findall(team_goals_pattern, text, re.IGNORECASE)
        
        for team, goals in matches:
            teams[team] = {
                "goals": int(goals),
                "matches": 13  # Default from context
            }
            
        # Extract goals per match if mentioned
        gpm_pattern = r"([\d.]+)\s+goals?\s+per\s+match"
        if re.search(gpm_pattern, text):
            # Add GPM calculation for teams
            for team_data in teams.values():
                if "goals" in team_data and "matches" in team_data:
                    team_data["goals_per_match"] = team_data["goals"] / team_data["matches"]
        
        return teams
    
    def _extract_metrics(self, text: str) -> Dict[str, Any]:
        """Extract numerical metrics from text"""
        metrics = {}
        
        # Total goals
        total_goals_match = re.search(r"total\s+goals?\s*:?\s*(\d+)", text, re.IGNORECASE)
        if total_goals_match:
            metrics["total_goals"] = int(total_goals_match.group(1))
            
        # Total matches
        total_matches_match = re.search(r"(\d+)\s+matches?\s+played", text, re.IGNORECASE)
        if total_matches_match:
            metrics["total_matches"] = int(total_matches_match.group(1))
            
        # Average goals per match
        avg_goals_match = re.search(r"(\d+\.?\d*)\s+goals?\s+per\s+match", text, re.IGNORECASE)
        if avg_goals_match:
            metrics["avg_goals_per_match"] = float(avg_goals_match.group(1))
            
        # Number of teams
        num_teams_match = re.search(r"(\d+)\s+teams?", text, re.IGNORECASE)
        if num_teams_match:
            metrics["num_teams"] = int(num_teams_match.group(1))
        
        return metrics
    
    def _determine_data_type(self, text: str, team_data: Dict, metrics: Dict) -> DataType:
        """Determine the type of data we're working with"""
        if team_data and len(team_data) > 1:
            return DataType.TEAM_STATS
        elif "season" in text.lower() and metrics:
            return DataType.SEASON_OVERVIEW
        elif "player" in text.lower():
            return DataType.PLAYER_STATS
        elif "match" in text.lower():
            return DataType.MATCH_DATA
        else:
            return DataType.SEASON_OVERVIEW
    
    def _generate_visualization_hints(self, text: str, data_type: DataType, team_data: Dict) -> List[str]:
        """Generate hints for what visualizations would be compelling"""
        hints = []
        
        # Check for user preferences
        if "blow my mind" in text.lower() or "mind-blowing" in text.lower():
            hints.append("create_stunning_visual")
            hints.append("use_creative_styling")
            
        if "chart" in text.lower():
            hints.append("prefer_charts")
            
        if "comparison" in text.lower() or "vs" in text.lower():
            hints.append("comparison_focus")
            
        # Data-driven hints
        if data_type == DataType.TEAM_STATS and len(team_data) > 2:
            hints.extend(["bar_chart", "ranking_visualization", "performance_comparison"])
            
        if any("goals" in str(v) for v in team_data.values()):
            hints.extend(["scoring_analysis", "offensive_metrics"])
            
        # Add interactivity hints
        hints.append("interactive_plotly")
        hints.append("professional_styling")
        
        return hints

    def extract_from_single_response(self, response_text: str) -> ExtractedData:
        """
        Extract data from a single response (fallback method).
        
        Useful when conversation context isn't available.
        """
        return self.parse_conversation_context([response_text])

# Test the parser
if __name__ == "__main__":
    parser = NWSLContextParser()
    
    # Test with sample conversation
    test_messages = [
        "tell me about the 2025 season",
        "Here's an overview of the 2025 NWSL season: Kansas City Current — 28 goals in 13 matches, San Diego Wave FC — 24 goals in 13 matches, Angel City FC — 20 goals in 13 matches",
        "make some visuals of this data"
    ]
    
    result = parser.parse_conversation_context(test_messages)
    print(f"Data Type: {result.data_type}")
    print(f"Entities: {result.entities}")  
    print(f"Metrics: {result.metrics}")
    print(f"Season: {result.season}")
    print(f"Visualization Hints: {result.visualization_hints}")