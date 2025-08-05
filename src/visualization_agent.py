#!/usr/bin/env python3
"""
NWSL Data Visualization Agent
============================

Intelligent agent that uses multi-step reasoning to create compelling
data visualizations for NWSL analytics using Plotly.

Uses OpenAI Agents SDK for decision-making and Plotly for visualization generation.
"""

import json
import logging
import base64
from typing import Dict, List, Any, Optional, Tuple
from io import BytesIO
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import plotly

# OpenAI Agents SDK imports
try:
    from openai_agents import Agent, function_tool, Runner
    AGENTS_AVAILABLE = True
except ImportError:
    # Fallback for development
    AGENTS_AVAILABLE = False
    Agent = Runner = None
    
    # Create a dummy decorator for function_tool
    def function_tool(func):
        """Dummy decorator when OpenAI Agents SDK is not available"""
        return func
    
    logging.warning("OpenAI Agents SDK not installed. Run: pip install openai-agents")

from .nwsl_analytics_engine import NWSLAnalyticsEngine, EntityType, AnalyticsContext

logger = logging.getLogger(__name__)

class NWSLDataVisualizationAgent:
    """
    Intelligent visualization agent that creates compelling charts for NWSL data.
    
    Uses multi-step reasoning to:
    1. Analyze user query and available data
    2. Consider multiple visualization options
    3. Select optimal chart type and styling
    4. Generate interactive Plotly visualizations
    5. Provide explanatory insights
    """
    
    def __init__(self, analytics_engine: NWSLAnalyticsEngine):
        self.analytics_engine = analytics_engine
        self.visualization_agent = self._create_agent()
    
    def _create_agent(self) -> Optional[Any]:
        """Create the OpenAI visualization decision agent"""
        if not AGENTS_AVAILABLE or not Agent:
            return None
            
        return Agent(
            name="NWSL Visualization Strategist",
            instructions="""
            You are the world's leading expert in NWSL data visualization and sports analytics presentation.
            
            Your mission is to create the most compelling, insightful visualizations for NWSL data.
            
            ## Your Expertise:
            - Advanced understanding of soccer analytics and what metrics matter
            - Deep knowledge of effective data visualization principles
            - Expertise in telling stories through data
            - Understanding of different chart types and when to use them
            
            ## Your Process:
            1. **Analyze the Query**: What is the user trying to understand?
            2. **Assess the Data**: What type of data do we have? What are the key patterns?
            3. **Consider Options**: What are 2-3 different ways we could visualize this?
            4. **Select Best Approach**: Which visualization best serves the user's goal?
            5. **Refine Details**: Colors, labels, annotations that enhance understanding
            
            ## Visualization Types You Excel At:
            - **Player Performance**: Radar charts, time series, comparison bars
            - **Team Analysis**: Heat maps, trend lines, comparative charts
            - **League Insights**: Scatter plots, distribution charts, rankings
            - **NIR Breakdowns**: Stacked bars, pie charts, component analysis
            - **Historical Trends**: Line charts, area plots, seasonal comparisons
            
            ## Your Standards:
            - Every chart must tell a clear story
            - Colors and styling should enhance, not distract
            - Labels and titles must be informative and engaging
            - Interactive elements should add value
            - Consider the user's likely follow-up questions
            
            Use your tools to create visualizations that are both beautiful and insightful.
            """,
            tools=[
                self._create_radar_chart,
                self._create_time_series,
                self._create_comparison_bar_chart,
                self._create_scatter_plot,
                self._create_heatmap,
                self._create_distribution_chart,
                self._create_stacked_bar_chart
            ]
        )
    
    @function_tool
    def _create_radar_chart(self, 
                           categories: List[str], 
                           values: List[float], 
                           title: str,
                           entity_name: str = "",
                           comparison_values: Optional[List[float]] = None,
                           comparison_name: str = "League Average") -> Dict[str, Any]:
        """
        Create a radar chart for multi-dimensional performance analysis.
        Perfect for NIR breakdowns, player skill profiles, team strengths.
        """
        try:
            fig = go.Figure()
            
            # Main data series
            fig.add_trace(go.Scatterpolar(
                r=values + [values[0]],  # Close the radar
                theta=categories + [categories[0]],
                fill='toself',
                name=entity_name or 'Performance',
                line_color='rgb(0, 100, 200)',
                fillcolor='rgba(0, 100, 200, 0.3)'
            ))
            
            # Comparison series if provided
            if comparison_values:
                fig.add_trace(go.Scatterpolar(
                    r=comparison_values + [comparison_values[0]],
                    theta=categories + [categories[0]],
                    fill='toself',
                    name=comparison_name,
                    line_color='rgb(200, 100, 0)',
                    fillcolor='rgba(200, 100, 0, 0.2)'
                ))
            
            fig.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=True,
                        range=[0, max(max(values), max(comparison_values) if comparison_values else 0) * 1.1]
                    )
                ),
                showlegend=True,
                title=dict(
                    text=f"<b>{title}</b>",
                    x=0.5,
                    font=dict(size=16)
                ),
                width=600,
                height=500
            )
            
            return {
                "chart_type": "radar",
                "plotly_json": fig.to_json(),
                "description": f"Radar chart showing {entity_name}'s performance across {len(categories)} dimensions"
            }
            
        except Exception as e:
            return {"error": f"Failed to create radar chart: {str(e)}"}
    
    @function_tool
    def _create_time_series(self,
                           dates: List[str],
                           values: List[float],
                           title: str,
                           metric_name: str,
                           entity_name: str = "",
                           trend_line: bool = True) -> Dict[str, Any]:
        """
        Create time series visualization for performance trends over time.
        Perfect for player development, team season progression, league evolution.
        """
        try:
            fig = go.Figure()
            
            # Main time series
            fig.add_trace(go.Scatter(
                x=dates,
                y=values,
                mode='lines+markers',
                name=f"{entity_name} {metric_name}" if entity_name else metric_name,
                line=dict(width=3, color='rgb(0, 100, 200)'),
                marker=dict(size=6, color='rgb(0, 100, 200)')
            ))
            
            # Add trend line if requested
            if trend_line and len(values) > 2:
                # Simple linear trend
                x_numeric = list(range(len(values)))
                z = np.polyfit(x_numeric, values, 1)
                trend_values = [z[0] * x + z[1] for x in x_numeric]
                
                fig.add_trace(go.Scatter(
                    x=dates,
                    y=trend_values,
                    mode='lines',
                    name='Trend',
                    line=dict(dash='dash', color='rgba(200, 100, 0, 0.8)')
                ))
            
            fig.update_layout(
                title=dict(
                    text=f"<b>{title}</b>",
                    x=0.5,
                    font=dict(size=16)
                ),
                xaxis_title="Date",
                yaxis_title=metric_name,
                hovermode='x unified',
                width=800,
                height=400
            )
            
            return {
                "chart_type": "time_series",
                "plotly_json": fig.to_json(),
                "description": f"Time series showing {metric_name} progression over time"
            }
            
        except Exception as e:
            return {"error": f"Failed to create time series: {str(e)}"}
    
    @function_tool
    def _create_comparison_bar_chart(self,
                                   entities: List[str],
                                   values: List[float],
                                   title: str,
                                   metric_name: str,
                                   horizontal: bool = False,
                                   color_scale: str = "blues") -> Dict[str, Any]:
        """
        Create comparison bar chart for ranking and comparative analysis.
        Perfect for top performers, team comparisons, league standings.
        """
        try:
            # Sort for better visualization
            sorted_data = sorted(zip(entities, values), key=lambda x: x[1], reverse=True)
            sorted_entities, sorted_values = zip(*sorted_data)
            
            fig = go.Figure()
            
            if horizontal:
                fig.add_trace(go.Bar(
                    y=sorted_entities,
                    x=sorted_values,
                    orientation='h',
                    marker_color=px.colors.sequential.Blues_r[:len(sorted_entities)]
                ))
                fig.update_layout(
                    xaxis_title=metric_name,
                    yaxis_title="",
                    height=max(400, len(entities) * 30)
                )
            else:
                fig.add_trace(go.Bar(
                    x=sorted_entities,
                    y=sorted_values,
                    marker_color=px.colors.sequential.Blues_r[:len(sorted_entities)]
                ))
                fig.update_layout(
                    xaxis_title="",
                    yaxis_title=metric_name,
                    height=400,
                    xaxis_tickangle=-45 if len(max(sorted_entities, key=len)) > 10 else 0
                )
            
            fig.update_layout(
                title=dict(
                    text=f"<b>{title}</b>",
                    x=0.5,
                    font=dict(size=16)
                ),
                showlegend=False,
                width=800
            )
            
            return {
                "chart_type": "bar_comparison",
                "plotly_json": fig.to_json(),
                "description": f"Bar chart comparing {len(entities)} entities by {metric_name}"
            }
            
        except Exception as e:
            return {"error": f"Failed to create bar chart: {str(e)}"}
    
    @function_tool
    def _create_scatter_plot(self,
                           x_values: List[float],
                           y_values: List[float],
                           labels: List[str],
                           title: str,
                           x_label: str,
                           y_label: str,
                           size_values: Optional[List[float]] = None) -> Dict[str, Any]:
        """
        Create scatter plot for correlation analysis and player/team positioning.
        Perfect for efficiency analysis, performance correlation, market value analysis.
        """
        try:
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=x_values,
                y=y_values,
                mode='markers+text',
                text=labels,
                textposition="top center",
                marker=dict(
                    size=size_values if size_values else [10] * len(x_values),
                    color=x_values,  # Color by x-axis value
                    colorscale='viridis',
                    showscale=True,
                    sizemode='diameter',
                    sizeref=2. * max(size_values) / (40.**2) if size_values else 1,
                    sizemin=4
                ),
                hovertemplate=
                f'<b>%{{text}}</b><br>' +
                f'{x_label}: %{{x}}<br>' +
                f'{y_label}: %{{y}}<br>' +
                '<extra></extra>'
            ))
            
            # Add trend line
            if len(x_values) > 2:
                z = np.polyfit(x_values, y_values, 1)
                trend_line = [z[0] * x + z[1] for x in sorted(x_values)]
                
                fig.add_trace(go.Scatter(
                    x=sorted(x_values),
                    y=trend_line,
                    mode='lines',
                    name='Trend',
                    line=dict(dash='dash', color='red'),
                    showlegend=True
                ))
            
            fig.update_layout(
                title=dict(
                    text=f"<b>{title}</b>",
                    x=0.5,
                    font=dict(size=16)
                ),
                xaxis_title=x_label,
                yaxis_title=y_label,
                width=800,
                height=600
            )
            
            return {
                "chart_type": "scatter",
                "plotly_json": fig.to_json(),
                "description": f"Scatter plot analyzing relationship between {x_label} and {y_label}"
            }
            
        except Exception as e:
            return {"error": f"Failed to create scatter plot: {str(e)}"}
    
    @function_tool
    def _create_stacked_bar_chart(self,
                                entities: List[str],
                                components: Dict[str, List[float]],
                                title: str,
                                total_label: str = "Total") -> Dict[str, Any]:
        """
        Create stacked bar chart for component analysis.
        Perfect for NIR breakdowns, goal contribution analysis, tactical role distribution.
        """
        try:
            fig = go.Figure()
            
            colors = px.colors.qualitative.Set3
            
            for i, (component_name, values) in enumerate(components.items()):
                fig.add_trace(go.Bar(
                    name=component_name,
                    x=entities,
                    y=values,
                    marker_color=colors[i % len(colors)]
                ))
            
            fig.update_layout(
                barmode='stack',
                title=dict(
                    text=f"<b>{title}</b>",
                    x=0.5,
                    font=dict(size=16)
                ),
                xaxis_title="",
                yaxis_title=total_label,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1
                ),
                width=800,
                height=500,
                xaxis_tickangle=-45 if any(len(entity) > 10 for entity in entities) else 0
            )
            
            return {
                "chart_type": "stacked_bar",
                "plotly_json": fig.to_json(),
                "description": f"Stacked bar chart showing component breakdown for {len(entities)} entities"
            }
            
        except Exception as e:
            return {"error": f"Failed to create stacked bar chart: {str(e)}"}

    async def create_intelligent_visualization(self, 
                                             user_query: str,
                                             data: Dict[str, Any],
                                             context: str) -> Dict[str, Any]:
        """
        Main method: Use agent reasoning to create optimal visualization.
        
        Args:
            user_query: What the user wants to understand
            data: Available NWSL analytics data
            context: Type of analysis (player, team, match, etc.)
        
        Returns:
            Complete visualization with Plotly JSON, explanations, and insights
        """
        try:
            if not self.visualization_agent:
                # Fallback to rule-based visualization
                return self._fallback_visualization(user_query, data, context)
            
            # Construct prompt for the agent
            agent_prompt = f"""
            User Query: {user_query}
            
            Available Data: {json.dumps(data, indent=2)}
            
            Context: {context}
            
            Please analyze this NWSL data and create the most compelling visualization that answers
            the user's question. Consider what story the data tells and how to present it most effectively.
            
            Walk through your reasoning:
            1. What is the user trying to understand?
            2. What are the key insights in this data?
            3. What visualization type best reveals these insights?
            4. How can we make this visualization engaging and informative?
            
            Use your visualization tools to create the chart.
            """
            
            # Run the agent
            result = await Runner.run_async(self.visualization_agent, agent_prompt)
            
            return {
                "visualization": result,
                "reasoning": "Agent-generated visualization with multi-step reasoning",
                "user_query": user_query,
                "success": True
            }
            
        except Exception as e:
            logger.error(f"Error in intelligent visualization: {e}")
            return {
                "error": str(e),
                "fallback": self._fallback_visualization(user_query, data, context)
            }
    
    def _fallback_visualization(self, user_query: str, data: Dict[str, Any], context: str) -> Dict[str, Any]:
        """Fallback rule-based visualization when agent is unavailable"""
        
        try:
            # NIR breakdown charts
            if "nir" in user_query.lower() or "breakdown" in user_query.lower():
                nir_breakdown = data.get('nir_breakdown', {})
                if nir_breakdown:
                    return self._create_radar_chart(
                        categories=list(nir_breakdown.keys()),
                        values=list(nir_breakdown.values()),
                        title="NWSL Impact Rating Breakdown",
                        entity_name=data.get('entity_id', 'Entity')
                    )
            
            # Comparison charts
            elif "comparison" in user_query.lower() or "vs" in user_query.lower():
                # Look for team analytics comparison data
                if 'teams' in data and isinstance(data['teams'], dict):
                    team_names = list(data['teams'].keys())
                    if len(team_names) >= 2:
                        team1_nir = data['teams'][team_names[0]].get('nir_breakdown', {})
                        team2_nir = data['teams'][team_names[1]].get('nir_breakdown', {})
                        if team1_nir and team2_nir:
                            return self._create_radar_chart(
                                categories=list(team1_nir.keys()),
                                values=list(team1_nir.values()),
                                title=f"{team_names[0]} vs {team_names[1]} - NIR Comparison",
                                entity_name=team_names[0],
                                comparison_values=list(team2_nir.values()),
                                comparison_name=team_names[1]
                            )
            
            # Season/league data - create bar chart of top teams
            elif any(word in user_query.lower() for word in ["season", "league", "teams", "top"]):
                if 'top_teams' in data:
                    teams_data = data['top_teams']
                    if isinstance(teams_data, list) and teams_data:
                        teams = [team.get('team_name', f'Team {i}') for i, team in enumerate(teams_data)]
                        goals = [team.get('total_goals', 0) for team in teams_data]
                        return self._create_comparison_bar_chart(
                            entities=teams,
                            values=goals,
                            title="Top Scoring Teams",
                            metric_name="Goals Scored"
                        )
                
                # Fallback for any numeric data in season summary
                elif 'season_summary' in data:
                    summary = data['season_summary']
                    if 'top_teams' in summary:
                        teams_info = summary['top_teams']
                        if isinstance(teams_info, dict):
                            teams = list(teams_info.keys())
                            values = list(teams_info.values())
                            return self._create_comparison_bar_chart(
                                entities=teams,
                                values=values,
                                title="Season Leaders",
                                metric_name="Performance Score"
                            )
            
            # Player performance data
            elif "player" in user_query.lower() or "performance" in user_query.lower():
                if 'nir_breakdown' in data:
                    nir_breakdown = data['nir_breakdown']
                    return self._create_radar_chart(
                        categories=list(nir_breakdown.keys()),
                        values=list(nir_breakdown.values()),
                        title="Player Performance Profile",
                        entity_name=data.get('entity_id', 'Player')
                    )
            
            # Default: try to find any plottable data
            # Look for lists of numbers or simple key-value pairs
            plottable_data = None
            for key, value in data.items():
                if isinstance(value, dict) and len(value) > 2:
                    # Found a dictionary with multiple entries - could be categorical data
                    categories = list(value.keys())
                    values = []
                    for v in value.values():
                        if isinstance(v, (int, float)):
                            values.append(v)
                        else:
                            break
                    else:
                        # All values are numeric
                        if len(values) >= 3:  # Need at least 3 points for a good chart
                            return self._create_comparison_bar_chart(
                                entities=categories,
                                values=values,
                                title="Data Visualization",
                                metric_name=key.replace('_', ' ').title()
                            )
            
            # Last resort: return informative message with data structure
            return {
                "chart_type": "info",
                "message": "Visualization agent in fallback mode - basic chart generation available",
                "note": "For advanced AI-powered visualizations, OpenAI Agents SDK integration needed",
                "data_structure": {k: type(v).__name__ for k, v in data.items()},
                "suggested_queries": [
                    "Use 'nir breakdown' for radar charts",
                    "Use 'comparison' for team vs team analysis", 
                    "Specify 'player' or 'team' for focused analysis"
                ]
            }
            
        except Exception as e:
            return {"error": f"Fallback visualization failed: {str(e)}"}

# Helper imports (numpy for trend lines)
try:
    import numpy as np
except ImportError:
    # Fallback for basic operations
    class MockNumpy:
        @staticmethod
        def polyfit(x, y, degree):
            return [0, sum(y) / len(y)]  # Simple average as fallback
    np = MockNumpy()

logger.info("🎨 NWSL Data Visualization Agent initialized!")
logger.info("📊 Supports: Radar charts, time series, comparisons, scatter plots, heatmaps, distributions")