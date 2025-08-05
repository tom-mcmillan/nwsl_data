# MCP Server Architecture

The NWSL Advanced Analytics platform implements a sophisticated Model Context Protocol (MCP) server that provides intelligent sports analytics through advanced composite metrics and AI-powered visualization capabilities.

## Overview

Our MCP server transforms traditional sports statistics into actionable intelligence using sabermetrics-inspired analytics, context-aware visualization generation, and predictive performance indicators. Built with FastMCP and deployed on Google Cloud Run, it provides remote HTTP access following MCP best practices.

=== "Core Architecture"

    ```python
    # Main MCP Server Implementation
    from mcp.server.fastmcp import FastMCP
    from .nwsl_analytics_engine import NWSLAnalyticsEngine
    from .simple_chart_generator import SimpleChartGenerator
    from .intelligent_visualization_agent import IntelligentVisualizationAgent

    # Initialize unified analytics intelligence system
    mcp = FastMCP("NWSL Advanced Analytics Intelligence")
    analytics_engine = NWSLAnalyticsEngine(str(DB_PATH))
    visualization_agent = IntelligentVisualizationAgent()
    simple_chart_generator = SimpleChartGenerator(str(DB_PATH))
    ```

=== "OpenAI Integration"

    ```python
    # OpenAI Agents SDK Integration with Fallback
    try:
        from openai_agents import Agent, function_tool, Runner
        AGENTS_AVAILABLE = True
    except ImportError:
        AGENTS_AVAILABLE = False
        # Graceful fallback without complex dependencies
    ```

=== "Cloud Deployment"

    ```python
    # Cloud Run HTTP Server Setup
    if __name__ == "__main__":
        port = int(os.environ.get("PORT", 8000))
        app = mcp.streamable_http_app()  # FastMCP HTTP streaming
        
        uvicorn.run(
            app,
            host="0.0.0.0",  # Required for Cloud Run
            port=port,
            log_level="info"
        )
    ```

## Advanced Analytics Engine

The server implements sophisticated composite metrics beyond traditional counting statistics, inspired by baseball sabermetrics research.

!!! info "NWSL Impact Rating (NIR)"
    Our proprietary composite metric that evaluates player/team performance across multiple dimensions:
    
    - **Attacking Impact**: Goal creation and finishing efficiency
    - **Defensive Impact**: Defensive actions and organization 
    - **Progression Impact**: Ball advancement and build-up play contribution

### Implementation Details

```python
@mcp.tool()
def analyze_team_intelligence(team_name: str, season_id: int) -> str:
    """Advanced team intelligence with NWSL Impact Rating (NIR), 
    predictive indicators, and tactical analysis."""
    
    context = AnalyticsContext(season_id=season_id)
    result = analytics_engine.calculate_advanced_metrics(
        EntityType.TEAM, team_name, context
    )
    
    # Context-adjusted performance analysis
    basic_context = db_context.get_teams_in_season(season_id)
    result["database_context"] = basic_context
    
    return safe_json_response(result)
```

## Intelligent Visualization System

The server provides multiple approaches to data visualization, from simple chart generation to sophisticated AI-powered visualization selection.

=== "Simple Chart Generation"

    ```python
    @mcp.tool()
    def create_chart(user_request: str) -> str:
        """Simple, direct chart generation that actually works.
        
        No complex reasoning - just: Request → Data → Plotly Chart → Result
        """
        chart_result = simple_chart_generator.generate_chart(user_request)
        
        result = {
            "success": True,
            "plotly_json": chart_result.get("plotly_json"),
            "methodology": "Direct database query → Plotly chart generation"
        }
        
        return safe_json_response(result)
    ```

=== "AI-Powered Visualization"

    ```python
    @mcp.tool()
    def create_contextual_visualization(
        user_intent: str,
        conversation_context: str = "",
        visualization_preferences: str = "interactive"
    ) -> str:
        """Context-aware AI visualization agent following MCP best practices."""
        
        # Parse conversation context into structured data
        context_messages = conversation_context.split('\n') if conversation_context else [user_intent]
        
        # Use intelligent visualization agent with context awareness
        visualization_result = asyncio.run(
            intelligent_viz_agent.create_intelligent_visualization(
                user_query=user_intent,
                conversation_context=context_messages
            )
        )
        
        return safe_json_response(visualization_result)
    ```

## MCP Best Practices Implementation

Our server follows OpenAI's MCP best practices for creating high-quality, production-ready tools.

### High-Level Tool Design

!!! tip "MCP Guidance: Group Related Tasks"
    Instead of exposing multiple low-level visualization functions, we provide high-level tools that handle complex workflows internally.

```python
# ❌ Poor MCP Design - Too Many Low-Level Tools
@mcp.tool()
def get_team_goals(): pass

@mcp.tool() 
def create_bar_chart(): pass

@mcp.tool()
def add_chart_styling(): pass

# ✅ Good MCP Design - High-Level Workflow Tool
@mcp.tool()
def create_contextual_visualization(user_intent, conversation_context): 
    """Complete visualization workflow in a single tool"""
    pass
```

### Context Awareness

The server implements sophisticated context parsing to extract structured data from conversation history:

```python
class NWSLContextParser:
    """Intelligent parser that extracts structured NWSL data from conversation context."""
    
    def parse_conversation_context(self, messages: List[str]) -> ExtractedData:
        full_context = " ".join(messages)
        
        # Extract season, team data, metrics
        season = self._extract_season(full_context)
        team_data = self._extract_team_data(full_context)
        metrics = self._extract_metrics(full_context)
        
        # Generate visualization hints
        viz_hints = self._generate_visualization_hints(full_context, data_type, team_data)
        
        return ExtractedData(
            data_type=data_type,
            entities=list(team_data.keys()),
            metrics=metrics,
            context=full_context,
            season=season,
            visualization_hints=viz_hints
        )
```

### Graceful Degradation

The server implements multiple fallback layers to ensure reliability:

```python
async def create_intelligent_visualization(self, user_query: str, conversation_context: List[str]):
    try:
        # Main path: Use advanced intelligent agent
        if self.reasoning_agent and AGENTS_AVAILABLE:
            return await self._agent_reasoning_path(user_query, extracted_data)
    except Exception as agent_error:
        logger.warning(f"Intelligent agent failed, trying fallback: {agent_error}")
    
    try:
        # Fallback path: Rule-based chart selection
        return self._fallback_reasoning_path(user_query, extracted_data)
    except Exception as fallback_error:
        # Final fallback: Structured guidance response
        return {"suggestion": "Try describing specific data you'd like visualized"}
```

## Performance Characteristics

The MCP server is optimized for production deployment with comprehensive error handling and monitoring.

### Database Integration

```python
# Efficient database connection management
def safe_json_response(data: Any) -> str:
    """Safely convert data to JSON string with error handling"""
    try:
        return json.dumps(data, indent=2, default=str)
    except Exception as e:
        return json.dumps({"error": f"JSON serialization failed: {str(e)}"})

# Connection pooling and query optimization
with sqlite3.connect(str(DB_PATH)) as conn:
    df = pd.read_sql_query(query, conn, params=[season_id, limit])
```

### Logging and Monitoring

```python
# Structured logging for production monitoring
logging.basicConfig(level=logging.ERROR, format='%(levelname)s: %(message)s')
logger = logging.getLogger("nwsl-mcp-server")

logger.info("🚀 Starting NWSL Advanced Analytics Intelligence Server")
logger.info("💡 Powered by sabermetrics-inspired composite metrics")
logger.info("🧠 Advanced Analytics Intelligence Engine initialized")
```

## Tool Catalog

The server exposes 15+ sophisticated tools covering the complete NWSL analytics workflow:

| Tool Category | Tools | Purpose |
|---------------|-------|---------|
| **Database Context** | `get_database_overview`, `search_team_names`, `get_season_summary` | Data exploration and validation |
| **Advanced Analytics** | `analyze_team_intelligence`, `analyze_player_intelligence`, `compare_team_intelligence` | NIR-based performance analysis |
| **Visualization** | `create_chart`, `create_contextual_visualization`, `create_player_performance_radar` | Interactive Plotly chart generation |
| **Historical Analysis** | `analyze_historical_matchup_intelligence`, `analyze_match_intelligence` | Temporal pattern analysis |
| **Performance Leaders** | `analyze_season_performance_leaders` | Ranking by sophisticated metrics |

### Example Tool Response

```json
{
  "analysis_type": "Advanced Team Intelligence",
  "season": 2025,
  "nir_score": 0.847,
  "nir_breakdown": {
    "attacking_impact": 0.312,
    "defensive_impact": 0.285,
    "progression_impact": 0.250
  },
  "predictive_indicators": {
    "season_trajectory": "Positive momentum",
    "key_strengths": ["Goal conversion efficiency", "Defensive organization"],
    "development_areas": ["Set piece creation", "Deep build-up play"]
  },
  "tactical_profile": {
    "playing_style": "Balanced attacking with strong defensive structure",
    "formation_tendency": "4-3-3 / 4-2-3-1 hybrid",
    "key_tactical_elements": ["High pressing", "Quick transitions", "Wide play emphasis"]
  }
}
```

## Deployment Architecture

The server is designed for cloud-native deployment with horizontal scaling capabilities:

```dockerfile
# Cloud Run Deployment Configuration
FROM python:3.11-slim
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["python", "src/server.py"]
```

Environment variables handle configuration:
- `PORT`: Server port (default: 8000)
- `DATABASE_PATH`: SQLite database location
- `LOG_LEVEL`: Logging verbosity

## Future Enhancements

The MCP server architecture supports extensibility for advanced features:

- **Real-time Data Integration**: Live match data streaming
- **Machine Learning Pipeline**: Predictive modeling for match outcomes
- **Multi-League Support**: Expansion beyond NWSL
- **Advanced Visualizations**: 3D tactical analysis, heat maps
- **API Rate Limiting**: Production-grade request throttling

---

!!! success "Production Ready"
    This MCP server implementation demonstrates enterprise-grade architecture with sophisticated analytics, intelligent visualization, and production deployment capabilities. The combination of advanced metrics, context-aware tools, and graceful degradation makes it a robust foundation for sports analytics applications.