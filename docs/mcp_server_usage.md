# NWSL Data MCP Server Usage Guide

## Overview
This MCP server provides comprehensive access to NWSL (National Women's Soccer League) data from 2013-2025, including player statistics, team performance, match results, and league standings.

## Available Tools

### 1. `search_players`
Search for NWSL players by name or partial name.

**Parameters:**
- `search_term` (string): Player name or partial name to search for
- `limit` (int, optional): Number of results to return (default: 10, max: 50)

**Example:**
```
Search for "Morgan" → Returns players like Alex Morgan, etc.
```

### 2. `get_player_stats`
Get detailed statistics for a specific NWSL player.

**Parameters:**
- `player_name` (string): Name of the player to query
- `season` (int, optional): Season year (2013-2025). If not provided, returns all seasons

**Example:**
```
get_player_stats("Alex Morgan", 2025) → Returns Alex Morgan's 2025 stats
```

### 3. `get_team_performance`
Get team performance data and expected goals statistics.

**Parameters:**
- `team_name` (string): Name of the team (e.g., "Courage", "Angel City", "Thorns")
- `season` (int, optional): Season year. If not provided, returns last 5 seasons

**Example:**
```
get_team_performance("Courage", 2025) → Returns North Carolina Courage's 2025 performance
```

### 4. `get_league_standings`
Get NWSL league standings ranked by points and goal differential.

**Parameters:**
- `season` (int): Season year (2013-2025)

**Example:**
```
get_league_standings(2025) → Returns 2025 NWSL standings table
```

### 5. `get_season_overview`
Get comprehensive overview statistics for a specific NWSL season.

**Parameters:**
- `season` (int): Season year (2013-2025)

**Example:**
```
get_season_overview(2025) → Returns games played, teams, avg goals, etc.
```

### 6. `get_match_results`
Get detailed match information and results.

**Parameters:**
- `team1` (string): First team name (required)
- `team2` (string, optional): Second team name for head-to-head matchups
- `season` (int, optional): Season year filter
- `limit` (int, optional): Number of matches to return (default: 10, max: 50)

**Examples:**
```
get_match_results("Courage") → Returns recent Courage matches
get_match_results("Courage", "Thorns") → Returns head-to-head matches
get_match_results("Courage", season=2025) → Returns Courage's 2025 matches
```

### 7. `get_top_performers`
Get top performing players by various statistical categories.

**Parameters:**
- `season` (int): Season year (2013-2025)
- `category` (string): Statistical category - 'goals', 'assists', 'minutes', or 'games'
- `limit` (int, optional): Number of players to return (default: 10, max: 50)

**Examples:**
```
get_top_performers(2025, "goals") → Returns top goal scorers in 2025
get_top_performers(2025, "assists", 15) → Returns top 15 assist leaders
```

### 8. `get_team_roster`
Get current roster for a specific team and season.

**Parameters:**
- `team_name` (string): Name of the team
- `season` (int): Season year (2013-2025)

**Example:**
```
get_team_roster("Courage", 2025) → Returns North Carolina Courage's 2025 roster
```

### 9. `list_teams`
List all teams that participated in a given season.

**Parameters:**
- `season` (int, optional): Season year. If not provided, shows current season (2025)

**Example:**
```
list_teams(2025) → Returns all teams in 2025 season
```

### 10. `analyze_season` ⭐ **NEW**
Comprehensive season analysis with narrative insights and context.

**Parameters:**
- `season` (int): Season year (2013-2025)

**Example:**
```
analyze_season(2025) → Rich analysis: "📊 2025 NWSL Season Analysis... Current dominated with 33 points..."
```

### 11. `compare_seasons` ⭐ **NEW**
Compare two NWSL seasons with key differences highlighted.

**Parameters:**
- `season1` (int): First season year (2013-2025)
- `season2` (int): Second season year (2013-2025)

**Example:**
```
compare_seasons(2024, 2025) → "⚖️ Season Comparison: League growth, scoring trends, competitiveness..."
```

### 12. `get_data_status` ⭐ **NEW**
Show what NWSL data is available and suggest optimal queries.

**Returns:**
Overview of data availability and recommended queries

**Example:**
```
get_data_status() → "📋 NWSL Data Availability Status... Current season has 91 games..."
```

## Team Name Reference

For 2025 season, use these team names:
- Angel City
- Bay FC
- Courage (North Carolina Courage)
- Current (Kansas City Current)
- Dash (Houston Dash)
- Gotham FC (NJ/NY Gotham FC)
- Louisville (Racing Louisville FC)
- Pride (Orlando Pride)
- Reign (Seattle Reign FC)
- Royals (Utah Royals)
- Spirit (Washington Spirit)
- Stars (San Diego Wave FC)
- Thorns (Portland Thorns FC)
- Wave (San Diego Wave FC)

## Usage with Claude Desktop

Add this server to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "nwsl-data": {
      "command": "python",
      "args": ["-m", "src.server"]
    }
  }
}
```

## Usage with OpenAI Responses API

```python
from openai import OpenAI

client = OpenAI()

resp = client.responses.create(
    model="gpt-4.1",
    tools=[{
        "type": "mcp",
        "server_label": "nwsl-data", 
        "server_url": "https://your-server-url.com/mcp",
        "require_approval": "never"
    }],
    input="What are the 2025 NWSL league standings?"
)
```

## Performance Features

- **Caching**: Season overviews and league standings are cached for better performance
- **Input Validation**: All inputs are validated with helpful error messages
- **Team Name Matching**: Intelligent team name matching supports various formats
- **Error Handling**: Comprehensive error handling with meaningful messages
- **Live Calculations**: Standings calculated from match data when needed
- **Intelligent Fallbacks**: Graceful handling when data is missing

## Data Coverage

- **Seasons**: 2013-2025 (complete)
- **Teams**: All NWSL teams across all seasons
- **Statistics**: Goals, assists, minutes played, expected goals (xG), team performance
- **Match Data**: Complete match results, scores, expected goals

## Common Usage Patterns

### 🔥 **Start with Analytical Tools** (Recommended)
1. **Season Overview**: `analyze_season(2025)` - Get narrative insights and context
2. **Compare Seasons**: `compare_seasons(2024, 2025)` - Understand trends and changes
3. **Check Data**: `get_data_status()` - Know what queries work best

### 📊 **Traditional Deep Dives**
4. **Team Analysis**: `get_team_performance("Courage", 2025)` → `get_match_results("Courage", season=2025)`
5. **Player Research**: `search_players("Morgan")` → `get_player_stats("Alex Morgan", 2025)`
6. **League Context**: `get_league_standings(2025)` → `get_top_performers(2025, "goals")`

### 💬 **Conversational Queries**
The analytical tools are designed for natural conversation:
- "Tell me about the 2025 season" → `analyze_season(2025)`
- "How does 2025 compare to 2024?" → `compare_seasons(2024, 2025)`
- "What data do you have?" → `get_data_status()`

## Error Messages & Fallbacks

The server provides helpful error messages for:
- Invalid season years (must be 2013-2025)
- Unknown team names (suggests alternatives)
- Invalid limits (must be 1-50)
- Missing data for specific queries

**Smart Fallbacks:**
- When pre-calculated standings are missing, calculates from match data
- When player data is unavailable, suggests team-level queries
- When queries fail, provides alternative suggestions
- Always includes follow-up query recommendations

## Support

For issues or questions about the NWSL Data MCP Server, check the database integrity and ensure all required data files are present.