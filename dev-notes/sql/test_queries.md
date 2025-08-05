# NWSL MCP Server - Test Queries for OpenAI Playground

## Setup Instructions

1. Install MCP dependencies:
   ```bash
   pip install mcp>=1.0.0
   ```

2. Test the server locally:
   ```bash
   python -m src.server
   ```

3. For OpenAI Playground integration, add this configuration to your MCP settings:
   ```json
   {
     "mcpServers": {
       "nwsl-data": {
         "command": "python",
         "args": ["-m", "src.server"],
         "env": {}
       }
     }
   }
   ```

## Available Tools

### 1. `query_player_stats`
Get detailed statistics for a specific player
- **player_name** (required): Name of the player
- **season** (optional): Season year (e.g., 2024)

### 2. `query_team_performance` 
Get team performance data and statistics
- **team_name** (required): Name of the team
- **season** (optional): Season year

### 3. `query_match_data`
Get detailed match information and results
- **team1** (required): First team name
- **team2** (optional): Second team name for head-to-head
- **season** (optional): Season year
- **limit** (optional): Number of matches to return (default: 10)

### 4. `query_league_standings`
Get league standings and team rankings
- **season** (required): Season year
- **match_type** (optional): Match type filter (default: "Regular Season")

### 5. `search_players`
Search for players by name or partial name
- **search_term** (required): Player name or partial name
- **limit** (optional): Number of results (default: 10)

### 6. `get_season_overview`
Get overview statistics for a specific season
- **season** (required): Season year

## Example Test Queries for OpenAI Playground

### Player Queries
```
Show me Megan Rapinoe's career statistics across all seasons
```

```
What were Alex Morgan's stats in the 2019 season?
```

```
Find all players with "Smith" in their name
```

### Team Queries
```
How did Angel City FC perform in their inaugural 2022 season?
```

```
Show me Portland Thorns' performance over the last 3 seasons
```

```
Compare Seattle Reign and OL Reign performance data
```

### Match Queries
```
Show me the last 5 matches between Portland Thorns and Seattle Reign
```

```
What were the highest-scoring games in the 2023 season?
```

```
Show me all playoff matches from 2024
```

### League Analysis
```
What were the 2024 NWSL regular season standings based on expected goals?
```

```
Give me an overview of the 2023 NWSL season
```

```
Which teams had the best attacking performance in 2022?
```

### Advanced Queries
```
Who were the top scorers in NWSL history?
```

```
Which venues have hosted the most NWSL matches?
```

```
Show me teams that performed better than expected based on xG
```

## Database Coverage
- **Years**: 2013-2025
- **Total Matches**: 1,563
- **Total Players**: 946
- **Teams**: 17
- **Data includes**: Match results, player statistics, team performance, expected goals (xG), venues, and more

## Tips for Effective Queries
1. Use partial team/player names if unsure of exact spelling
2. Specify seasons for more focused results
3. Use the search tools first to find correct names
4. Combine multiple tools for comprehensive analysis
5. Ask follow-up questions to drill down into specific areas

## Common Team Names to Use
- Portland Thorns, Thorns
- Seattle Reign, OL Reign
- Angel City FC, ACFC
- North Carolina Courage, Courage
- Orlando Pride, Pride
- Houston Dash, Dash
- Chicago Red Stars
- Washington Spirit
- San Diego Wave FC, Wave
- Utah Royals
- Gotham FC
- Boston Breakers (historical)
- Western New York Flash (historical)