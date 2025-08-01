# Database Schema Updates - Team Season Name Table

## Changes Made

### 1. Added match scores to match table
- Added `home_goals` and `away_goals` columns computed from `match_team_summary`
- Populated historical match scores manually (97 missing records)

### 2. Created team_season_name table
- **Purpose**: Single source of truth for team-season relationships
- **Primary Key**: `tsn_id` (tsn_ + 8-character hex)
- **Schema**:
  ```sql
  CREATE TABLE team_season_name (
      tsn_id TEXT PRIMARY KEY,
      season_id INTEGER NOT NULL,
      team_id TEXT NOT NULL,
      team_name_season_1 TEXT,
      team_name_season_2 TEXT,
      UNIQUE(season_id, team_id),
      FOREIGN KEY (season_id) REFERENCES season(season_id),
      FOREIGN KEY (team_id) REFERENCES team(team_id)
  );
  ```
- **Records**: 135 team-season combinations (2013-2025)

### 3. Updated team table column names
- Renamed columns to: `team_name_1`, `team_name_2`, `team_name_3`, `team_name_4`

## Next Steps
- Create `team_record` table for season standings
- Populate historical team records from match data
- Update MCP server with new standings functionality

Date: $(date)
