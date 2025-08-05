# Data Processing Scripts

Scripts for transforming and processing raw NWSL data.

## Match Processing
- `create_match_team_from_players.py` - Generate team data from player records
- `populate_match_player_summary.py` - Create match-player summary records
- `populate_match_player_summary_2018.py` - 2018-specific processing

## Team Processing
- `create_minimal_team_summary.py` - Generate basic team summaries
- `create_team_summary_from_csvs.py` - Process CSV data into team summaries

## Usage

### Match Data Processing
```bash
python data-processing/create_match_team_from_players.py
python data-processing/populate_match_player_summary.py
```

### Team Data Processing
```bash
python data-processing/create_team_summary_from_csvs.py
python data-processing/create_minimal_team_summary.py
```

## Dependencies
- pandas
- sqlite3
- Custom data models