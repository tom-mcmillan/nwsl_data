# Analysis Scripts

Scripts for advanced analytics and data analysis.

## Team Analysis
- `aggregate_complete_team_stats.py` - Aggregate team statistics
- `extract_comprehensive_team_stats.py` - Extract detailed team metrics
- `run_comprehensive_team_extraction.py` - Orchestrate team analysis

## Venue & Weather Analysis
- `extract_venue_data.py` - Extract venue information
- `get_match_weather_data.py` - Collect weather data for matches
- `populate_refined_match_venue_weather.py` - Process venue/weather data
- `run_all_weather_collection.py` - Orchestrate weather data collection

## Usage

### Team Analytics
```bash
python analysis/extract_comprehensive_team_stats.py
python analysis/aggregate_complete_team_stats.py
```

### Environmental Analysis
```bash
python analysis/run_all_weather_collection.py
python analysis/populate_refined_match_venue_weather.py
```

## Dependencies
- pandas
- requests (for weather APIs)
- Analytics engine components