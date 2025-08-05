# Scripts Directory

This directory contains all processing and analysis scripts for the NWSL data project, organized by functionality.

## Directory Structure

### Core Processing Scripts (Root Level)
Essential scripts for data processing and analysis:

- `populate_match_player_summary_2018.py` - **Core extraction engine** for processing individual matches
- `populate_match_player_summary.py` - Alternative processor for modern format matches
- `html_player_stats_extractor.py` - Extract player statistics from HTML files
- `html_team_stats_extractor.py` - Extract team statistics from HTML files
- `fbref_scraper.py` - Web scraping utilities for FBRef data
- `master_scraper.py` - Comprehensive scraping coordinator

### `seasonal_processors/`
Complete season processing scripts that extract statistical data for entire NWSL seasons:

- `process_2013_season.py` - Process all 91 matches from inaugural 2013 season
- `process_2014_season.py` - Process all 111 matches from 2014 season  
- `process_2015_season.py` - Process all 93 matches from 2015 season
- `process_2016_season.py` - Process all 103 matches from 2016 season
- `process_2017_season.py` - Process all 123 matches from 2017 season
- `process_2018_season.py` - Process all 111 matches from 2018 season
- `process_2019_season.py` - Process all 111 matches from 2019 season
- `process_2020_season.py` - Process all 41 matches from 2020 season
- `process_2021_season.py` - Process all 144 matches from 2021 season
- `process_2022_season.py` - Process all 176 matches from 2022 season
- `process_2014_complete.py` - Complete processor with corrected 2014 match IDs

### `html_checkers/`
Validation scripts that verify HTML file availability before processing seasons:

- `check_2013_html_files.py` through `check_2021_html_files.py`
- Each script validates HTML file availability for its respective season
- Reports coverage percentages and identifies missing files

### `batch_scripts/`
Historical batch processing scripts used during database construction:

- `batch_player_extraction.py` - Extract player data in batches
- `batch_roster_YYYY_season.py` - Season-specific roster extraction
- `batch_roster_*.py` - Various batch processing utilities for roster data
- These scripts represent the historical development process

### `temp_scripts/`
One-off utilities, debugging tools, and temporary processing scripts:

- Data fixing scripts: `fix_*.py`, `apply_*.py`
- Debugging utilities: `debug_*.py`, `analyze_*.py`  
- Database utilities: `create_*.py`, `database_inserter.py`
- Extraction experiments: `extract_*.py`, `comprehensive_*.py`
- Player ID management: `identify_*.py`, `add_missing_players.py`

### Analysis & Utilities (Root Level)
Scripts for database analysis and maintenance:

- `analyze_match_completeness.py` - Analyze database completion rates
- `aggregate_complete_team_stats.py` - Generate team performance aggregates
- `extract_comprehensive_team_stats.py` - Extract detailed team statistics
- `get_match_weather_data.py` - Weather data collection
- `import_city_data.py` - Geographic data import
- `scrape_player_data.py` - Player profile scraping

## Usage

### Season Processing
```bash
# Check HTML file availability first
python scripts/html_checkers/check_2023_html_files.py

# Process a complete season
python scripts/seasonal_processors/process_2023_season.py

# Process individual match
python scripts/populate_match_player_summary_2018.py [match_id]
```

### Core Dependencies
All processing scripts depend on:
- Database connection to `data/processed/nwsldata.db`
- HTML files located at `/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files/`
- Python packages listed in `requirements.txt`

## Format Compatibility

### Legacy Format (2013-2018)
- **24 statistical fields**
- Uses `populate_match_player_summary_2018.py` engine
- Handles age parsing in "YY-DDD" format
- Compatible seasons: 2013, 2014, 2015, 2016, 2017, 2018

### Modern Format (2019-2025)  
- **37 statistical fields**
- Enhanced data structure with additional metrics
- Direct processing compatibility
- Compatible seasons: 2019, 2020, 2021, 2022, 2023, 2024, 2025

## Processing Results

### Success Metrics
- **Processing Speed**: ~35 records/second average
- **Match Success Rate**: 100% for seasons with available HTML files
- **Data Integrity**: Comprehensive validation and error handling

### Output Format
Each processor reports:
- Total matches processed
- Records updated count
- Processing time and speed
- Failed matches (if any)

## Script Categories

### Production Scripts
Core scripts used for ongoing data processing:
- `seasonal_processors/` - Complete season processing
- `html_checkers/` - Data validation
- Core extraction engines

### Development Scripts  
Historical and experimental scripts:
- `batch_scripts/` - Historical batch processing
- `temp_scripts/` - Debugging and one-off utilities

### Analysis Scripts
Scripts for database analysis and reporting:
- Completion analysis tools
- Statistical aggregation utilities
- Data quality validation

This organized structure reflects the evolution from ad-hoc processing to systematic, reusable season processors, with clear separation between production scripts and development utilities.