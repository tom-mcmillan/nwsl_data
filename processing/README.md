# Processing Scripts Directory

This directory contains organized scripts for processing NWSL match data and extracting statistical information from HTML files.

## Directory Structure

### `seasonal_processors/`
Season-specific processing scripts that extract complete statistical data for entire NWSL seasons.

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
Validation scripts that verify HTML file availability before processing seasons.

- `check_2013_html_files.py` through `check_2021_html_files.py`
- Each script validates HTML file availability for its respective season
- Reports coverage percentages and identifies missing files

## Usage

### Season Processing
```bash
# Process a complete season
python processing/seasonal_processors/process_2023_season.py

# Check HTML file availability first
python processing/html_checkers/check_2023_html_files.py
```

### Core Dependencies
All processing scripts depend on:
- `scripts/populate_match_player_summary_2018.py` - Core extraction engine
- Database connection to `data/processed/nwsldata.db`
- HTML files located at `/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files/`

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

## Historical Context

These scripts were developed systematically to process the complete NWSL historical database:
1. **Initial Focus**: 2022-2025 seasons with modern format
2. **Legacy Extension**: 2015-2018 seasons using format compatibility
3. **Historical Completion**: 2013-2014 founding seasons
4. **Quality Control**: Manual fixes and data validation

The organized structure reflects the evolution from ad-hoc processing to systematic, reusable season processors.