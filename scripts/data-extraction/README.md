# Data Extraction Scripts

Scripts for extracting NWSL data from various sources.

## Web Scraping
- `batch_scraper.py` - Batch web scraping utilities
- `fbref_scraper.py` - FBRef statistics scraper
- `master_scraper.py` - Main scraping orchestration
- `scrape_player_data.py` - Player data extraction

## HTML Processing
- `html_player_stats_extractor.py` - Extract player stats from HTML files
- `html_team_stats_extractor.py` - Extract team stats from HTML files

## Batch Processing
- `batch_scripts/` - Season-specific batch extraction scripts
  - `batch_player_extraction.py` - Player data batch extraction
  - `batch_roster_*` - Historical roster extraction by season/batch

## Seasonal Processing
- `seasonal_processors/` - Season-specific data processing
  - `process_20XX_season.py` - Process complete season data

## Usage

### Basic Scraping
```bash
python data-extraction/scrape_player_data.py
python data-extraction/fbref_scraper.py
```

### Batch Operations
```bash
python data-extraction/batch_scripts/batch_player_extraction.py
python data-extraction/seasonal_processors/process_2024_season.py
```