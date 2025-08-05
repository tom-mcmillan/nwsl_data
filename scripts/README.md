# NWSL Data Scripts

This directory contains scripts for processing NWSL data and maintaining the database.

## Organization

Scripts are organized by functional categories following clean architecture principles:

### 📥 Data Extraction (`data-extraction/`)
- Web scraping (FBRef, official sources)
- HTML processing and parsing
- Batch extraction utilities
- Season-specific processors

### ⚙️ Data Processing (`data-processing/`)
- Match data transformation
- Team summary generation
- Player record processing

### 🗄️ Database Management (`database-management/`)
- Schema creation and updates
- Reference data import
- Data maintenance utilities

### 📊 Analysis (`analysis/`)
- Advanced analytics processing
- Team and player metrics
- Venue and weather analysis

### ✅ Validation (`validation/`)
- Data quality checks
- Completeness analysis
- HTML file validation

### 🧪 Testing (`testing/`)
- Integration tests
- MCP server testing
- Data integrity checks

### 🚀 Deployment (`deployment/`)
- Deployment utilities (reserved for future use)

## Usage

### Run From Project Root
```bash
# Always run scripts from project root for proper path resolution
python scripts/data-extraction/scrape_player_data.py
python scripts/analysis/extract_comprehensive_team_stats.py
```

### Install Dependencies
```bash
# Install required packages
pip install -r requirements/dev.txt
```

### Season Processing Workflow
```bash
# 1. Validate data availability
python scripts/validation/html_checkers/check_2024_html_files.py

# 2. Extract and process season data
python scripts/data-extraction/seasonal_processors/process_2024_season.py

# 3. Validate results
python scripts/validation/analyze_match_completeness.py
```

## Core Processing Engines

### Legacy Format (2013-2018)
- **24 statistical fields**
- Uses enhanced extraction engine
- Handles age parsing in "YY-DDD" format
- Processing speed: ~35 records/second

### Modern Format (2019-2025)
- **37 statistical fields**
- Enhanced data structure with additional metrics
- Direct processing compatibility
- Advanced analytics integration

## Best Practices

1. **Path Resolution**: Always run from project root
2. **Environment**: Use development environment for script execution
3. **Documentation**: Each category has detailed README with usage examples
4. **Testing**: Run validation scripts before deploying processed data
5. **Dependencies**: Check requirements before running scripts

## Migration Notes

Scripts have been reorganized from flat structure to functional categories:
- Original scripts moved to appropriate functional directories
- Maintained backward compatibility through clear documentation
- Each category includes specific usage instructions

## Processing Results

### Success Metrics
- **Processing Speed**: ~35 records/second average
- **Match Success Rate**: 100% for seasons with available HTML files
- **Data Integrity**: Comprehensive validation and error handling

### Dependencies
- Database: `data/processed/nwsldata.db`
- HTML files: Season-specific HTML data files
- Python packages: Listed in `requirements/` directory