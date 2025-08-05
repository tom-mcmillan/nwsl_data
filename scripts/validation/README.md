# Validation Scripts

Scripts for data quality validation and completeness checking.

## Match Analysis
- `analyze_match_completeness.py` - Check match data completeness

## HTML Validation
- `html_checkers/` - Season-specific HTML file validation
  - `check_20XX_html_files.py` - Validate HTML files for each season

## Usage

### Data Completeness
```bash
python validation/analyze_match_completeness.py
```

### HTML File Validation
```bash
python validation/html_checkers/check_2024_html_files.py
```

## Dependencies
- pandas
- BeautifulSoup4 (for HTML parsing)
- Custom validation utilities