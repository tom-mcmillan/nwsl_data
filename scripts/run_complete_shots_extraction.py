#!/usr/bin/env python3
"""
Run comprehensive shots extraction to achieve complete 2019-2025 coverage.
Fills gaps in 2021-2023 and extracts all 2024 data.
"""

import sqlite3
import logging
from pathlib import Path
from extract_local_shots_data import extract_shots_from_local_csv, resolve_player_ids, save_shots_to_database

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_missing_shots_matches(db_path, target_years=None):
    """Get matches that need shots data extraction."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Build year filter
    year_filter = ""
    if target_years:
        year_placeholders = ','.join(['?' for _ in target_years])
        year_filter = f"AND s.season_year IN ({year_placeholders})"
    
    query = f"""
    SELECT m.match_id, m.match_date, s.season_year
    FROM match m
    JOIN season s ON m.season_id = s.season_id
    LEFT JOIN (SELECT DISTINCT match_id FROM match_shot) ms ON m.match_id = ms.match_id
    WHERE s.season_year >= 2019 {year_filter} AND ms.match_id IS NULL
    ORDER BY m.match_date
    """
    
    if target_years:
        cursor.execute(query, target_years)
    else:
        cursor.execute(query)
    
    results = cursor.fetchall()
    conn.close()
    
    return [(match_id, match_date, season_year) for match_id, match_date, season_year in results]

def main():
    """Extract shots data for all missing matches 2019-2025."""
    
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    tables_dir = "/Users/thomasmcmillan/projects/nwsl_data/notebooks/match_html_files/tables"
    
    # Get all matches missing shots data from 2019-2025
    missing_matches = get_missing_shots_matches(db_path)
    
    if not missing_matches:
        logging.info("No missing shots data found - all matches already have shots!")
        return
    
    logging.info(f"Found {len(missing_matches)} matches missing shots data")
    
    # Group by year for reporting
    by_year = {}
    for match_id, match_date, season_year in missing_matches:
        if season_year not in by_year:
            by_year[season_year] = []
        by_year[season_year].append((match_id, match_date))
    
    for year, matches in by_year.items():
        logging.info(f"  {year}: {len(matches)} matches")
    
    total_shots = 0
    successful_matches = 0
    failed_matches = 0
    no_csv_matches = 0
    
    for i, (match_id, match_date, season_year) in enumerate(missing_matches, 1):
        try:
            # Check if CSV file exists
            shots_file = Path(tables_dir) / match_id / f"{match_id}_shots_all.csv"
            if not shots_file.exists():
                no_csv_matches += 1
                logging.warning(f"✗ Match {match_id} ({season_year}): No shots CSV file")
                continue
            
            logging.info(f"Processing match {i}/{len(missing_matches)}: {match_id} ({season_year}) - {match_date}")
            
            # Extract shots from local CSV files
            shots = extract_shots_from_local_csv(match_id, tables_dir)
            
            if shots:
                # Resolve player IDs
                resolve_player_ids(shots, db_path)
                
                # Save to database
                save_shots_to_database(shots, db_path)
                
                total_shots += len(shots)
                successful_matches += 1
                logging.info(f"✓ Match {match_id}: {len(shots)} shots extracted")
            else:
                # Some matches might have empty shots files (no shots taken)
                successful_matches += 1
                logging.info(f"✓ Match {match_id}: 0 shots (valid - no shots taken)")
                
        except Exception as e:
            failed_matches += 1
            logging.error(f"✗ Match {match_id}: Error - {e}")
    
    # Final summary
    logging.info("\n" + "="*60)
    logging.info("COMPLETE SHOTS EXTRACTION SUMMARY")
    logging.info("="*60)
    logging.info(f"Total matches processed: {len(missing_matches)}")
    logging.info(f"Successful extractions: {successful_matches}")
    logging.info(f"Failed extractions: {failed_matches}")
    logging.info(f"No CSV file available: {no_csv_matches}")
    logging.info(f"Total shots extracted: {total_shots}")
    logging.info(f"Success rate: {successful_matches / len(missing_matches) * 100:.1f}%")
    
    # Verify final coverage
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
    SELECT 
        s.season_year,
        COUNT(DISTINCT m.match_id) as total_matches,
        COUNT(DISTINCT ms.match_id) as matches_with_shots,
        ROUND(COUNT(DISTINCT ms.match_id) * 100.0 / COUNT(DISTINCT m.match_id), 1) as coverage_pct
    FROM match m
    JOIN season s ON m.season_id = s.season_id
    LEFT JOIN (SELECT DISTINCT match_id FROM match_shot) ms ON m.match_id = ms.match_id
    WHERE s.season_year >= 2019
    GROUP BY s.season_year
    ORDER BY s.season_year
    """)
    
    logging.info("\nFINAL SHOTS COVERAGE BY YEAR:")
    for row in cursor.fetchall():
        year, total, with_shots, coverage = row
        logging.info(f"  {year}: {with_shots}/{total} matches ({coverage}%)")
    
    conn.close()
    
    print(f"\n🎯 Successfully extracted shots data! Added {total_shots} shots from {successful_matches} matches.")

if __name__ == "__main__":
    main()