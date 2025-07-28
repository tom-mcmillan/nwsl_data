#!/usr/bin/env python3
"""
Run comprehensive goalkeeper statistics extraction for all matches with local CSV data.
"""

import os
import logging
from pathlib import Path
from extract_local_goalkeeper_stats import extract_goalkeeper_stats_from_local_csvs, resolve_goalkeeper_ids_and_details, save_comprehensive_goalkeeper_stats

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """Extract goalkeeper stats for all matches with local CSV data."""
    
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    tables_dir = "/Users/thomasmcmillan/projects/nwsl_data/notebooks/match_html_files/tables"
    
    # Get all match directories
    tables_path = Path(tables_dir)
    if not tables_path.exists():
        logging.error(f"Tables directory not found: {tables_dir}")
        return
    
    match_dirs = [d for d in tables_path.iterdir() if d.is_dir()]
    logging.info(f"Found {len(match_dirs)} match directories")
    
    total_goalkeepers = 0
    successful_matches = 0
    failed_matches = 0
    
    for i, match_dir in enumerate(match_dirs, 1):
        match_id = match_dir.name
        
        # Check if this match has goalkeeper data
        keeper_files = list(match_dir.glob(f"{match_id}_keeper_stats_*.csv"))
        if not keeper_files:
            continue
            
        try:
            logging.info(f"Processing match {i}/{len(match_dirs)}: {match_id}")
            
            # Extract goalkeeper stats from local CSV files
            goalkeepers = extract_goalkeeper_stats_from_local_csvs(match_id, tables_dir)
            
            if goalkeepers:
                # Resolve player IDs, team names, and season IDs
                resolve_goalkeeper_ids_and_details(goalkeepers, db_path)
                
                # Save to database
                save_comprehensive_goalkeeper_stats(goalkeepers, db_path)
                
                total_goalkeepers += len(goalkeepers)
                successful_matches += 1
                logging.info(f"✓ Match {match_id}: {len(goalkeepers)} goalkeepers processed")
            else:
                failed_matches += 1
                logging.warning(f"✗ Match {match_id}: No goalkeeper data extracted")
                
        except Exception as e:
            failed_matches += 1
            logging.error(f"✗ Match {match_id}: Error - {e}")
    
    # Summary
    logging.info("\n" + "="*60)
    logging.info("GOALKEEPER EXTRACTION SUMMARY")
    logging.info("="*60)
    logging.info(f"Total matches processed: {successful_matches + failed_matches}")
    logging.info(f"Successful matches: {successful_matches}")
    logging.info(f"Failed matches: {failed_matches}")
    logging.info(f"Total goalkeepers extracted: {total_goalkeepers}")
    logging.info(f"Success rate: {successful_matches / (successful_matches + failed_matches) * 100:.1f}%")
    
    print(f"\n🎯 Successfully extracted goalkeeper stats for {total_goalkeepers} goalkeepers from {successful_matches} matches!")

if __name__ == "__main__":
    main()