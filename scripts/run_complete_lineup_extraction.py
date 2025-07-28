#!/usr/bin/env python3
"""
Run comprehensive lineup extraction for all matches with summary CSV data.
Populates match_lineups table with starting XI, bench players, and formations.
"""

import os
import logging
from pathlib import Path
from extract_match_lineups import extract_lineup_from_match, resolve_player_ids, save_lineups_to_database

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """Extract lineup data for all matches with summary CSV files."""
    
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    tables_dir = "/Users/thomasmcmillan/projects/nwsl_data/notebooks/match_html_files/tables"
    
    # Get all match directories
    tables_path = Path(tables_dir)
    if not tables_path.exists():
        logging.error(f"Tables directory not found: {tables_dir}")
        return
    
    match_dirs = [d for d in tables_path.iterdir() if d.is_dir()]
    logging.info(f"Found {len(match_dirs)} match directories")
    
    total_players = 0
    successful_matches = 0
    failed_matches = 0
    no_summary_matches = 0
    
    for i, match_dir in enumerate(match_dirs, 1):
        match_id = match_dir.name
        
        # Check if this match has summary data files
        summary_files = list(match_dir.glob(f"{match_id}_stats_*_summary.csv"))
        if not summary_files:
            no_summary_matches += 1
            continue
            
        try:
            logging.info(f"Processing match {i}/{len(match_dirs)}: {match_id}")
            
            # Extract lineup data from local CSV files
            lineups = extract_lineup_from_match(match_id, tables_dir)
            
            if lineups:
                # Resolve player IDs
                resolve_player_ids(lineups, db_path)
                
                # Save to database
                save_lineups_to_database(lineups, db_path)
                
                total_players += len(lineups)
                successful_matches += 1
                
                # Count starters for verification
                starters = len([l for l in lineups if l['is_starter']])
                bench = len([l for l in lineups if not l['is_starter']])
                logging.info(f"✓ Match {match_id}: {len(lineups)} players ({starters} starters, {bench} bench)")
            else:
                failed_matches += 1
                logging.warning(f"✗ Match {match_id}: No lineup data extracted")
                
        except Exception as e:
            failed_matches += 1
            logging.error(f"✗ Match {match_id}: Error - {e}")
    
    # Summary
    logging.info("\n" + "="*60)
    logging.info("LINEUP EXTRACTION SUMMARY")
    logging.info("="*60)
    logging.info(f"Total match directories: {len(match_dirs)}")
    logging.info(f"Matches with summary files: {len(match_dirs) - no_summary_matches}")
    logging.info(f"Successful extractions: {successful_matches}")
    logging.info(f"Failed extractions: {failed_matches}")
    logging.info(f"No summary files: {no_summary_matches}")
    logging.info(f"Total players extracted: {total_players}")
    if successful_matches > 0:
        logging.info(f"Average players per match: {total_players / successful_matches:.1f}")
    
    # Verify final data
    logging.info("\nFINAL LINEUP DATA VERIFICATION:")
    
    import sqlite3
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Overall counts
    cursor.execute("SELECT COUNT(*) FROM match_lineups")
    total_records = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT match_id) FROM match_lineups")
    matches_with_lineups = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM match_lineups WHERE is_starter = 1")
    total_starters = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM match_lineups WHERE is_starter = 0")
    total_bench = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT formation) FROM match_lineups WHERE formation IS NOT NULL")
    unique_formations = cursor.fetchone()[0]
    
    logging.info(f"  Total lineup records: {total_records}")
    logging.info(f"  Matches with lineups: {matches_with_lineups}")
    logging.info(f"  Starting players: {total_starters}")
    logging.info(f"  Bench players: {total_bench}")
    logging.info(f"  Unique formations detected: {unique_formations}")
    
    # Sample formations
    cursor.execute("""
        SELECT formation, COUNT(*) as count 
        FROM match_lineups 
        WHERE formation IS NOT NULL 
        GROUP BY formation 
        ORDER BY count DESC 
        LIMIT 10
    """)
    formations = cursor.fetchall()
    
    if formations:
        logging.info("  Most common formations:")
        for formation, count in formations:
            logging.info(f"    {formation}: {count} instances")
    
    conn.close()
    
    print(f"\n🎯 Successfully extracted lineup data for {total_players} players from {successful_matches} matches!")

if __name__ == "__main__":
    main()