#!/usr/bin/env python3
"""
Run comprehensive team statistics extraction for all matches with summary CSV data.
Populates match_team table with aggregate team performance metrics.
"""

import os
import logging
from pathlib import Path
from extract_team_stats import extract_team_stats_from_match, save_team_stats_to_database

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """Extract team stats for all matches with summary CSV files."""
    
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    tables_dir = "/Users/thomasmcmillan/projects/nwsl_data/notebooks/match_html_files/tables"
    
    # Get all match directories
    tables_path = Path(tables_dir)
    if not tables_path.exists():
        logging.error(f"Tables directory not found: {tables_dir}")
        return
    
    match_dirs = [d for d in tables_path.iterdir() if d.is_dir()]
    logging.info(f"Found {len(match_dirs)} match directories")
    
    total_teams = 0
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
            
            # Extract team stats from local CSV files
            team_stats = extract_team_stats_from_match(match_id, tables_dir)
            
            if team_stats:
                # Save to database
                save_team_stats_to_database(team_stats, db_path)
                
                total_teams += len(team_stats)
                successful_matches += 1
                
                logging.info(f"✓ Match {match_id}: {len(team_stats)} teams processed")
            else:
                failed_matches += 1
                logging.warning(f"✗ Match {match_id}: No team stats extracted")
                
        except Exception as e:
            failed_matches += 1
            logging.error(f"✗ Match {match_id}: Error - {e}")
    
    # Summary
    logging.info("\n" + "="*60)
    logging.info("TEAM STATS EXTRACTION SUMMARY")
    logging.info("="*60)
    logging.info(f"Total match directories: {len(match_dirs)}")
    logging.info(f"Matches with summary files: {len(match_dirs) - no_summary_matches}")
    logging.info(f"Successful extractions: {successful_matches}")
    logging.info(f"Failed extractions: {failed_matches}")
    logging.info(f"No summary files: {no_summary_matches}")
    logging.info(f"Total team records extracted: {total_teams}")
    if successful_matches > 0:
        logging.info(f"Average teams per match: {total_teams / successful_matches:.1f}")
    
    # Verify final data
    logging.info("\nFINAL TEAM STATS DATA VERIFICATION:")
    
    import sqlite3
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Overall counts
    cursor.execute("SELECT COUNT(*) FROM match_team")
    total_records = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT match_id) FROM match_team")
    matches_with_team_stats = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT team_id) FROM match_team")
    unique_teams = cursor.fetchone()[0]
    
    # Sample statistics
    cursor.execute("""
        SELECT 
            AVG(shots) as avg_shots,
            AVG(shots_on_target) as avg_sot,
            AVG(pass_accuracy) as avg_pass_pct,
            AVG(passes_completed) as avg_passes_completed,
            AVG(yellow_cards) as avg_yellow_cards
        FROM match_team 
        WHERE shots IS NOT NULL
    """)
    avg_stats = cursor.fetchone()
    
    logging.info(f"  Total team records: {total_records}")
    logging.info(f"  Matches with team stats: {matches_with_team_stats}")
    logging.info(f"  Unique teams: {unique_teams}")
    
    if avg_stats and avg_stats[0] is not None:
        logging.info("  Average team performance per match:")
        logging.info(f"    Shots: {avg_stats[0]:.1f}")
        logging.info(f"    Shots on target: {avg_stats[1]:.1f}")
        logging.info(f"    Pass accuracy: {avg_stats[2]:.1f}%")
        logging.info(f"    Passes completed: {avg_stats[3]:.0f}")
        logging.info(f"    Yellow cards: {avg_stats[4]:.1f}")
    
    # Show teams with most shots per match
    cursor.execute("""
        SELECT team_id, COUNT(*) as matches, AVG(shots) as avg_shots, AVG(shots_on_target) as avg_sot
        FROM match_team 
        WHERE shots IS NOT NULL
        GROUP BY team_id
        ORDER BY avg_shots DESC
        LIMIT 5
    """)
    top_teams = cursor.fetchall()
    
    if top_teams:
        logging.info("  Most attacking teams (by average shots per match):")
        for team_id, matches, avg_shots, avg_sot in top_teams:
            logging.info(f"    {team_id}: {avg_shots:.1f} shots ({avg_sot:.1f} on target) in {matches} matches")
    
    conn.close()
    
    print(f"\n🎯 Successfully extracted team statistics for {total_teams} team records from {successful_matches} matches!")

if __name__ == "__main__":
    main()