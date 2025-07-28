#!/usr/bin/env python3
"""
Production-ready player stats extraction for any season or match list
Now with comprehensive logging, anti-blocking measures, and variable delays
"""

import sqlite3
import time
import random
import logging
import os
from datetime import datetime
from extract_comprehensive_player_stats import scrape_player_stats, resolve_player_ids_and_team_names, save_comprehensive_player_stats

# Configure logging to file
def setup_logging(log_filename="player_extraction.log"):
    """Set up logging to file only - no console output."""
    log_path = f"/Users/thomasmcmillan/projects/nwsl_data/{log_filename}"
    
    # Create logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # File handler ONLY - NO CONSOLE OUTPUT
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    # Add ONLY the file handler
    logger.addHandler(file_handler)
    
    return log_path

def anti_blocking_delay(min_delay=3, max_delay=12, batch_position=None, total_batches=None):
    """
    Implement variable delays with anti-blocking measures.
    """
    # Base random delay
    base_delay = random.uniform(min_delay, max_delay)
    
    # Add variation based on position to avoid patterns
    if batch_position and total_batches:
        position_factor = 0.5 * (batch_position % 5) / 5
        base_delay += position_factor
        
        # Longer delays for every 10th request
        if batch_position % 10 == 0:
            base_delay += random.uniform(2, 5)
    
    # Return the delay value for printing
    return base_delay

def extract_player_stats_by_season(season_id, batch_name=None, log_filename=None):
    """
    Extract player stats for an entire season.
    
    Parameters:
    season_id: int - The season to extract (e.g., 2023, 2022, 2021)
    batch_name: str - Optional name for the batch (defaults to "Season {season_id}")
    log_filename: str - Optional log filename (defaults to "{season_id}_extraction.log")
    
    Example:
    extract_player_stats_by_season(2023)
    extract_player_stats_by_season(2022, "2022 Season Backfill", "2022_backfill.log")
    """
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    
    if batch_name is None:
        batch_name = f"Season {season_id}"
    
    if log_filename is None:
        log_filename = f"{season_id}_extraction.log"
    
    # Setup logging
    log_path = setup_logging(log_filename)
    
    logging.info(f"Starting extraction for {batch_name}")
    logging.info(f"Logging to: {log_path}")
    
    # Get all matches for the season
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT match_id, match_date 
        FROM match 
        WHERE season_id = ?
        ORDER BY match_date
    """, (season_id,))
    
    all_matches = cursor.fetchall()
    logging.info(f"Found {len(all_matches)} matches for season {season_id}")
    
    # Check which ones already have data
    cursor.execute("""
        SELECT DISTINCT match_id 
        FROM match_player 
        WHERE match_id IN (
            SELECT match_id FROM match WHERE season_id = ?
        )
    """, (season_id,))
    
    existing = {row[0] for row in cursor.fetchall()}
    logging.info(f"Found {len(existing)} matches with existing player stats")
    conn.close()
    
    if not all_matches:
        print(f"❌ No matches found for season {season_id}")
        logging.error(f"❌ No matches found for season {season_id}")
        return
    
    # Filter to matches that need extraction
    matches_to_process = [(match_id, match_date) for match_id, match_date in all_matches 
                         if match_id not in existing]
    
    logging.info(f"Need to process {len(matches_to_process)} matches")
    
    _run_extraction(matches_to_process, all_matches, existing, batch_name)

def extract_player_stats_by_matches(match_ids, batch_name="Custom Match List", log_filename="custom_extraction.log"):
    """
    Extract player stats for specific match IDs.
    
    Parameters:
    match_ids: list - List of match IDs to extract (e.g., ['abc123', 'def456'])
    batch_name: str - Name for this batch extraction
    log_filename: str - Log filename for this extraction
    
    Example:
    extract_player_stats_by_matches(['abc123', 'def456'], "Missing Matches", "missing_matches.log")
    """
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    
    # Setup logging
    log_path = setup_logging(log_filename)
    
    # Get match details
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Build query for specific match IDs
    placeholders = ','.join(['?' for _ in match_ids])
    cursor.execute(f"""
        SELECT match_id, match_date 
        FROM match 
        WHERE match_id IN ({placeholders})
        ORDER BY match_date
    """, match_ids)
    
    all_matches = cursor.fetchall()
    
    # Check which ones already have data
    cursor.execute(f"""
        SELECT DISTINCT match_id 
        FROM match_player 
        WHERE match_id IN ({placeholders})
    """, match_ids)
    
    existing = {row[0] for row in cursor.fetchall()}
    conn.close()
    
    if not all_matches:
        print("❌ No valid matches found for provided IDs")
        return
    
    # Filter to matches that need extraction
    matches_to_process = [(match_id, match_date) for match_id, match_date in all_matches 
                         if match_id not in existing]
    
    # Log detailed info to file only
    logging.info(f"Starting extraction for {batch_name}")
    logging.info(f"Logging to: {log_path}")
    logging.info(f"Processing {len(match_ids)} match IDs: {match_ids}")
    logging.info(f"Found {len(all_matches)} valid matches in database")
    logging.info(f"Found {len(existing)} matches with existing player stats")
    logging.info(f"Need to process {len(matches_to_process)} matches")
    
    _run_extraction(matches_to_process, all_matches, existing, batch_name)

def _run_extraction(matches_to_process, all_matches, existing, batch_name):
    """Internal function that does the actual extraction work with production-ready features."""
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    
    # Clean console output matching match downloader style
    print(f"\n{'='*60}")
    print(f"{batch_name} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    print(f"Total matches: {len(all_matches)}")
    print(f"Already extracted (skipping): {len(existing)}")
    print(f"New matches to extract: {len(matches_to_process)}")
    
    if not matches_to_process:
        print("\n✓ All matches already extracted! Nothing to do.")
        return
    
    print(f"\nExtracting player stats from {len(matches_to_process)} matches...")
    print("Using Selenium with anti-blocking measures\n")
    
    # Log to file
    logging.info(f"{'='*70}")
    logging.info(f"{batch_name} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info(f"{'='*70}")
    logging.info(f"Total matches: {len(all_matches)}")
    logging.info(f"Already extracted (skipping): {len(existing)}")
    logging.info(f"New matches to extract: {len(matches_to_process)}")
    logging.info("Using comprehensive stats extraction with 3-12 second variable delays")
    
    # Process each match
    success_count = 0
    error_count = 0
    start_time = time.time()
    results = []
    
    for i, (match_id, match_date) in enumerate(matches_to_process, 1):
        fbref_url = f"https://fbref.com/en/matches/{match_id}"
        
        print(f"[{i}/{len(matches_to_process)}] Extracting {match_id}...")
        print(f"Scraping player stats from FBRef...")
        
        # Log detailed info to file
        logging.info(f"Processing match {i}/{len(matches_to_process)}: {match_id} ({match_date})")
        logging.info(f"Scraping comprehensive player stats from: {fbref_url}")
        
        try:
            # Extract player stats
            players = scrape_player_stats(fbref_url, match_id)
            
            if players:
                # Resolve player IDs and team names
                resolve_player_ids_and_team_names(players, db_path)
                
                # Save to database
                save_comprehensive_player_stats(players, db_path)
                
                success_count += 1
                results.append({'match_id': match_id, 'status': 'success', 'players': len(players)})
                print(f"✓ Success! Extracted stats for {len(players)} players")
                
                # Log to file
                logging.info(f"✅ Successfully extracted stats for {len(players)} players in match {match_id}")
                
                # Anti-blocking delay
                if i < len(matches_to_process):
                    delay = anti_blocking_delay(min_delay=3, max_delay=12, 
                                              batch_position=i, total_batches=len(matches_to_process))
                    print(f"Waiting {int(delay)} seconds before next extraction...\n")
                    time.sleep(delay)
                    logging.info(f"   Waited {delay:.1f} seconds")
                
            else:
                error_count += 1
                results.append({'match_id': match_id, 'status': 'no_data'})
                print(f"✗ Failed: No player stats found")
                
                # Log to file
                logging.warning(f"⚠️  No player stats found for match {match_id}")
                
                # Still delay on errors
                if i < len(matches_to_process):
                    delay = anti_blocking_delay(min_delay=5, max_delay=15, 
                                              batch_position=i, total_batches=len(matches_to_process))
                    print(f"Waiting {int(delay)} seconds before next extraction...\n")
                    time.sleep(delay)
                    logging.info(f"   Waited {delay:.1f} seconds")
                
        except Exception as e:
            error_count += 1
            results.append({'match_id': match_id, 'status': 'error', 'error': str(e)})
            print(f"✗ Failed: {str(e)}")
            
            # Log to file
            logging.error(f"❌ Error processing match {match_id}: {str(e)}")
            
            # Longer delay on errors
            if i < len(matches_to_process):
                delay = anti_blocking_delay(min_delay=8, max_delay=20, 
                                          batch_position=i, total_batches=len(matches_to_process))
                print(f"Waiting {int(delay)} seconds before next extraction...\n")
                time.sleep(delay)
                logging.info(f"   Waited {delay:.1f} seconds")
    
    # Final summary matching match downloader style
    elapsed = time.time() - start_time
    
    print("\n" + "="*60)
    print(f"Extraction complete! Time: {int(elapsed//60)}m {int(elapsed%60)}s")
    print(f"✓ Successful: {success_count}")
    print(f"✗ Failed: {error_count}")
    
    # Show detailed results
    print("\nDetailed results:")
    for r in results:
        status_icon = "✓" if r['status'] == 'success' else "✗"
        print(f"  {status_icon} {r['match_id']}: {r['status']}")
        if r['status'] == 'success':
            print(f"     Players extracted: {r['players']}")
    
    print("="*60)
    
    # Log detailed summary to file
    logging.info(f"{'='*70}")
    logging.info(f"EXTRACTION COMPLETE!")
    logging.info(f"Batch: {batch_name}")
    logging.info(f"Duration: {int(elapsed//60)}m {int(elapsed%60)}s")
    logging.info(f"Total matches processed: {len(matches_to_process)}")
    logging.info(f"✓ Successful: {success_count}")
    logging.info(f"✗ Errors: {error_count}")
    logging.info(f"Success rate: {success_count/len(matches_to_process)*100:.1f}%")
    logging.info(f"Average time per match: {elapsed/len(matches_to_process):.1f}s")
    logging.info(f"{'='*70}")

# Convenience functions for easy notebook use
def extract_2024():
    """Extract all 2024 season matches with production logging."""
    extract_player_stats_by_season(2024, "2024 Season", "2024_extraction.log")

def extract_2023():
    """Extract all 2023 season matches with production logging."""
    extract_player_stats_by_season(2023, "2023 Season", "2023_extraction.log")

def extract_2022():
    """Extract all 2022 season matches with production logging."""
    extract_player_stats_by_season(2022, "2022 Season", "2022_extraction.log")

def extract_2021():
    """Extract all 2021 season matches with production logging."""
    extract_player_stats_by_season(2021, "2021 Season", "2021_extraction.log")

def get_extraction_summary(season_id=None):
    """
    Get summary of extraction results for a season or all seasons.
    
    Parameters:
    season_id: int - Optional season to summarize (e.g., 2024, 2023)
                     If None, shows summary for all seasons
    """
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    if season_id:
        # Season-specific summary
        cursor.execute("SELECT COUNT(*) FROM match WHERE season_id = ?", (season_id,))
        total_matches = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(DISTINCT match_id) 
            FROM match_player 
            WHERE match_id IN (SELECT match_id FROM match WHERE season_id = ?)
        """, (season_id,))
        matches_with_stats = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) 
            FROM match_player 
            WHERE match_id IN (SELECT match_id FROM match WHERE season_id = ?)
        """, (season_id,))
        total_player_records = cursor.fetchone()[0]
        
        coverage = matches_with_stats/total_matches*100 if total_matches > 0 else 0
        avg_players = total_player_records/matches_with_stats if matches_with_stats > 0 else 0
        
        print(f"""
====== {season_id} PLAYER STATS EXTRACTION SUMMARY ======
Total {season_id} matches: {total_matches}
Matches with player stats: {matches_with_stats}
Coverage: {coverage:.1f}%
Total player records: {total_player_records:,}
Average players per match: {avg_players:.1f}
========================================================
        """)
    else:
        # All seasons summary
        cursor.execute("""
            SELECT season_id, COUNT(*) as total_matches
            FROM match 
            GROUP BY season_id 
            ORDER BY season_id DESC
        """)
        season_totals = cursor.fetchall()
        
        print(f"\n====== ALL SEASONS PLAYER STATS SUMMARY ======")
        for season, total in season_totals:
            cursor.execute("""
                SELECT COUNT(DISTINCT match_id) 
                FROM match_player 
                WHERE match_id IN (SELECT match_id FROM match WHERE season_id = ?)
            """, (season,))
            with_stats = cursor.fetchone()[0]
            coverage = with_stats/total*100 if total > 0 else 0
            print(f"Season {season}: {with_stats}/{total} matches ({coverage:.1f}%)")
        print(f"===============================================\n")
    
    conn.close()

if __name__ == "__main__":
    # Example usage - uncomment to run
    # extract_player_stats_by_season(2