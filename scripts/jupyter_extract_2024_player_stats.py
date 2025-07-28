#!/usr/bin/env python3
"""
Jupyter-friendly player stats extraction using local CSV files for any season
"""

import sqlite3
import time
import logging
from datetime import datetime
from extract_local_player_stats import extract_player_stats_from_local_csvs, resolve_player_ids_and_team_names, save_comprehensive_player_stats

def extract_player_stats_jupyter(season_id=2024):
    """
    Extract player stats with live progress output for Jupyter notebooks.
    """
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    
    # Get all matches for the specified season
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT match_id, match_date 
        FROM match 
        WHERE season_id = ?
        ORDER BY match_date
    """, (season_id,))
    
    all_matches = cursor.fetchall()
    
    # Check which ones already have data
    cursor.execute("""
        SELECT DISTINCT match_id 
        FROM match_player 
        WHERE match_id IN (
            SELECT match_id FROM match WHERE season_id = ?
        )
    """, (season_id,))
    
    existing = {row[0] for row in cursor.fetchall()}
    conn.close()
    
    # Filter to matches that need extraction
    matches_to_process = [(match_id, match_date) for match_id, match_date in all_matches 
                         if match_id not in existing]
    
    print(f"\n{'='*70}")
    print(f"{season_id} NWSL Player Stats Extraction - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}")
    print(f"Total {season_id} matches: {len(all_matches)}")
    print(f"Already extracted (skipping): {len(existing)}")
    print(f"New matches to extract: {len(matches_to_process)}")
    
    if not matches_to_process:
        print(f"\n✓ All {season_id} matches already extracted! Nothing to do.")
        return
    
    print(f"\nExtracting player stats from {len(matches_to_process)} matches...")
    print("Using local CSV files (no web scraping - much faster!)\n")
    
    # Process each match
    success_count = 0
    error_count = 0
    start_time = time.time()
    
    for i, (match_id, match_date) in enumerate(matches_to_process, 1):
        print(f"[{i}/{len(matches_to_process)}] {match_id} ({match_date})...")
        
        try:
            # Extract player stats from local CSV files
            players = extract_player_stats_from_local_csvs(match_id)
            
            if players:
                # Resolve player IDs and team names
                resolve_player_ids_and_team_names(players, db_path)
                
                # Save to database
                save_comprehensive_player_stats(players, db_path)
                
                success_count += 1
                print(f"✓ Success! Extracted stats for {len(players)} players from local data")
                
            else:
                error_count += 1
                print(f"✗ No local data found for match {match_id}")
                
        except Exception as e:
            error_count += 1
            print(f"✗ Error: {str(e)}")
        
        # Progress update every 10 matches
        if i % 10 == 0:
            elapsed = time.time() - start_time
            rate = i / (elapsed / 60)  # matches per minute
            remaining_time = (len(matches_to_process) - i) / rate if rate > 0 else 0
            
            print(f"📊 Progress: {i}/{len(matches_to_process)} matches ({i/len(matches_to_process)*100:.1f}%)")
            print(f"   Success: {success_count}, Errors: {error_count}")
            print(f"   Rate: {rate:.1f} matches/min, Est. remaining: {remaining_time:.1f} min\n")
    
    # Final summary
    elapsed = time.time() - start_time
    
    print(f"{'='*70}")
    print(f"Extraction Complete! Time: {int(elapsed//60)}m {int(elapsed%60)}s")
    print(f"✓ Successful: {success_count}/{len(matches_to_process)}")
    print(f"✗ Errors: {error_count}/{len(matches_to_process)}")
    print(f"Success rate: {success_count/len(matches_to_process)*100:.1f}%")
    print(f"{'='*70}")
    
    # Show final database stats
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT COUNT(DISTINCT match_id) as matches, COUNT(*) as total_records
        FROM match_player 
        WHERE match_id IN (SELECT match_id FROM match WHERE season_id = ?)
    """, (season_id,))
    
    final_matches, final_records = cursor.fetchone()
    total_season_matches = len(all_matches)
    conn.close()
    
    print(f"\n📈 Final {season_id} Database Stats:")
    print(f"   Matches with player data: {final_matches}/{total_season_matches}")
    print(f"   Total player records: {final_records:,}")
    if total_season_matches > 0:
        print(f"   Coverage: {final_matches/total_season_matches*100:.1f}%")

# Convenience functions for easy notebook use
def run_2024_extraction():
    """Simple function to run the 2024 extraction."""
    extract_player_stats_jupyter(2024)

def run_all_seasons_extraction():
    """Extract all seasons from 2013-2025."""
    seasons = [2025, 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016, 2015, 2014, 2013]
    
    print(f"\n🚀 Starting comprehensive extraction for {len(seasons)} seasons: {', '.join(map(str, seasons))}")
    print(f"{'='*80}")
    
    total_start_time = time.time()
    season_results = []
    
    for season in seasons:
        season_start = time.time()
        
        # Extract this season
        extract_player_stats_jupyter(season)
        
        season_time = time.time() - season_start
        season_results.append((season, season_time))
        
        print(f"\n{'='*40}")
        print(f"Season {season} completed in {season_time:.1f}s")
        print(f"{'='*40}\n")
    
    total_time = time.time() - total_start_time
    
    print(f"\n🎉 ALL SEASONS EXTRACTION COMPLETE!")
    print(f"{'='*80}")
    print(f"Total time: {int(total_time//60)}m {int(total_time%60)}s")
    print(f"\nSeason breakdown:")
    for season, season_time in season_results:
        print(f"  {season}: {season_time:.1f}s")
    print(f"{'='*80}")

if __name__ == "__main__":
    run_all_seasons_extraction()