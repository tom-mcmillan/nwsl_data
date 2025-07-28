#!/usr/bin/env python3
"""
Extract player stats for specific match IDs - similar to fbref_match_downloader
"""

import sqlite3
import time
from datetime import datetime
from extract_comprehensive_player_stats import scrape_player_stats, resolve_player_ids_and_team_names, save_comprehensive_player_stats

def extract_player_stats_batch(match_ids, batch_name="Player Stats Batch"):
    """
    Extract player stats for a batch of matches, skipping already extracted ones.
    
    Parameters:
    match_ids: list - List of match IDs to extract (e.g., ['abc123', 'def456'])
    batch_name: str - Name for this batch extraction
    
    Example:
    match_ids = ['414d2972', '5a808fa8', '9ad58931']
    extract_player_stats_batch(match_ids, "Missing 2024 Matches")
    """
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    
    # Check what's already extracted
    extracted = []
    if match_ids:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check which matches already have player data
        placeholders = ','.join(['?' for _ in match_ids])
        cursor.execute(f"""
            SELECT DISTINCT match_id 
            FROM match_player 
            WHERE match_id IN ({placeholders})
        """, match_ids)
        
        extracted = [row[0] for row in cursor.fetchall()]
        conn.close()
    
    # Filter to only new matches
    new_matches = [m for m in match_ids if m not in extracted]
    skipped = [m for m in match_ids if m in extracted]
    
    print(f"\n{'='*70}")
    print(f"{batch_name} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}")
    print(f"Total matches provided: {len(match_ids)}")
    print(f"Already extracted (skipping): {len(skipped)}")
    print(f"New matches to extract: {len(new_matches)}")
    
    if not new_matches:
        print("\n✓ All matches already extracted! Nothing to do.")
        return []
    
    print(f"\nExtracting: {', '.join(new_matches[:10])}{', ...' if len(new_matches) > 10 else ''}")
    print("Using comprehensive player stats extraction\n")
    
    # Extract each match
    results = []
    start_time = time.time()
    success_count = 0
    error_count = 0
    
    for i, match_id in enumerate(new_matches, 1):
        print(f"[{i}/{len(new_matches)}] Extracting {match_id}...")
        
        try:
            fbref_url = f"https://fbref.com/en/matches/{match_id}"
            
            # Extract player stats
            players = scrape_player_stats(fbref_url, match_id)
            
            if players:
                # Resolve player IDs and team names
                resolve_player_ids_and_team_names(players, db_path)
                
                # Save to database
                save_comprehensive_player_stats(players, db_path)
                
                success_count += 1
                result = {'match_id': match_id, 'status': 'success', 'player_count': len(players)}
                results.append(result)
                
                print(f"✓ Success! Extracted stats for {len(players)} players")
                
            else:
                error_count += 1
                result = {'match_id': match_id, 'status': 'error', 'error': 'No player stats found'}
                results.append(result)
                print(f"✗ No player stats found")
            
            # Rate limiting
            if i < len(new_matches):
                print(f"Waiting 3 seconds...\n")
                time.sleep(3)
            else:
                print("")  # Just newline for last match
                
        except Exception as e:
            error_count += 1
            result = {'match_id': match_id, 'status': 'error', 'error': str(e)}
            results.append(result)
            print(f"✗ Exception: {e}")
            
            if i < len(new_matches):
                print(f"Waiting 3 seconds...\n")
                time.sleep(3)
        
        # Progress update every 10 matches
        if i % 10 == 0:
            elapsed = time.time() - start_time
            rate = i / (elapsed / 60) if elapsed > 0 else 0
            remaining_time = (len(new_matches) - i) / rate if rate > 0 else 0
            
            print(f"📊 Progress: {i}/{len(new_matches)} matches ({i/len(new_matches)*100:.1f}%)")
            print(f"   Success: {success_count}, Errors: {error_count}")
            print(f"   Rate: {rate:.1f} matches/min, Est. remaining: {remaining_time:.1f} min\n")
    
    # Summary
    elapsed = time.time() - start_time
    
    print(f"{'='*70}")
    print(f"Batch complete! Time: {int(elapsed//60)}m {int(elapsed%60)}s")
    print(f"✓ Successful: {success_count}/{len(new_matches)}")
    print(f"✗ Failed: {error_count}/{len(new_matches)}")
    print(f"Success rate: {success_count/len(new_matches)*100:.1f}%")
    print(f"{'='*70}")
    
    return results

# Example usage function
def extract_sample_matches():
    """Example of how to use the extraction function."""
    # Sample match IDs - replace with your actual match IDs
    match_ids = [
        '414d2972', '5a808fa8', '9ad58931', '580abedf', 'cab0661f'
    ]
    
    results = extract_player_stats_batch(match_ids, "Sample Extraction")
    return results

if __name__ == "__main__":
    extract_sample_matches()