#!/usr/bin/env python3
"""
Batch extract comprehensive player statistics for all 2025 NWSL matches.
"""

import sqlite3
import time
import logging
from extract_comprehensive_player_stats import scrape_player_stats, resolve_player_ids_and_team_names, save_comprehensive_player_stats

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_2025_matches(db_path):
    """Get all 2025 matches that need player stats extraction."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT match_id, match_date 
        FROM match 
        WHERE match_date LIKE '2025%'
        ORDER BY match_date
    """)
    
    matches = cursor.fetchall()
    conn.close()
    
    return [(match_id, match_date) for match_id, match_date in matches]

def check_existing_extractions(db_path):
    """Check which matches already have player stats extracted."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT DISTINCT match_id 
        FROM match_player 
        WHERE match_id IN (
            SELECT match_id FROM match WHERE match_date LIKE '2025%'
        )
    """)
    
    existing = {row[0] for row in cursor.fetchall()}
    conn.close()
    
    return existing

def batch_extract_player_stats(db_path):
    """Extract player stats for all 2025 matches."""
    
    # Get all 2025 matches
    all_matches = get_2025_matches(db_path)
    logging.info(f"Found {len(all_matches)} matches in 2025")
    
    # Check which ones already have data
    existing_extractions = check_existing_extractions(db_path)
    logging.info(f"Found {len(existing_extractions)} matches with existing player stats")
    
    # Filter to matches that need extraction
    matches_to_process = [(match_id, match_date) for match_id, match_date in all_matches 
                         if match_id not in existing_extractions]
    
    logging.info(f"Need to process {len(matches_to_process)} matches")
    
    if not matches_to_process:
        logging.info("All 2025 matches already have player stats extracted!")
        return
    
    # Process each match
    success_count = 0
    error_count = 0
    
    for i, (match_id, match_date) in enumerate(matches_to_process, 1):
        fbref_url = f"https://fbref.com/en/matches/{match_id}"
        
        logging.info(f"Processing match {i}/{len(matches_to_process)}: {match_id} ({match_date})")
        
        try:
            # Extract player stats
            players = scrape_player_stats(fbref_url, match_id)
            
            if players:
                # Resolve player IDs and team names
                resolve_player_ids_and_team_names(players, db_path)
                
                # Save to database
                save_comprehensive_player_stats(players, db_path)
                
                success_count += 1
                logging.info(f"✅ Successfully extracted stats for {len(players)} players in match {match_id}")
                
                # Rate limiting - be respectful to FBRef
                time.sleep(3)
                
            else:
                logging.warning(f"⚠️  No player stats found for match {match_id}")
                error_count += 1
                
        except Exception as e:
            logging.error(f"❌ Error processing match {match_id}: {e}")
            error_count += 1
            
            # Continue processing other matches even if one fails
            continue
        
        # Progress update every 10 matches
        if i % 10 == 0:
            logging.info(f"Progress: {i}/{len(matches_to_process)} matches processed. Success: {success_count}, Errors: {error_count}")
    
    # Final summary
    logging.info(f"""
    ====== BATCH EXTRACTION COMPLETE ======
    Total matches processed: {len(matches_to_process)}
    Successful extractions: {success_count}
    Errors: {error_count}
    Success rate: {success_count/len(matches_to_process)*100:.1f}%
    ========================================
    """)

def get_extraction_summary(db_path):
    """Get summary of extraction results."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Total 2025 matches
    cursor.execute("SELECT COUNT(*) FROM match WHERE match_date LIKE '2025%'")
    total_matches = cursor.fetchone()[0]
    
    # Matches with player stats
    cursor.execute("""
        SELECT COUNT(DISTINCT match_id) 
        FROM match_player 
        WHERE match_id IN (SELECT match_id FROM match WHERE match_date LIKE '2025%')
    """)
    matches_with_stats = cursor.fetchone()[0]
    
    # Total player records
    cursor.execute("""
        SELECT COUNT(*) 
        FROM match_player 
        WHERE match_id IN (SELECT match_id FROM match WHERE match_date LIKE '2025%')
    """)
    total_player_records = cursor.fetchone()[0]
    
    # Average players per match
    if matches_with_stats > 0:
        avg_players = total_player_records / matches_with_stats
    else:
        avg_players = 0
    
    conn.close()
    
    print(f"""
    ====== 2025 PLAYER STATS EXTRACTION SUMMARY ======
    Total 2025 matches: {total_matches}
    Matches with player stats: {matches_with_stats}
    Coverage: {matches_with_stats/total_matches*100:.1f}%
    Total player records: {total_player_records:,}
    Average players per match: {avg_players:.1f}
    ===================================================
    """)

def main():
    """Main function."""
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    
    print("Starting batch extraction of 2025 player stats...")
    
    # Show current status
    get_extraction_summary(db_path)
    
    # Run batch extraction
    batch_extract_player_stats(db_path)
    
    # Show final status
    get_extraction_summary(db_path)

if __name__ == "__main__":
    main()