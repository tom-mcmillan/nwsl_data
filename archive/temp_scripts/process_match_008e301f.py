#!/usr/bin/env python3
"""
Process match 008e301f specifically
"""

import sqlite3
import os
import sys
import uuid
import logging

# Add current directory to path
sys.path.append('/Users/thomasmcmillan/projects/nwsl_data')
from fbref_player_extractor import FBRefPlayerExtractor

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_match_008e301f():
    """Process match 008e301f specifically"""
    
    match_id = "008e301f"
    html_dir = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files"
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    html_file = f"match_{match_id}.html"
    html_path = os.path.join(html_dir, html_file)
    
    print(f"🎯 Processing match: {match_id}")
    print(f"📄 HTML file: {html_file}")
    
    # Check if HTML file exists
    if not os.path.exists(html_path):
        print(f"❌ HTML file not found: {html_path}")
        return False
    
    # Check existing data
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM match_player WHERE match_id = ?", (match_id,))
    existing_count = cursor.fetchone()[0]
    print(f"📊 Existing player records for this match: {existing_count}")
    
    # Delete existing data for clean insert
    if existing_count > 0:
        print(f"🗑️  Deleting existing records...")
        cursor.execute("DELETE FROM match_player WHERE match_id = ?", (match_id,))
        conn.commit()
        print(f"✅ Deleted {existing_count} existing records")
    
    conn.close()
    
    # Extract player data
    print(f"\n🔍 Extracting player data from HTML...")
    extractor = FBRefPlayerExtractor(db_path)
    success = extractor.process_html_file(html_path)
    
    if not success or not extractor.processed_matches:
        print(f"❌ Failed to extract player stats")
        return False
    
    # Get extracted data
    _, players_data = extractor.processed_matches[-1]
    print(f"✅ Extracted data for {len(players_data)} players")
    
    # Show first player's data structure
    if players_data:
        sample_player = players_data[0]
        print(f"\n📊 Sample player: {sample_player.get('player_name', 'Unknown')}")
        populated_fields = {k: v for k, v in sample_player.items() if v is not None}
        print(f"Fields with data: {len(populated_fields)}")
        for key, value in list(populated_fields.items())[:5]:
            print(f"  {key}: {value}")
    
    # Insert with minimal fields to avoid column mismatch
    print(f"\n💾 Inserting player data into database...")
    success_count = insert_players_minimal(players_data, db_path)
    
    if success_count > 0:
        print(f"✅ Successfully inserted {success_count} player records for match {match_id}")
        return True
    else:
        print(f"❌ Failed to insert player data")
        return False

def insert_players_minimal(players: list, db_path: str) -> int:
    """Insert players with minimal essential fields only"""
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        inserted_count = 0
        
        for player in players:
            # Generate unique ID
            match_player_id = str(uuid.uuid4())[:8]
            
            # Get basic resolved info
            team_name = None
            player_id_resolved = None
            season_id = None
            
            # Resolve season_id from match
            if player.get('match_id'):
                cursor.execute("SELECT season_id FROM match WHERE match_id = ?", (player['match_id'],))
                result = cursor.fetchone()
                if result:
                    season_id = result[0]
            
            # Insert with core fields only
            insert_sql = """
                INSERT INTO match_player (
                    match_player_id, match_id, player_id, player_name, team_id, team_name,
                    shirt_number, position, minutes_played, season_id,
                    summary_perf_gls, summary_perf_ast, summary_exp_xg
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            values = (
                match_player_id,
                player.get('match_id'),
                player_id_resolved,
                player.get('player_name'),
                player.get('team_id'),
                team_name,
                player.get('shirt_number'),
                player.get('position'),
                player.get('minutes_played'),
                season_id,
                player.get('summary_perf_gls'),
                player.get('summary_perf_ast'),
                player.get('summary_exp_xg')
            )
            
            cursor.execute(insert_sql, values)
            inserted_count += 1
            
            print(f"  ✓ Inserted: {player.get('player_name')} (Goals: {player.get('summary_perf_gls')}, xG: {player.get('summary_exp_xg')})")
        
        conn.commit()
        conn.close()
        
        return inserted_count
        
    except Exception as e:
        print(f"❌ Database insertion error: {str(e)}")
        return 0

if __name__ == "__main__":
    success = process_match_008e301f()
    
    if success:
        print(f"\n🎉 SUCCESS: Match 008e301f processed!")
        
        # Show final verification
        conn = sqlite3.connect("/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db")
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM match_player WHERE match_id = '008e301f'")
        final_count = cursor.fetchone()[0]
        print(f"📊 Final verification: {final_count} player records in database for match 008e301f")
        conn.close()
    else:
        print(f"\n💥 FAILED: Could not process match 008e301f")