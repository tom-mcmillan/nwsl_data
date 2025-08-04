#!/usr/bin/env python3
"""
Script to create match_player_summary records for matches that have match_player 
records but no match_player_summary records.

This script creates the initial records, then the populate script can fill in the statistics.
"""

import sqlite3
import sys
import uuid

def create_match_player_summary_records(match_id):
    """Create match_player_summary records for a match based on its match_player records."""
    
    # Database connection
    db_path = "data/processed/nwsldata.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print(f"Creating match_player_summary records for match {match_id}...")
    
    # Get all match_player records for this match
    cursor.execute("""
        SELECT mp.match_player_id, mp.match_id, mp.player_id, mp.player_name, 
               mp.team_id, mp.minutes_played, mp.season_id
        FROM match_player mp
        WHERE mp.match_id = ?
    """, (match_id,))
    
    match_player_records = cursor.fetchall()
    
    if not match_player_records:
        print(f"No match_player records found for match {match_id}")
        conn.close()
        return False
    
    print(f"Found {len(match_player_records)} match_player records")
    
    # Check if match_player_summary records already exist
    cursor.execute("""
        SELECT COUNT(*)
        FROM match_player_summary mps
        WHERE mps.match_id = ?
    """, (match_id,))
    
    existing_count = cursor.fetchone()[0]
    
    if existing_count > 0:
        print(f"Match {match_id} already has {existing_count} match_player_summary records")
        conn.close()
        return True
    
    # Create match_player_summary records
    records_created = 0
    
    for record in match_player_records:
        match_player_id, match_id, player_id, player_name, team_id, minutes_played, season_id = record
        
        # Generate match_player_summary_id
        summary_id = f"mps_{uuid.uuid4().hex[:8]}"
        
        try:
            cursor.execute("""
                INSERT INTO match_player_summary (
                    match_player_summary_id, match_player_id, match_id, player_id, player_name, 
                    team_id, minutes_played, season_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                summary_id, match_player_id, match_id, player_id, player_name, 
                team_id, minutes_played, season_id
            ))
            
            print(f"Created match_player_summary record for {player_name} (ID: {summary_id})")
            records_created += 1
            
        except sqlite3.Error as e:
            print(f"Error creating record for {player_name}: {e}")
    
    # Commit changes
    conn.commit()
    conn.close()
    
    print(f"\nMatch {match_id} processing complete:")
    print(f"  - Records created: {records_created}")
    
    return records_created > 0

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python create_match_player_summary_records.py <match_id>")
        sys.exit(1)
    
    match_id = sys.argv[1]
    success = create_match_player_summary_records(match_id)
    
    if success:
        print(f"Successfully created match_player_summary records for match {match_id}")
    else:
        print(f"Failed to create match_player_summary records for match {match_id}")