#!/usr/bin/env python3
"""
Identify all FBRef ID mismatches in 2022 season by running populate script
and collecting all "Warning: No match_player record found" messages.
"""

import sqlite3
import subprocess
import sys
import re
from collections import defaultdict

def get_2022_null_matches():
    """Get 2022 matches with null records for players who played >1 minute."""
    db_path = "data/processed/nwsldata.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT DISTINCT mps.match_id 
        FROM match_player_summary mps
        JOIN match_player mp ON mps.match_player_id = mp.match_player_id
        WHERE mps.season_id = '2022' AND mps.goals IS NULL AND mp.minutes_played > 1
        ORDER BY mps.match_id
    """)
    
    matches = [row[0] for row in cursor.fetchall()]
    conn.close()
    return matches

def extract_fbref_warnings(match_id):
    """Run populate script and extract FBRef ID warnings."""
    try:
        result = subprocess.run(
            ['python', 'scripts/populate_match_player_summary.py', match_id],
            capture_output=True, text=True, timeout=30
        )
        
        # Extract warnings about missing FBRef IDs
        warnings = []
        for line in result.stdout.split('\n'):
            if 'Warning: No match_player record found for FBRef ID' in line:
                # Parse: "Warning: No match_player record found for FBRef ID a179cd70 (Vanessa Gilles)"
                match = re.search(r'FBRef ID (\w+) \(([^)]+)\)', line)
                if match:
                    fbref_id = match.group(1)
                    player_name = match.group(2)
                    warnings.append((fbref_id, player_name))
        
        return warnings
    except Exception as e:
        print(f"Error processing match {match_id}: {e}")
        return []

def main():
    print("Identifying FBRef ID mismatches in 2022 season...")
    
    matches = get_2022_null_matches()
    print(f"Found {len(matches)} matches with null records for players who played >1 minute")
    
    # Collect all FBRef mismatches
    fbref_mismatches = defaultdict(set)  # fbref_id -> set of player names
    processed_matches = 0
    
    for match_id in matches:
        print(f"Processing {processed_matches + 1}/{len(matches)}: {match_id}...", end=" ")
        
        warnings = extract_fbref_warnings(match_id)
        if warnings:
            for fbref_id, player_name in warnings:
                fbref_mismatches[fbref_id].add(player_name)
            print(f"Found {len(warnings)} mismatches")
        else:
            print("No mismatches")
        
        processed_matches += 1
        
        # Process first 10 matches to get a good sample
        if processed_matches >= 10:
            break
    
    print(f"\n📊 FBRef ID Mismatches Found:")
    print(f"{'FBRef ID':<12} {'Player Name(s)'}")
    print("-" * 40)
    
    for fbref_id, player_names in fbref_mismatches.items():
        names_str = ", ".join(sorted(player_names))
        print(f"{fbref_id:<12} {names_str}")
    
    print(f"\nTotal unique FBRef IDs needing correction: {len(fbref_mismatches)}")
    
    # Now get the current player_id for each of these players from our database
    if fbref_mismatches:
        print(f"\n🔍 Current Database Player IDs:")
        db_path = "data/processed/nwsldata.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        all_player_names = set()
        for player_names in fbref_mismatches.values():
            all_player_names.update(player_names)
        
        print(f"{'Player Name':<25} {'Current DB ID':<12} {'Needed FBRef ID':<12}")
        print("-" * 55)
        
        for player_name in sorted(all_player_names):
            cursor.execute("SELECT player_id FROM player WHERE player_name = ?", (player_name,))
            result = cursor.fetchone()
            current_id = result[0] if result else "NOT FOUND"
            
            # Find the FBRef ID this player needs
            needed_fbref_id = None
            for fbref_id, names in fbref_mismatches.items():
                if player_name in names:
                    needed_fbref_id = fbref_id
                    break
            
            print(f"{player_name:<25} {current_id:<12} {needed_fbref_id:<12}")
        
        conn.close()

if __name__ == "__main__":
    main()