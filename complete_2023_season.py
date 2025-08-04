#!/usr/bin/env python3
"""
Complete processing of all remaining null records in 2023 season match_player_summary.
"""

import sqlite3
import subprocess
import sys

def get_2023_matches_with_nulls():
    """Get all 2023 matches that still have null records."""
    db_path = "data/processed/nwsldata.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT DISTINCT match_id 
        FROM match_player_summary 
        WHERE season_id = '2023' AND goals IS NULL
        ORDER BY match_id
    """)
    
    matches = [row[0] for row in cursor.fetchall()]
    conn.close()
    return matches

def process_match(match_id):
    """Process a single match."""
    try:
        result = subprocess.run(
            ['python', 'scripts/populate_match_player_summary.py', match_id],
            capture_output=True, text=True, timeout=30
        )
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)

def main():
    matches = get_2023_matches_with_nulls()
    print(f"Found {len(matches)} matches with null records in 2023 season")
    
    success_count = 0
    failed_matches = []
    
    for i, match_id in enumerate(matches):
        print(f"Processing {i+1}/{len(matches)}: {match_id}...", end=" ")
        
        success, stdout, stderr = process_match(match_id)
        if success:
            # Extract updated and skipped counts from output
            lines = stdout.split('\n')
            for line in lines:
                if 'Records updated:' in line and 'Records skipped:' in line:
                    print(f"SUCCESS - {line.strip()}")
                    break
            else:
                print("SUCCESS")
            success_count += 1
        else:
            print("FAILED")
            if "HTML file not found" in stderr:
                failed_matches.append((match_id, "No HTML file"))
            else:
                failed_matches.append((match_id, "Processing error"))
    
    print(f"\n✅ Results:")
    print(f"  - Successfully processed: {success_count}")
    print(f"  - Failed: {len(failed_matches)}")
    
    if failed_matches:
        print(f"\nFailed matches:")
        for match_id, reason in failed_matches[:10]:  # Show first 10
            print(f"  {match_id}: {reason}")
        if len(failed_matches) > 10:
            print(f"  ... and {len(failed_matches) - 10} more")

if __name__ == "__main__":
    main()