#!/usr/bin/env python3
"""
Process all 2022 season matches with null statistics.
"""

import sqlite3
import subprocess
import sys

def get_2022_null_matches():
    """Get all 2022 matches with null records."""
    db_path = "data/processed/nwsldata.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT DISTINCT match_id 
        FROM match_player_summary 
        WHERE season_id = '2022' AND goals IS NULL
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
    matches = get_2022_null_matches()
    print(f"Found {len(matches)} matches with null records in 2022 season")
    
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
                    updated = line.split('Records updated: ')[1].split(',')[0]
                    skipped = line.split('Records skipped: ')[1]
                    print(f"SUCCESS - Updated: {updated}, Skipped: {skipped}")
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
        for match_id, reason in failed_matches[:20]:  # Show first 20
            print(f"  {match_id}: {reason}")
        if len(failed_matches) > 20:
            print(f"  ... and {len(failed_matches) - 20} more")

if __name__ == "__main__":
    main()