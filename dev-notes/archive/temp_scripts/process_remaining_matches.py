#!/usr/bin/env python3
"""
Process all remaining matches with null statistics.
"""

import sqlite3
import subprocess


def get_null_matches():
    """Get matches with null statistics."""
    db_path = "data/processed/nwsldata.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT DISTINCT match_id 
        FROM match_player_summary 
        WHERE season_id = 2023 AND goals IS NULL
        ORDER BY match_id
    """)

    matches = [row[0] for row in cursor.fetchall()]
    conn.close()
    return matches


def process_match(match_id):
    """Process a single match."""
    try:
        result = subprocess.run(
            ["python", "scripts/populate_match_player_summary.py", match_id],
            capture_output=True,
            text=True,
            timeout=30,
        )
        return result.returncode == 0
    except Exception as e:
        print(f"Error processing {match_id}: {e}")
        return False


def main():
    matches = get_null_matches()
    print(f"Found {len(matches)} matches with null statistics")

    success_count = 0
    failed_matches = []

    for i, match_id in enumerate(matches):
        print(f"Processing {i+1}/{len(matches)}: {match_id}...", end=" ")

        if process_match(match_id):
            print("SUCCESS")
            success_count += 1
        else:
            print("FAILED")
            failed_matches.append(match_id)

    print("\n✅ Results:")
    print(f"  - Successfully processed: {success_count}")
    print(f"  - Failed: {len(failed_matches)}")

    if failed_matches:
        print("\nFailed matches:")
        for match_id in failed_matches[:10]:  # Show first 10
            print(f"  {match_id}")
        if len(failed_matches) > 10:
            print(f"  ... and {len(failed_matches) - 10} more")


if __name__ == "__main__":
    main()
