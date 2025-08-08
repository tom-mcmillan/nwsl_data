#!/usr/bin/env python3
"""
Re-process matches with significant null values after player ID corrections.
"""

import sqlite3
import subprocess


def get_matches_with_nulls():
    """Get matches with null records for players who played >1 minute."""
    db_path = "data/processed/nwsldata.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT DISTINCT mps.match_id 
        FROM match_player_summary mps
        JOIN match_player mp ON mps.match_player_id = mp.match_player_id
        WHERE mps.season_id = '2023' AND mps.goals IS NULL AND mp.minutes_played > 1
        ORDER BY mps.match_id
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
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)


def main():
    matches = get_matches_with_nulls()
    print(f"Found {len(matches)} matches with null values for players who played >1 minute")

    success_count = 0
    failed_matches = []

    for i, match_id in enumerate(matches):
        print(f"Processing {i+1}/{len(matches)}: {match_id}...", end=" ")

        success, stdout, stderr = process_match(match_id)
        if success:
            # Extract updated and skipped counts from output
            lines = stdout.split("\n")
            for line in lines:
                if "Records updated:" in line and "Records skipped:" in line:
                    updated = line.split("Records updated: ")[1].split(",")[0]
                    skipped = line.split("Records skipped: ")[1]
                    print(f"SUCCESS - Updated: {updated}, Skipped: {skipped}")
                    break
            else:
                print("SUCCESS")
            success_count += 1
        else:
            print("FAILED")
            failed_matches.append((match_id, "Processing error"))

    print("\n✅ Results:")
    print(f"  - Successfully processed: {success_count}")
    print(f"  - Failed: {len(failed_matches)}")

    if failed_matches:
        print("\nFailed matches:")
        for match_id, reason in failed_matches:
            print(f"  {match_id}: {reason}")


if __name__ == "__main__":
    main()
