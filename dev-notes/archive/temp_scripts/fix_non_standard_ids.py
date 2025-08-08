#!/usr/bin/env python3
"""
Script to fix non-standard mp_99707xxx IDs in match_player and match_player_summary tables.
"""

import sqlite3
import uuid


def generate_new_id():
    """Generate a new standard mp_xxxxxxxx ID."""
    return f"mp_{uuid.uuid4().hex[:8]}"


def fix_non_standard_ids():
    """Fix all non-standard mp_99707xxx IDs."""

    db_path = "data/processed/nwsldata.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("Finding all non-standard mp_99707xxx records...")

    # Get all problematic match_player records
    cursor.execute("""
        SELECT match_player_id, match_id, player_name 
        FROM match_player 
        WHERE match_player_id LIKE 'mp_99707%' 
        ORDER BY match_player_id
    """)

    problematic_records = cursor.fetchall()
    print(f"Found {len(problematic_records)} problematic match_player records")

    # Create mapping of old_id -> new_id
    id_mapping = {}
    for old_mp_id, match_id, player_name in problematic_records:
        new_mp_id = generate_new_id()
        new_mps_id = f"mps_{new_mp_id[3:]}"  # Replace mp_ with mps_

        id_mapping[old_mp_id] = {
            "new_mp_id": new_mp_id,
            "new_mps_id": new_mps_id,
            "match_id": match_id,
            "player_name": player_name,
        }

    print(f"Generated {len(id_mapping)} new ID mappings")

    # Update match_player table
    print("Updating match_player table...")
    for old_mp_id, mapping in id_mapping.items():
        cursor.execute(
            """
            UPDATE match_player 
            SET match_player_id = ? 
            WHERE match_player_id = ?
        """,
            (mapping["new_mp_id"], old_mp_id),
        )

    # Update match_player_summary table
    print("Updating match_player_summary table...")
    for old_mp_id, mapping in id_mapping.items():
        old_mps_id = f"mps_{old_mp_id[3:]}"  # Convert mp_99707xxx to mps_99707xxx

        cursor.execute(
            """
            UPDATE match_player_summary 
            SET match_player_summary_id = ?, match_player_id = ?
            WHERE match_player_summary_id = ?
        """,
            (mapping["new_mps_id"], mapping["new_mp_id"], old_mps_id),
        )

    # Commit changes
    conn.commit()
    conn.close()

    print("✅ Successfully updated all non-standard IDs!")
    print(f"Updated {len(id_mapping)} match_player records")
    print(f"Updated {len(id_mapping)} match_player_summary records")

    return id_mapping


if __name__ == "__main__":
    mapping = fix_non_standard_ids()

    # Print first few mappings for verification
    print("\nSample ID mappings:")
    for i, (old_id, new_data) in enumerate(list(mapping.items())[:5]):
        print(f"  {old_id} -> {new_data['new_mp_id']} (mps: {new_data['new_mps_id']})")
        if i >= 4:
            break
