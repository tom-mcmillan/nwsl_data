#!/usr/bin/env python3
"""
Fix player_id values for 8 players in 2022 season and propagate changes
through match_player and match_player_summary tables.
"""

import sqlite3


def fix_2022_player_ids():
    """Update player_id values for identified players."""

    # Mapping from old_id -> (new_id, player_name)
    id_corrections = {
        "c03420f3": ("0835cd1b", "Tinaya Alexander"),
        "bdeab53f": ("504b9126", "Morgan Goff"),
        "331ddc2a": ("2ceac0c2", "Miri Taylor"),
        "1bebf242": ("a428bcd0", "Carly Telford"),
        "dc85a229": ("006619db", "Audrey Harding"),
        "ea2ad9cf": ("9187e9bd", "Averie Collins"),
        "92bc96c0": ("7679f18f", "Devon Kerr"),
        "becbb333": ("eb1483ab", "Carrie Lawrence"),
    }

    db_path = "data/processed/nwsldata.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print(f"Fixing player_id values for {len(id_corrections)} players...")

    total_updated = 0

    for old_id, (new_id, player_name) in id_corrections.items():
        print(f"\n🔄 Updating {player_name}: {old_id} → {new_id}")

        # 1. Update player table
        cursor.execute(
            """
            UPDATE player 
            SET player_id = ?
            WHERE player_id = ?
        """,
            (new_id, old_id),
        )
        player_updates = cursor.rowcount

        # 2. Update match_player table (all seasons)
        cursor.execute(
            """
            UPDATE match_player 
            SET player_id = ?
            WHERE player_id = ?
        """,
            (new_id, old_id),
        )
        mp_updates = cursor.rowcount

        # 3. Update match_player_summary table (all seasons)
        cursor.execute(
            """
            UPDATE match_player_summary 
            SET player_id = ?
            WHERE player_id = ?
        """,
            (new_id, old_id),
        )
        mps_updates = cursor.rowcount

        # 4. Update match_goalkeeper_summary table (all seasons)
        cursor.execute(
            """
            UPDATE match_goalkeeper_summary 
            SET player_id = ?
            WHERE player_id = ?
        """,
            (new_id, old_id),
        )
        mgs_updates = cursor.rowcount

        print("  ✅ Updated records:")
        print(f"     - player: {player_updates}")
        print(f"     - match_player: {mp_updates}")
        print(f"     - match_player_summary: {mps_updates}")
        print(f"     - match_goalkeeper_summary: {mgs_updates}")

        total_updated += player_updates + mp_updates + mps_updates + mgs_updates

    # Commit all changes
    conn.commit()
    conn.close()

    print(f"\n✅ Successfully updated {total_updated} total records across all tables!")
    return len(id_corrections)


if __name__ == "__main__":
    updates = fix_2022_player_ids()
    print("\nReady to re-run populate script on 2022 matches with corrected player IDs.")
