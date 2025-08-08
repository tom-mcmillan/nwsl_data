#!/usr/bin/env python3
"""
Apply FBRef ID mappings for 50 players with null 2023 records.
"""

import sqlite3


def apply_fbref_mappings():
    """Apply all confirmed FBRef ID mappings."""

    # Complete mapping from our spot check
    mappings = {
        # Players that need ID updates
        "74887860": "4e12d471",  # Kirsten Davis
        "470b26b3": "9fef2983",  # Paige Metayer
        "35864ba3": "bf959e17",  # Wang Shuang
        "b999a14e": "6de9ccc5",  # Lena Silano
        "295f4306": "6fe0b3fe",  # Lily Nabet
        "91b864fb": "864383e7",  # Inès Jaurena
        "9a854d95": "05e6b7dc",  # Croix Soto
        "9512b32d": "bb91c574",  # Jillienne Aguilera
        "1998123e": "391e003f",  # Nicole Douglas
        "6d22d022": "39c750b9",  # Frankie Tagliaferri
        "4fcab1f1": "1788cbeb",  # Sierra Enge
        "f86c29df": "0b275490",  # Marissa Sheva
        "c58e149a": "4a1e993d",  # Isabella Briede
        "42e269b6": "0b22aae8",  # Olivia Athens
        "c65b28ec": "c50ff385",  # Rebecca Holloway
        "ffa9361f": "9dc4de50",  # Alex Chidiac
        "9413657a": "b17fdaea",  # Kelsey Turnbow
        "002347a3": "f4d0619c",  # Samantha Fisher
        "994875c3": "fab5ceed",  # Sarah Griffith
        "f8e553a1": "74c78fea",  # Anaïg Butel
        "a5b4bf69": "ebcc0897",  # Madison Elwell
        "6a664f6c": "60c22595",  # Sandra Starke
        "01560b29": "3cf46aa1",  # Jyllissa Harris
        "b774b128": "57ed42de",  # Ouleymata Sarr
        "418082e4": "b9b91bb5",  # Svava Rós Guðmundsdóttir
        "755a959f": "8907c2bd",  # Shelby Hogan
        "c9db7427": "cd3e01dc",  # Andressa
        "ca231f81": "814471bd",  # Claire Lavogez
        "ad4e5eb2": "de10b12b",  # Natalie Beckman
        "a754e77a": "98a2af13",  # Gabby Provenzano
        "01b77eeb": "41880331",  # Stine Ballisager Pedersen
        "7be85ead": "614d8f70",  # Amanda Kowalski
        "0c112128": "6ef00140",  # Hope Breslin
        # Players with same IDs (no update needed, but included for completeness)
        "f7d5735f": "f7d5735f",  # Clarisse Le Bihan
        "3d0e9d13": "3d0e9d13",  # Jenna Nighswonger
        "0a2fdba2": "0a2fdba2",  # Scarlett Camberos
        "f593cab9": "f593cab9",  # Jun Endō
        "57b8b79c": "57b8b79c",  # Naomi Girma
        "27a4ff80": "27a4ff80",  # Sofia Jakobsson
        "dbdabf96": "dbdabf96",  # Elyse Bennett
        "54e15a35": "54e15a35",  # Izzy D'Aquila
        "11865a9d": "11865a9d",  # Julia Lester
        "6772c295": "6772c295",  # Júlia
        "ef1ec0d9": "ef1ec0d9",  # Kerolin
        "6385427b": "6385427b",  # Amirah Ali
        "ab8f6575": "ab8f6575",  # Cameron Tucker
        "8a985938": "8a985938",  # Emina Ekic
        "4ad01d4e": "4ad01d4e",  # Olivia Wingate
        "c4a75e48": "c4a75e48",  # Ryanne Brown
        "d83042df": "d83042df",  # Parker Goins
    }

    db_path = "data/processed/nwsldata.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print(f"Applying FBRef ID mappings for {len(mappings)} players...")

    updates_made = 0

    for old_id, new_id in mappings.items():
        if old_id == new_id:
            print(f"  ✓ {old_id} → {new_id} (no change needed)")
            continue

        # Update match_player table
        cursor.execute(
            """
            UPDATE match_player 
            SET player_id = ?
            WHERE player_id = ? AND season_id = '2023'
        """,
            (new_id, old_id),
        )

        mp_updated = cursor.rowcount

        # Update match_player_summary table
        cursor.execute(
            """
            UPDATE match_player_summary 
            SET player_id = ?
            WHERE player_id = ? AND season_id = '2023'
        """,
            (new_id, old_id),
        )

        mps_updated = cursor.rowcount

        if mp_updated > 0 or mps_updated > 0:
            print(f"  ✅ {old_id} → {new_id} (match_player: {mp_updated}, match_player_summary: {mps_updated})")
            updates_made += 1
        else:
            print(f"  ⚠️ {old_id} → {new_id} (no records found)")

    # Commit changes
    conn.commit()
    conn.close()

    print(f"\n✅ Successfully applied {updates_made} FBRef ID mappings!")
    return updates_made


if __name__ == "__main__":
    updates = apply_fbref_mappings()
    print("\nReady to re-run populate script on affected matches.")
