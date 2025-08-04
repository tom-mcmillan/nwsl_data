#!/usr/bin/env python3
"""
Apply FBRef ID mappings to 2022 season using the same mappings from 2023.
"""

import sqlite3

def apply_2022_fbref_mappings():
    """Apply all confirmed FBRef ID mappings to 2022 season."""
    
    # Same mappings from 2023 that need to be applied to 2022
    mappings = {
        # Players that need ID updates (old_id -> new_id)
        '74887860': '4e12d471',  # Kirsten Davis
        '9413657a': 'b17fdaea',  # Kelsey Turnbow
        'c58e149a': '4a1e993d',  # Isabella Briede
        '994875c3': 'fab5ceed',  # Sarah Griffith
        '9512b32d': 'bb91c574',  # Jillienne Aguilera
        '002347a3': 'f4d0619c',  # Samantha Fisher
        '0c112128': '6ef00140',  # Hope Breslin
        'ca231f81': '814471bd',  # Claire Lavogez
        '295f4306': '6fe0b3fe',  # Lily Nabet
        'a5b4bf69': 'ebcc0897',  # Madison Elwell
        'ad4e5eb2': 'de10b12b',  # Natalie Beckman
        '42e269b6': '0b22aae8',  # Olivia Athens
        'f86c29df': '0b275490',  # Marissa Sheva
        'c65b28ec': 'c50ff385',  # Rebecca Holloway
        'a754e77a': '98a2af13',  # Gabby Provenzano
        'ffa9361f': '9dc4de50',  # Alex Chidiac
        'f8e553a1': '74c78fea',  # Anaïg Butel
        '6a664f6c': '60c22595',  # Sandra Starke
        '01560b29': '3cf46aa1',  # Jyllissa Harris
        'b774b128': '57ed42de',  # Ouleymata Sarr
        '418082e4': 'b9b91bb5',  # Svava Rós Guðmundsdóttir
        '755a959f': '8907c2bd',  # Shelby Hogan
        'c9db7427': 'cd3e01dc',  # Andressa
        '7be85ead': '614d8f70',  # Amanda Kowalski
        '91b864fb': '864383e7',  # Inès Jaurena
        '35864ba3': 'bf959e17',  # Wang Shuang
        'b999a14e': '6de9ccc5',  # Lena Silano
        '9a854d95': '05e6b7dc',  # Croix Soto
        '1998123e': '391e003f',  # Nicole Douglas
        '6d22d022': '39c750b9',  # Frankie Tagliaferri
        '4fcab1f1': '1788cbeb',  # Sierra Enge
        '01b77eeb': '41880331',  # Stine Ballisager Pedersen
        '470b26b3': '9fef2983',  # Paige Metayer
    }
    
    db_path = "data/processed/nwsldata.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print(f"Applying FBRef ID mappings to 2022 season for {len(mappings)} players...")
    
    updates_made = 0
    
    for old_id, new_id in mappings.items():
        # Update match_player table
        cursor.execute("""
            UPDATE match_player 
            SET player_id = ?
            WHERE player_id = ? AND season_id = '2022'
        """, (new_id, old_id))
        
        mp_updated = cursor.rowcount
        
        # Update match_player_summary table
        cursor.execute("""
            UPDATE match_player_summary 
            SET player_id = ?
            WHERE player_id = ? AND season_id = '2022'
        """, (new_id, old_id))
        
        mps_updated = cursor.rowcount
        
        if mp_updated > 0 or mps_updated > 0:
            print(f"  ✅ {old_id} → {new_id} (match_player: {mp_updated}, match_player_summary: {mps_updated})")
            updates_made += 1
        elif old_id in ['f7d5735f', '3d0e9d13', '0a2fdba2', 'f593cab9', '57b8b79c', '27a4ff80', 'dbdabf96', '54e15a35', '11865a9d', '6772c295', 'ef1ec0d9', '6385427b', 'ab8f6575', '8a985938', '4ad01d4e', 'c4a75e48', 'd83042df']:
            print(f"  ✓ {old_id} → {new_id} (no change needed - same ID)")
        else:
            print(f"  ⚠️ {old_id} → {new_id} (no records found)")
    
    # Commit changes
    conn.commit()
    conn.close()
    
    print(f"\n✅ Successfully applied {updates_made} FBRef ID mappings to 2022 season!")
    return updates_made

if __name__ == "__main__":
    updates = apply_2022_fbref_mappings()
    print(f"\nReady to re-run populate script on affected 2022 matches.")