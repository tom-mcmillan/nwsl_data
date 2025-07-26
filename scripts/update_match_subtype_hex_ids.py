#!/usr/bin/env python3
"""
Update match_subtype table to use hex codes starting with mst_
"""

import sqlite3

def update_match_subtype_hex_ids(db_path='data/processed/nwsldata.db'):
    """Update subtype_id values to use hex format with mst_ prefix"""
    
    conn = sqlite3.connect(db_path)
    
    print("🔧 Updating match_subtype IDs to hex format...")
    
    # Define the mapping from old IDs to new hex IDs
    id_mapping = {
        # Playoff subtypes
        'st_playoff_r1': 'mst_a1b2c3d4',
        'st_playoff_qf': 'mst_e5f6g7h8', 
        'st_playoff_sf': 'mst_i9j0k1l2',
        'st_playoff_final': 'mst_m3n4o5p6',
        
        # Challenge Cup subtypes  
        'st_cc_prelim': 'mst_q7r8s9t0',
        'st_cc_qf': 'mst_u1v2w3x4',
        'st_cc_sf': 'mst_y5z6a7b8',
        'st_cc_final': 'mst_c9d0e1f2',
        
        # Fall Series subtypes
        'st_fall_group': 'mst_g3h4i5j6',
        'st_fall_final': 'mst_k7l8m9n0'
    }
    
    # Get current subtypes to verify they exist
    current_subtypes = conn.execute('SELECT subtype_id, subtype_name FROM match_subtype').fetchall()
    print(f"📋 Found {len(current_subtypes)} existing subtypes")
    
    # Step 1: Update any existing match_inventory records to use new IDs
    print("\n🔗 Updating match_inventory references...")
    
    for old_id, new_id in id_mapping.items():
        result = conn.execute('''
            UPDATE match_inventory 
            SET match_subtype_id = ? 
            WHERE match_subtype_id = ?
        ''', (new_id, old_id))
        
        if result.rowcount > 0:
            print(f"  ✅ Updated {result.rowcount} matches: {old_id} → {new_id}")
    
    # Step 2: Update the match_subtype table with new IDs
    print("\n🏗️ Updating match_subtype table...")
    
    for old_id, new_id in id_mapping.items():
        # Get existing record data
        existing = conn.execute('''
            SELECT match_type_id, subtype_name, display_order, description, created_at
            FROM match_subtype WHERE subtype_id = ?
        ''', (old_id,)).fetchone()
        
        if existing:
            match_type_id, subtype_name, display_order, description, created_at = existing
            
            # Insert with new ID
            conn.execute('''
                INSERT OR REPLACE INTO match_subtype 
                (subtype_id, match_type_id, subtype_name, display_order, description, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (new_id, match_type_id, subtype_name, display_order, description, created_at))
            
            # Delete old record
            conn.execute('DELETE FROM match_subtype WHERE subtype_id = ?', (old_id,))
            
            print(f"  ✅ {old_id} → {new_id}: {subtype_name}")
    
    conn.commit()
    
    # Verify the updates
    print("\n🔍 Verifying updated match_subtype table...")
    
    updated_subtypes = conn.execute('''
        SELECT ms.subtype_id, mt.match_type_name, ms.subtype_name, ms.display_order
        FROM match_subtype ms
        JOIN match_type mt ON ms.match_type_id = mt.match_type_id
        ORDER BY mt.match_type_name, ms.display_order
    ''').fetchall()
    
    print("📋 Updated match subtypes:")
    current_type = None
    for subtype_id, type_name, subtype_name, order in updated_subtypes:
        if type_name != current_type:
            print(f"\n   {type_name}:")
            current_type = type_name
        print(f"      {order}. {subtype_name} ({subtype_id})")
    
    # Check match_inventory references
    inventory_refs = conn.execute('''
        SELECT mi.match_subtype_id, COUNT(*) as count
        FROM match_inventory mi
        WHERE mi.match_subtype_id IS NOT NULL
        GROUP BY mi.match_subtype_id
        ORDER BY mi.match_subtype_id
    ''').fetchall()
    
    print(f"\n🔗 Match inventory subtype references:")
    for subtype_id, count in inventory_refs:
        subtype_name = conn.execute(
            'SELECT subtype_name FROM match_subtype WHERE subtype_id = ?', 
            (subtype_id,)
        ).fetchone()
        name = subtype_name[0] if subtype_name else "UNKNOWN"
        print(f"   {subtype_id}: {count} matches ({name})")
    
    conn.close()
    print("\n🎉 Match subtype hex ID update complete!")

if __name__ == "__main__":
    update_match_subtype_hex_ids()