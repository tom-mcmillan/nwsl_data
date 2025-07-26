#!/usr/bin/env python3
"""
Create match_subtype table and populate with scoped subtypes
"""

import sqlite3

def create_match_subtype_table(db_path='data/processed/nwsldata.db'):
    """Create match_subtype table with referential integrity"""
    
    conn = sqlite3.connect(db_path)
    
    print("🏗️ Creating match_subtype table...")
    
    # Create match_subtype table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS match_subtype (
            subtype_id TEXT PRIMARY KEY,           -- e.g., 'st_playoff_qf', 'st_cc_prelim'
            match_type_id TEXT NOT NULL,           -- FK to match_type
            subtype_name TEXT NOT NULL,            -- 'Quarterfinals', 'Preliminary Round'
            display_order INTEGER,                 -- For sorting (1=First Round, 2=QF, etc.)
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            FOREIGN KEY (match_type_id) REFERENCES match_type(match_type_id)
        )
    ''')
    
    print("✅ match_subtype table created")
    
    # Get existing match_type_ids
    match_types = conn.execute('SELECT match_type_id, match_type_name FROM match_type').fetchall()
    print(f"📋 Found match types: {[name for _, name in match_types]}")
    
    # Define subtypes for each competition type
    subtypes = []
    
    # Regular Season - no subtypes (NULL in match_inventory)
    # Regular season matches don't need subtypes
    
    # Playoff subtypes (scoped to playoffs)
    playoff_type_id = None
    for type_id, type_name in match_types:
        if type_name == 'Playoff':
            playoff_type_id = type_id
            break
    
    if playoff_type_id:
        subtypes.extend([
            ('st_playoff_r1', playoff_type_id, 'First Round', 1, 'Playoff first round matches'),
            ('st_playoff_qf', playoff_type_id, 'Quarterfinals', 2, 'Playoff quarterfinal matches'),
            ('st_playoff_sf', playoff_type_id, 'Semifinals', 3, 'Playoff semifinal matches'),
            ('st_playoff_final', playoff_type_id, 'Final', 4, 'Playoff final/championship match')
        ])
    
    # Challenge Cup subtypes (scoped to Challenge Cup)
    cc_type_id = None
    for type_id, type_name in match_types:
        if type_name == 'Challenge Cup':
            cc_type_id = type_id
            break
    
    if cc_type_id:
        subtypes.extend([
            ('st_cc_prelim', cc_type_id, 'Preliminary Round', 1, 'Challenge Cup preliminary round matches'),
            ('st_cc_qf', cc_type_id, 'Quarterfinals', 2, 'Challenge Cup quarterfinal matches'),
            ('st_cc_sf', cc_type_id, 'Semifinals', 3, 'Challenge Cup semifinal matches'),
            ('st_cc_final', cc_type_id, 'Final', 4, 'Challenge Cup final match')
        ])
    
    # Fall Series subtypes (if needed - COVID 2020 format)
    fall_type_id = None
    for type_id, type_name in match_types:
        if type_name == 'Fall Series':
            fall_type_id = type_id
            break
    
    if fall_type_id:
        subtypes.extend([
            ('st_fall_group', fall_type_id, 'Group Stage', 1, 'Fall Series group stage matches'),
            ('st_fall_final', fall_type_id, 'Final', 2, 'Fall Series final match')
        ])
    
    # Insert all subtypes
    print(f"📝 Inserting {len(subtypes)} match subtypes...")
    
    for subtype_id, match_type_id, subtype_name, display_order, description in subtypes:
        conn.execute('''
            INSERT OR REPLACE INTO match_subtype 
            (subtype_id, match_type_id, subtype_name, display_order, description)
            VALUES (?, ?, ?, ?, ?)
        ''', (subtype_id, match_type_id, subtype_name, display_order, description))
        
        print(f"  ✅ {subtype_id}: {subtype_name}")
    
    # Add match_subtype_id column to match_inventory if it doesn't exist
    print("🔧 Adding match_subtype_id column to match_inventory...")
    
    try:
        conn.execute('ALTER TABLE match_inventory ADD COLUMN match_subtype_id TEXT REFERENCES match_subtype(subtype_id)')
        print("✅ match_subtype_id column added to match_inventory")
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            print("✅ match_subtype_id column already exists")
        else:
            raise e
    
    conn.commit()
    
    # Verify the setup
    print("\n🔍 Verifying match_subtype table...")
    
    subtype_counts = conn.execute('''
        SELECT mt.match_type_name, COUNT(ms.subtype_id) as subtype_count
        FROM match_type mt
        LEFT JOIN match_subtype ms ON mt.match_type_id = ms.match_type_id
        GROUP BY mt.match_type_id, mt.match_type_name
        ORDER BY mt.match_type_name
    ''').fetchall()
    
    print("📊 Subtypes by match type:")
    for type_name, count in subtype_counts:
        print(f"   {type_name}: {count} subtypes")
    
    # Show all subtypes
    all_subtypes = conn.execute('''
        SELECT ms.subtype_id, mt.match_type_name, ms.subtype_name, ms.display_order
        FROM match_subtype ms
        JOIN match_type mt ON ms.match_type_id = mt.match_type_id
        ORDER BY mt.match_type_name, ms.display_order
    ''').fetchall()
    
    print("\n📋 All match subtypes:")
    current_type = None
    for subtype_id, type_name, subtype_name, order in all_subtypes:
        if type_name != current_type:
            print(f"\n   {type_name}:")
            current_type = type_name
        print(f"      {order}. {subtype_name} ({subtype_id})")
    
    conn.close()
    print("\n🎉 Match subtype setup complete!")

if __name__ == "__main__":
    create_match_subtype_table()