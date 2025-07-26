#!/usr/bin/env python3
"""
Assign proper subtypes to migrated playoff matches based on their filename/origin
"""

import sqlite3

def assign_playoff_subtypes(db_path='data/processed/nwsldata.db'):
    """Assign subtypes to playoff matches that were migrated to Regular Season"""
    
    conn = sqlite3.connect(db_path)
    
    print("🏷️ Assigning subtypes to migrated playoff matches...")
    
    # Get playoff matches that need subtypes (those that came from CSV with specific playoff types)
    # We'll identify them by their extraction_status and season patterns
    
    # Mapping of CSV playoff names to our subtype IDs
    subtype_mapping = {
        'Semifinals': 'mst_i9j0k1l2',
        'Semi-finals': 'mst_i9j0k1l2', 
        'Final': 'mst_m3n4o5p6',
        'Quarterfinals': 'mst_e5f6g7h8',
        'First Round': 'mst_a1b2c3d4',
        'First round': 'mst_a1b2c3d4'
    }
    
    # We need to look at specific match IDs that we know were playoffs from the CSV data
    # Based on the grep results, let's identify some key playoff matches
    
    known_playoff_matches = {
        # 2024 playoffs (from the CSV data we saw)
        '66eef21d': ('Quarterfinals', 'mst_e5f6g7h8'),
        '5fef737e': ('Quarterfinals', 'mst_e5f6g7h8'),
        '70f6ed43': ('Quarterfinals', 'mst_e5f6g7h8'),
        'e96c892d': ('Quarterfinals', 'mst_e5f6g7h8'),
        '87c96d20': ('Semifinals', 'mst_i9j0k1l2'),
        'd711b57e': ('Semifinals', 'mst_i9j0k1l2'),
        'f168f5b1': ('Final', 'mst_m3n4o5p6'),
        
        # 2023 playoffs
        'f2f758a1': ('First Round', 'mst_a1b2c3d4'),
        'd6b5ef91': ('Semifinals', 'mst_i9j0k1l2'),
        '5cc2c4e6': ('Semifinals', 'mst_i9j0k1l2'),
        '9d2567b5': ('Final', 'mst_m3n4o5p6'),
        
        # 2022 playoffs
        '063be4e2': ('First Round', 'mst_a1b2c3d4'),
        '254dd7b3': ('Semifinals', 'mst_i9j0k1l2'),
        '4ca190d3': ('Semifinals', 'mst_i9j0k1l2'),
        '64aa1dab': ('Final', 'mst_m3n4o5p6'),
        
        # 2021 playoffs
        '725b6b2b': ('First Round', 'mst_a1b2c3d4'),
        '441fc804': ('Semifinals', 'mst_i9j0k1l2'),
        '9211e53e': ('Semifinals', 'mst_i9j0k1l2'),
        'd7eebcbd': ('Final', 'mst_m3n4o5p6'),
        
        # 2019 playoffs
        '6b7e06cd': ('Semifinals', 'mst_i9j0k1l2'),
        '51f18293': ('Semifinals', 'mst_i9j0k1l2'),
        '9f7344bb': ('Final', 'mst_m3n4o5p6'),
        
        # 2018 playoffs
        'e17daaeb': ('Semifinals', 'mst_i9j0k1l2'),
        '2c32f3d3': ('Semifinals', 'mst_i9j0k1l2'),
        '22f04f44': ('Final', 'mst_m3n4o5p6'),
        
        # 2017 playoffs
        '6ff3d866': ('Semifinals', 'mst_i9j0k1l2'),
        'c91958d4': ('Semifinals', 'mst_i9j0k1l2'),
        '5206d9f2': ('Final', 'mst_m3n4o5p6'),
        
        # 2016 playoffs
        '67767cee': ('Semifinals', 'mst_i9j0k1l2'),
        'fbc0a335': ('Semifinals', 'mst_i9j0k1l2'),
        '55312976': ('Final', 'mst_m3n4o5p6'),
        
        # 2015 playoffs
        '093ed426': ('Semifinals', 'mst_i9j0k1l2'),
        '9bd6f453': ('Semifinals', 'mst_i9j0k1l2'),
        'c09cea95': ('Final', 'mst_m3n4o5p6'),
        
        # 2014 playoffs
        '217a40ab': ('Semifinals', 'mst_i9j0k1l2'),
        'f881e2c1': ('Semifinals', 'mst_i9j0k1l2'),
        '60f738e9': ('Final', 'mst_m3n4o5p6'),
        
        # 2013 playoffs
        'f481adf8': ('Semifinals', 'mst_i9j0k1l2'),
        '6c5b0538': ('Semifinals', 'mst_i9j0k1l2'),
        'ba3972b': ('Final', 'mst_m3n4o5p6')
    }
    
    successful_updates = 0
    failed_updates = 0
    
    print(f"🎯 Updating {len(known_playoff_matches)} known playoff matches...")
    
    for match_id, (subtype_name, subtype_id) in known_playoff_matches.items():
        # Check if match exists and update it
        existing = conn.execute('''
            SELECT match_id, match_date, season_id,
                   (SELECT team_name FROM teams WHERE team_id = home_team_id) as home,
                   (SELECT team_name FROM teams WHERE team_id = away_team_id) as away
            FROM match_inventory 
            WHERE match_id = ? AND match_type_name = 'Regular Season'
        ''', (match_id,)).fetchone()
        
        if existing:
            # Update with subtype
            conn.execute('''
                UPDATE match_inventory 
                SET match_subtype_id = ?, match_subtype_name = ?
                WHERE match_id = ?
            ''', (subtype_id, subtype_name, match_id))
            
            successful_updates += 1
            match_id_found, date, season, home, away = existing
            print(f"  ✅ {match_id}: {home} vs {away} ({season} {subtype_name})")
        else:
            failed_updates += 1
            print(f"  ❌ {match_id}: Match not found or not Regular Season")
    
    conn.commit()
    
    # Verification
    print(f"\n📊 Update Summary:")
    print(f"   ✅ Successfully updated: {successful_updates}")
    print(f"   ❌ Failed to update: {failed_updates}")
    
    # Show subtype distribution for Regular Season
    subtype_counts = conn.execute('''
        SELECT ms.subtype_name, COUNT(mi.match_id) as count, ms.display_order
        FROM match_subtype ms
        LEFT JOIN match_inventory mi ON ms.subtype_id = mi.match_subtype_id 
        WHERE ms.match_type_id = 'mt_a1b2c3d4'  -- Regular Season
        GROUP BY ms.subtype_id, ms.subtype_name, ms.display_order
        ORDER BY ms.display_order
    ''').fetchall()
    
    print(f"\n📊 Regular Season subtype distribution:")
    total_with_subtypes = 0
    for subtype_name, count, order in subtype_counts:
        print(f"   {order}. {subtype_name}: {count} matches")
        total_with_subtypes += count
    
    # Count regular season matches without subtypes
    no_subtype_count = conn.execute('''
        SELECT COUNT(*) FROM match_inventory 
        WHERE match_type_name = 'Regular Season' AND match_subtype_id IS NULL
    ''').fetchone()[0]
    
    print(f"   Regular season (no subtype): {no_subtype_count} matches")
    print(f"   Total Regular Season: {total_with_subtypes + no_subtype_count} matches")
    
    # Show sample of each playoff subtype
    for subtype_name, count, order in subtype_counts:
        if count > 0:
            samples = conn.execute('''
                SELECT mi.match_id, mi.match_date, mi.season_id,
                       (SELECT team_name FROM teams WHERE team_id = mi.home_team_id) as home,
                       (SELECT team_name FROM teams WHERE team_id = mi.away_team_id) as away
                FROM match_inventory mi
                JOIN match_subtype ms ON mi.match_subtype_id = ms.subtype_id
                WHERE ms.subtype_name = ? AND mi.match_type_name = 'Regular Season'
                ORDER BY mi.season_id DESC, mi.match_date DESC
                LIMIT 3
            ''', (subtype_name,)).fetchall()
            
            print(f"\n   🏆 Sample {subtype_name} matches:")
            for match_id, date, season, home, away in samples:
                print(f"      {match_id}: {home} vs {away} ({season} {date})")
    
    conn.close()
    print(f"\n🎉 Playoff subtype assignment complete!")
    return successful_updates

if __name__ == "__main__":
    updated = assign_playoff_subtypes()
    print(f"Updated {updated} playoff matches with subtypes")