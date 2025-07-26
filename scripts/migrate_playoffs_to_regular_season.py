#!/usr/bin/env python3
"""
Migrate playoff matches to be subtypes of Regular Season instead of separate match type
"""

import sqlite3

def migrate_playoffs_to_regular_season(db_path='data/processed/nwsldata.db'):
    """Migrate playoff structure to be subtypes of Regular Season"""
    
    conn = sqlite3.connect(db_path)
    
    print("🔄 Starting playoff migration to Regular Season subtypes...")
    
    # Step 1: Get current state
    print("\n📊 Current match type distribution:")
    current_counts = conn.execute('''
        SELECT match_type_name, COUNT(*) as count
        FROM match_inventory 
        GROUP BY match_type_name
        ORDER BY match_type_name
    ''').fetchall()
    
    for type_name, count in current_counts:
        print(f"   {type_name}: {count} matches")
    
    # Step 2: Get Regular Season match_type_id
    regular_season_result = conn.execute('''
        SELECT match_type_id FROM match_type WHERE match_type_name = 'Regular Season'
    ''').fetchone()
    
    if not regular_season_result:
        print("❌ Error: Regular Season match type not found!")
        return False
    
    regular_season_id = regular_season_result[0]
    print(f"\n🎯 Regular Season match_type_id: {regular_season_id}")
    
    # Step 3: Update playoff matches in match_inventory
    print("\n🔄 Updating playoff matches to Regular Season...")
    
    playoff_matches = conn.execute('''
        SELECT match_id, match_date, match_subtype_name, 
               (SELECT team_name FROM teams WHERE team_id = home_team_id) as home,
               (SELECT team_name FROM teams WHERE team_id = away_team_id) as away
        FROM match_inventory 
        WHERE match_type_name = 'Playoff'
        ORDER BY season_id, match_date
    ''').fetchall()
    
    print(f"   Found {len(playoff_matches)} playoff matches to migrate")
    
    # Show sample of what will be migrated
    if playoff_matches:
        print("   Sample playoff matches being migrated:")
        for match_id, date, subtype, home, away in playoff_matches[:5]:
            print(f"      {match_id}: {home} vs {away} ({date}) - {subtype}")
        if len(playoff_matches) > 5:
            print(f"      ... and {len(playoff_matches) - 5} more")
    
    # Update the matches
    update_result = conn.execute('''
        UPDATE match_inventory 
        SET match_type_id = ?, match_type_name = 'Regular Season'
        WHERE match_type_name = 'Playoff'
    ''', (regular_season_id,))
    
    print(f"   ✅ Updated {update_result.rowcount} playoff matches to Regular Season")
    
    # Step 4: Update playoff subtypes to be scoped to Regular Season
    print("\n🔄 Updating playoff subtypes to Regular Season scope...")
    
    playoff_subtypes = conn.execute('''
        SELECT ms.subtype_id, ms.subtype_name, ms.display_order, ms.description
        FROM match_subtype ms
        JOIN match_type mt ON ms.match_type_id = mt.match_type_id
        WHERE mt.match_type_name = 'Playoff'
    ''').fetchall()
    
    for subtype_id, subtype_name, display_order, description in playoff_subtypes:
        # Update the subtype to reference Regular Season instead of Playoff
        conn.execute('''
            UPDATE match_subtype 
            SET match_type_id = ?
            WHERE subtype_id = ?
        ''', (regular_season_id, subtype_id))
        
        print(f"   ✅ Updated subtype: {subtype_name} ({subtype_id}) → Regular Season")
    
    # Step 5: Remove the Playoff match type (after confirming no references)
    print("\n🗑️ Removing Playoff match type...")
    
    # Check if any matches still reference Playoff
    remaining_playoff_refs = conn.execute('''
        SELECT COUNT(*) FROM match_inventory WHERE match_type_name = 'Playoff'
    ''').fetchone()[0]
    
    if remaining_playoff_refs > 0:
        print(f"   ⚠️ Warning: {remaining_playoff_refs} matches still reference Playoff - not removing match type")
    else:
        # Safe to remove Playoff match type
        conn.execute('''
            DELETE FROM match_type WHERE match_type_name = 'Playoff'
        ''')
        print("   ✅ Removed Playoff match type")
    
    conn.commit()
    
    # Step 6: Verification
    print("\n🔍 Verifying migration results...")
    
    # Check new match type distribution
    new_counts = conn.execute('''
        SELECT match_type_name, COUNT(*) as count
        FROM match_inventory 
        GROUP BY match_type_name
        ORDER BY match_type_name
    ''').fetchall()
    
    print("📊 New match type distribution:")
    for type_name, count in new_counts:
        print(f"   {type_name}: {count} matches")
    
    # Check Regular Season subtypes
    regular_season_subtypes = conn.execute('''
        SELECT ms.subtype_name, COUNT(mi.match_id) as match_count, ms.display_order
        FROM match_subtype ms
        LEFT JOIN match_inventory mi ON ms.subtype_id = mi.match_subtype_id
        WHERE ms.match_type_id = ?
        GROUP BY ms.subtype_id, ms.subtype_name, ms.display_order
        ORDER BY ms.display_order
    ''', (regular_season_id,)).fetchall()
    
    print(f"\n📊 Regular Season subtypes (total with {regular_season_id}):")
    for subtype_name, match_count, order in regular_season_subtypes:
        print(f"   {order}. {subtype_name}: {match_count} matches")
    
    # Show sample of migrated matches
    sample_migrated = conn.execute('''
        SELECT mi.match_id, mi.match_date, mi.match_type_name, mi.match_subtype_name, mi.season_id,
               (SELECT team_name FROM teams WHERE team_id = mi.home_team_id) as home,
               (SELECT team_name FROM teams WHERE team_id = mi.away_team_id) as away
        FROM match_inventory mi
        WHERE mi.match_subtype_name IN ('First Round', 'Quarterfinals', 'Semifinals', 'Final')
              AND mi.match_type_name = 'Regular Season'
        ORDER BY mi.season_id DESC, mi.match_date DESC
        LIMIT 10
    ''').fetchall()
    
    print(f"\n🏆 Sample migrated playoff matches (now Regular Season subtypes):")
    for match_id, date, type_name, subtype_name, season, home, away in sample_migrated:
        print(f"   {match_id}: {home} vs {away} ({season} {subtype_name}) - {type_name}")
    
    # Final totals
    total_matches = sum(count for _, count in new_counts)
    print(f"\n🎯 Migration complete! Total matches: {total_matches}")
    
    # Check remaining match types
    remaining_types = conn.execute('SELECT match_type_name FROM match_type ORDER BY match_type_name').fetchall()
    print(f"📋 Remaining match types: {[name[0] for name in remaining_types]}")
    
    conn.close()
    return True

if __name__ == "__main__":
    success = migrate_playoffs_to_regular_season()
    if success:
        print("\n🎉 Playoff migration completed successfully!")
    else:
        print("\n❌ Migration failed!")