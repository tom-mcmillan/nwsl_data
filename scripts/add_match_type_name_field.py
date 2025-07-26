#!/usr/bin/env python3
"""
Add match_type_name field to match_inventory table for easier eye testing
"""

import sqlite3

class AddMatchTypeNameField:
    def __init__(self, db_path='data/processed/nwsldata.db'):
        self.db_path = db_path
        
    def add_match_type_name_field(self):
        """Add match_type_name column to match_inventory table"""
        print("🔧 Adding match_type_name field to match_inventory table...")
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Add match_type_name column
            conn.execute('''
                ALTER TABLE match_inventory 
                ADD COLUMN match_type_name TEXT
            ''')
            
            print("  ✅ Added match_type_name column")
            
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                print("  ℹ️ match_type_name column already exists")
            else:
                print(f"  ⚠️ Error: {e}")
                return
        
        # Update all existing records with match type names
        updated = conn.execute('''
            UPDATE match_inventory 
            SET match_type_name = (
                SELECT match_type_name 
                FROM match_type 
                WHERE match_type.match_type_id = match_inventory.match_type_id
            )
            WHERE match_type_id IS NOT NULL
        ''').rowcount
        
        conn.commit()
        conn.close()
        
        print(f"  ✅ Updated {updated} records with match type names")
        
    def verify_match_type_name_field(self):
        """Verify the match_type_name field is working correctly"""
        print("🔍 Verifying match_type_name field...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Check a few sample records
        samples = conn.execute('''
            SELECT match_id, match_date, 
                   (SELECT team_name FROM teams WHERE team_id = home_team_id) as home_team,
                   (SELECT team_name FROM teams WHERE team_id = away_team_id) as away_team,
                   match_type_id, match_type_name
            FROM match_inventory 
            WHERE season_id = 2013
            ORDER BY match_date 
            LIMIT 5
        ''').fetchall()
        
        print(f"   📋 Sample matches with match_type_name:")
        for match_id, date, home, away, type_id, type_name in samples:
            print(f"      {match_id}: {home} vs {away} ({date}) - {type_id} ({type_name})")
        
        # Count by match type
        counts = conn.execute('''
            SELECT match_type_name, COUNT(*) as count
            FROM match_inventory 
            GROUP BY match_type_name
            ORDER BY count DESC
        ''').fetchall()
        
        print(f"   📊 Matches by type:")
        for type_name, count in counts:
            print(f"      {type_name}: {count} matches")
        
        conn.close()

if __name__ == "__main__":
    adder = AddMatchTypeNameField()
    adder.add_match_type_name_field()
    adder.verify_match_type_name_field()