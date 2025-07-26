#!/usr/bin/env python3
"""
Create Match Type Table
Reference table for different types of NWSL matches
"""

import sqlite3

class MatchTypeTableCreator:
    def __init__(self, db_path='data/processed/nwsldata.db'):
        self.db_path = db_path
        
    def create_match_type_table(self):
        """Create the match_type table and populate with standard match types"""
        print("🏗️ Creating match_type table...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Drop existing table if it exists
        conn.execute('DROP TABLE IF EXISTS match_type')
        
        # Create new match_type table
        conn.execute('''
            CREATE TABLE match_type (
                match_type_id TEXT PRIMARY KEY,     -- Hex ID starting with mt_
                match_type_name TEXT NOT NULL UNIQUE,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Insert the 4 match types with hex IDs
        match_types = [
            ('mt_a1b2c3d4', 'Regular Season', 'Standard regular season matches during the main NWSL season'),
            ('mt_e5f6g7h8', 'Playoff', 'Post-season playoff matches including semifinals, finals, and championship'),
            ('mt_i9j0k1l2', 'Challenge Cup', 'NWSL Challenge Cup tournament matches (introduced in 2020)'),
            ('mt_m3n4o5p6', 'Fall Series', 'NWSL Fall Series matches (2020 COVID-adjusted season format)')
        ]
        
        for match_type_id, match_type_name, description in match_types:
            conn.execute('''
                INSERT INTO match_type (match_type_id, match_type_name, description)
                VALUES (?, ?, ?)
            ''', (match_type_id, match_type_name, description))
            
            print(f"  ✅ {match_type_id}: {match_type_name}")
        
        conn.commit()
        conn.close()
        
        print("✅ match_type table created successfully")
        
    def verify_match_type_table(self):
        """Verify the match_type table was created correctly"""
        print("🔍 Verifying match_type table...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Get all match types
        match_types = conn.execute('''
            SELECT match_type_id, match_type_name, description 
            FROM match_type 
            ORDER BY match_type_id
        ''').fetchall()
        
        print(f"   📊 Total match types: {len(match_types)}")
        print(f"   📋 Match types:")
        
        for match_type_id, match_type_name, description in match_types:
            print(f"      {match_type_id}: {match_type_name}")
            print(f"         └── {description}")
        
        conn.close()
        
    def add_match_type_to_inventory(self):
        """Add match_type_id column to match_inventory table"""
        print("🔧 Adding match_type_id to match_inventory table...")
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            # Add match_type_id column
            conn.execute('''
                ALTER TABLE match_inventory 
                ADD COLUMN match_type_id TEXT REFERENCES match_type(match_type_id)
            ''')
            
            # Set all current 2013 matches to Regular Season
            conn.execute('''
                UPDATE match_inventory 
                SET match_type_id = 'mt_a1b2c3d4' 
                WHERE season_id = 2013
            ''')
            
            updated_count = conn.execute('''
                SELECT COUNT(*) FROM match_inventory 
                WHERE match_type_id = 'mt_a1b2c3d4'
            ''').fetchone()[0]
            
            conn.commit()
            
            print(f"  ✅ Added match_type_id column to match_inventory")
            print(f"  ✅ Set {updated_count} matches to Regular Season")
            
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                print("  ℹ️ match_type_id column already exists")
            else:
                print(f"  ⚠️ Error: {e}")
        
        conn.close()

if __name__ == "__main__":
    creator = MatchTypeTableCreator()
    creator.create_match_type_table()
    creator.verify_match_type_table()
    creator.add_match_type_to_inventory()