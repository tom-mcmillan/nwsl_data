#!/usr/bin/env python3
"""
Create nation table and normalize nationality data
- Extract valid nations from messy nationality column
- Generate hex nation_ids with na_ prefix
- Update player table to use nation_id FK
"""

import sqlite3
import hashlib
import re

class NationTableCreator:
    def __init__(self, db_path='data/processed/nwsldata.db'):
        self.db_path = db_path
        
        # Clean mapping of messy nationality data to standard codes
        self.nationality_cleanup = {
            # US variations
            'us USA': 'USA',
            'USA': 'USA',
            
            # Canada variations  
            'ca CAN': 'CAN',
            'CAN': 'CAN',
            
            # Mexico variations
            'mx MEX': 'MEX', 
            'MEX': 'MEX',
            
            # Australia variations
            'au AUS': 'AUS',
            'AUS': 'AUS',
            
            # Germany variations
            'de GER': 'GER',
            'GER': 'GER',
            
            # England variations
            'eng ENG': 'ENG',
            'ENG': 'ENG',
            
            # Spain variations
            'es ESP': 'ESP',
            'ESP': 'ESP',
            
            # New Zealand variations
            'nz NZL': 'NZL',
            'NZL': 'NZL',
            
            # Japan variations
            'jp JPN': 'JPN',
            'JPN': 'JPN',
            
            # Other mappings for inconsistent codes
            'ws SAM': 'WSM',  # Samoa
            'wls WAL': 'WAL', # Wales  
            'tt TRI': 'TTO',  # Trinidad & Tobago
            'sct SCO': 'SCO', # Scotland
            'pl POL': 'POL',  # Poland
            'no NOR': 'NOR',  # Norway
            'ng NGA': 'NGA',  # Nigeria
            'jm JAM': 'JAM',  # Jamaica
            'ie IRL': 'IRL',  # Ireland
            'dk DEN': 'DEN',  # Denmark
            'cm CMR': 'CMR',  # Cameroon
            'br BRA': 'BRA',  # Brazil
            'ba BIH': 'BIH',  # Bosnia
            'at AUT': 'AUT',  # Austria
            
            # Standard 3-letter codes (keep as-is)
            'BRA': 'BRA', 'FRA': 'FRA', 'SWE': 'SWE', 'DEN': 'DEN',
            'NGA': 'NGA', 'ZAM': 'ZAM', 'VEN': 'VEN', 'KOR': 'KOR',
            'COL': 'COL', 'SCO': 'SCO', 'WAL': 'WAL', 'IRL': 'IRL',
            'POL': 'POL', 'NOR': 'NOR', 'JAM': 'JAM', 'CMR': 'CMR',
            'BIH': 'BIH', 'AUT': 'AUT', 'SUI': 'SUI', 'POR': 'POR',
            'NED': 'NED', 'MWI': 'MWI', 'KEN': 'KEN', 'HAI': 'HAI',
            'GUA': 'GUA', 'GHA': 'GHA', 'FIN': 'FIN', 'BER': 'BER',
            'CIV': 'CIV', 'ANG': 'ANG'
        }
        
        # Full nation names for the codes
        self.nation_names = {
            'USA': 'United States',
            'CAN': 'Canada', 
            'MEX': 'Mexico',
            'AUS': 'Australia',
            'GER': 'Germany',
            'ENG': 'England',
            'ESP': 'Spain',
            'NZL': 'New Zealand',
            'JPN': 'Japan',
            'BRA': 'Brazil',
            'FRA': 'France', 
            'SWE': 'Sweden',
            'DEN': 'Denmark',
            'NGA': 'Nigeria',
            'ZAM': 'Zambia',
            'VEN': 'Venezuela',
            'KOR': 'South Korea',
            'COL': 'Colombia',
            'SCO': 'Scotland',
            'WAL': 'Wales',
            'IRL': 'Ireland',
            'POL': 'Poland',
            'NOR': 'Norway',
            'JAM': 'Jamaica',
            'CMR': 'Cameroon',
            'BIH': 'Bosnia and Herzegovina',
            'AUT': 'Austria',
            'SUI': 'Switzerland',
            'POR': 'Portugal',
            'NED': 'Netherlands',
            'MWI': 'Malawi',
            'KEN': 'Kenya',
            'HAI': 'Haiti',
            'GUA': 'Guatemala',
            'GHA': 'Ghana',
            'FIN': 'Finland',
            'BER': 'Bermuda',
            'CIV': 'Ivory Coast',
            'ANG': 'Angola',
            'WSM': 'Samoa',
            'TTO': 'Trinidad and Tobago'
        }
        
    def generate_nation_id(self, nation_code):
        """Generate consistent hex nation_id from nation code"""
        # Create hash from nation code for consistency
        hash_input = f"nation_{nation_code}".encode('utf-8')
        hash_obj = hashlib.md5(hash_input)
        hex_hash = hash_obj.hexdigest()[:8]  # Take first 8 characters
        return f"na_{hex_hash}"
        
    def is_valid_nation_code(self, text):
        """Check if text is a valid nation code (not a player name)"""
        if not text or len(text.strip()) == 0:
            return False
            
        text = text.strip()
        
        # If it's in our cleanup mapping, it's valid
        if text in self.nationality_cleanup:
            return True
            
        # Skip obvious player names (contains spaces and looks like names)
        if ' ' in text and len(text) > 6:
            # Check if it looks like "First Last" pattern
            words = text.split()
            if len(words) >= 2 and all(word[0].isupper() and word[1:].islower() for word in words):
                return False
                
        # Skip very long strings that look like names
        if len(text) > 10:
            return False
            
        return True
        
    def extract_valid_nations(self):
        """Extract and clean nation codes from current nationality data"""
        print("🔍 Extracting valid nations from nationality data...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Get all unique nationality values
        raw_nationalities = conn.execute('''
            SELECT DISTINCT nationality, COUNT(*) as count
            FROM player 
            WHERE nationality IS NOT NULL AND nationality != ''
            GROUP BY nationality
            ORDER BY count DESC
        ''').fetchall()
        
        print(f"   📊 Found {len(raw_nationalities)} unique nationality values")
        
        valid_nations = set()
        invalid_count = 0
        
        for nationality, count in raw_nationalities:
            if self.is_valid_nation_code(nationality):
                # Clean the nationality code
                clean_code = self.nationality_cleanup.get(nationality, nationality)
                valid_nations.add(clean_code)
                print(f"   ✅ {nationality} → {clean_code} ({count} players)")
            else:
                print(f"   ❌ Skipping invalid: {nationality} ({count} players)")
                invalid_count += 1
                
        conn.close()
        
        print(f"\n   📈 Results: {len(valid_nations)} valid nations, {invalid_count} invalid entries")
        return sorted(valid_nations)
        
    def populate_nation_table(self, nation_codes):
        """Populate the nation table with nation codes and hex IDs"""
        print(f"\n🌍 Populating nation table with {len(nation_codes)} nations...")
        
        conn = sqlite3.connect(self.db_path)
        
        for nation_code in nation_codes:
            nation_id = self.generate_nation_id(nation_code)
            nation_name = self.nation_names.get(nation_code, nation_code)
            
            conn.execute('''
                INSERT OR REPLACE INTO nation (nation_id, nation_code, nation_name)
                VALUES (?, ?, ?)
            ''', (nation_id, nation_code, nation_name))
            
            print(f"   ➕ {nation_id}: {nation_code} ({nation_name})")
            
        conn.commit()
        conn.close()
        
        print(f"   ✅ Nation table populated with {len(nation_codes)} nations")
        
    def update_player_table_structure(self):
        """Update player table to use nation_id instead of nationality"""
        print(f"\n🔧 Updating player table structure...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Add nation_id column
        try:
            conn.execute('ALTER TABLE player ADD COLUMN nation_id TEXT REFERENCES nation(nation_id)')
            print("   ➕ Added nation_id column to player table")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                print("   ⚠️ nation_id column already exists")
            else:
                raise
                
        conn.close()
        
    def migrate_player_nationalities(self):
        """Migrate existing nationality data to nation_id references"""
        print(f"\n📋 Migrating player nationality data...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Get all players with nationality data
        players = conn.execute('''
            SELECT player_id, player_name, nationality 
            FROM player 
            WHERE nationality IS NOT NULL AND nationality != ''
        ''').fetchall()
        
        migrated_count = 0
        skipped_count = 0
        
        for player_id, player_name, nationality in players:
            if self.is_valid_nation_code(nationality):
                # Clean the nationality and find corresponding nation_id
                clean_code = self.nationality_cleanup.get(nationality, nationality)
                nation_id = self.generate_nation_id(clean_code)
                
                # Update player with nation_id
                conn.execute('''
                    UPDATE player 
                    SET nation_id = ?
                    WHERE player_id = ?
                ''', (nation_id, player_id))
                
                migrated_count += 1
            else:
                # Clear invalid nationality data
                conn.execute('''
                    UPDATE player 
                    SET nationality = NULL
                    WHERE player_id = ?
                ''', (player_id,))
                skipped_count += 1
                
        conn.commit()
        
        # Verify migration
        players_with_nation = conn.execute('''
            SELECT COUNT(*) FROM player WHERE nation_id IS NOT NULL
        ''').fetchone()[0]
        
        print(f"   ✅ Migrated {migrated_count} players to nation_id")
        print(f"   🧹 Cleared {skipped_count} invalid nationality entries")
        print(f"   📊 Total players with nation_id: {players_with_nation}")
        
        conn.close()
        
    def verify_migration(self):
        """Verify the nation table and migration results"""
        print(f"\n🔍 Verifying nation table and migration...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Nation table stats
        nation_count = conn.execute('SELECT COUNT(*) FROM nation').fetchone()[0]
        print(f"   🌍 Nations in table: {nation_count}")
        
        # Player migration stats
        players_with_nation = conn.execute('''
            SELECT COUNT(*) FROM player WHERE nation_id IS NOT NULL
        ''').fetchone()[0]
        total_players = conn.execute('SELECT COUNT(*) FROM player').fetchone()[0]
        
        print(f"   👥 Players with nation: {players_with_nation}/{total_players}")
        
        # Top nations by player count
        top_nations = conn.execute('''
            SELECT n.nation_code, n.nation_name, COUNT(p.player_id) as player_count
            FROM nation n
            LEFT JOIN player p ON n.nation_id = p.nation_id
            GROUP BY n.nation_id, n.nation_code, n.nation_name
            ORDER BY player_count DESC
            LIMIT 10
        ''').fetchall()
        
        print(f"\n   🏆 Top nations by player count:")
        for nation_code, nation_name, count in top_nations:
            print(f"      {nation_code} ({nation_name}): {count} players")
            
        conn.close()
        
    def cleanup_old_nationality_column(self):
        """Remove the old nationality column (optional)"""
        print(f"\n🧹 Removing old nationality column...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Create new table without nationality column
        conn.execute('''
            CREATE TABLE player_new (
                player_id TEXT PRIMARY KEY,
                player_name TEXT NOT NULL,
                nation_id TEXT REFERENCES nation(nation_id),
                dob DATE
            )
        ''')
        
        # Copy data to new table
        conn.execute('''
            INSERT INTO player_new (player_id, player_name, nation_id, dob)
            SELECT player_id, player_name, nation_id, dob
            FROM player
        ''')
        
        # Replace old table
        conn.execute('DROP TABLE player')
        conn.execute('ALTER TABLE player_new RENAME TO player')
        
        conn.commit()
        conn.close()
        
        print("   ✅ Old nationality column removed")
        
    def create_nation_system(self):
        """Complete nation table creation and migration process"""
        print("🚀 Creating nation normalization system...")
        
        # Extract valid nations from messy data
        valid_nations = self.extract_valid_nations()
        
        # Populate nation table
        self.populate_nation_table(valid_nations)
        
        # Update player table structure  
        self.update_player_table_structure()
        
        # Migrate existing data
        self.migrate_player_nationalities()
        
        # Verify results
        self.verify_migration()
        
        # Ask user if they want to remove old column
        print(f"\n❓ Remove old nationality column? This will clean up the schema.")
        response = input("   Remove nationality column? (y/N): ").strip().lower()
        
        if response in ['y', 'yes']:
            self.cleanup_old_nationality_column()
        else:
            print("   ⚠️ Keeping old nationality column for now")
            
        print(f"\n🎉 Nation normalization complete!")

def main():
    creator = NationTableCreator()
    creator.create_nation_system()

if __name__ == "__main__":
    main()