#!/usr/bin/env python3
"""
Fix nation table duplicates and consolidate data
"""

import sqlite3
import hashlib

class NationDuplicateFixer:
    def __init__(self, db_path='data/processed/nwsldata.db'):
        self.db_path = db_path
        
        # Canonical nation mapping - consolidate duplicates
        self.canonical_nations = {
            # USA consolidation
            'us USA': 'USA',
            'USA': 'USA',
            
            # Canada consolidation  
            'ca CAN': 'CAN',
            'CAN': 'CAN',
            
            # Mexico consolidation
            'mx MEX': 'MEX',
            'MEX': 'MEX',
            
            # Australia consolidation
            'au AUS': 'AUS', 
            'AUS': 'AUS',
            
            # Germany consolidation
            'de GER': 'GER',
            'GER': 'GER',
            
            # England consolidation
            'eng ENG': 'ENG',
            'ENG': 'ENG',
            
            # Spain consolidation
            'es ESP': 'ESP',
            'ESP': 'ESP',
            
            # Japan consolidation
            'jp JPN': 'JPN',
            'JPN': 'JPN',
            
            # New Zealand consolidation
            'nz NZL': 'NZL',
            'NZL': 'NZL',
            
            # Other mappings for weird codes
            'at AUT': 'AUT',
            'ba BIH': 'BIH', 
            'br BRA': 'BRA',
            'cm CMR': 'CMR',
            'dk DEN': 'DEN',
            'ie IRL': 'IRL',
            'jm JAM': 'JAM',
            'ng NGA': 'NGA',
            'no NOR': 'NOR',
            'pl POL': 'POL',
            'sct SCO': 'SCO',
            'tt TRI': 'TTO',  # Trinidad & Tobago
            'wls WAL': 'WAL',
            'ws SAM': 'WSM',  # Samoa
            
            # Keep standard codes as-is
            'BRA': 'BRA', 'FRA': 'FRA', 'SWE': 'SWE', 'DEN': 'DEN',
            'NGA': 'NGA', 'ZAM': 'ZAM', 'VEN': 'VEN', 'KOR': 'KOR',
            'COL': 'COL', 'SCO': 'SCO', 'WAL': 'WAL', 'IRL': 'IRL',
            'POL': 'POL', 'NOR': 'NOR', 'JAM': 'JAM', 'CMR': 'CMR',
            'BIH': 'BIH', 'AUT': 'AUT', 'SUI': 'SUI', 'POR': 'POR',
            'NED': 'NED', 'MWI': 'MWI', 'KEN': 'KEN', 'HAI': 'HAI',
            'GUA': 'GUA', 'GHA': 'GHA', 'FIN': 'FIN', 'BER': 'BER',
            'CIV': 'CIV', 'ANG': 'ANG'
        }
        
        # Remove single names that aren't real countries
        self.invalid_nations = {
            'Marta', 'Quinn', 'Rafinha', 'Rosana', 'Thaisa', 'Poliana', 
            'Mônica', 'Gabi', 'Angelina', 'Bia', 'Bruna', 'Camila', 'Debinha'
        }
        
        # Full nation names
        self.nation_names = {
            'USA': 'United States', 'CAN': 'Canada', 'MEX': 'Mexico',
            'AUS': 'Australia', 'GER': 'Germany', 'ENG': 'England',
            'ESP': 'Spain', 'NZL': 'New Zealand', 'JPN': 'Japan',
            'BRA': 'Brazil', 'FRA': 'France', 'SWE': 'Sweden',
            'DEN': 'Denmark', 'NGA': 'Nigeria', 'ZAM': 'Zambia',
            'VEN': 'Venezuela', 'KOR': 'South Korea', 'COL': 'Colombia',
            'SCO': 'Scotland', 'WAL': 'Wales', 'IRL': 'Ireland',
            'POL': 'Poland', 'NOR': 'Norway', 'JAM': 'Jamaica',
            'CMR': 'Cameroon', 'BIH': 'Bosnia and Herzegovina', 'AUT': 'Austria',
            'SUI': 'Switzerland', 'POR': 'Portugal', 'NED': 'Netherlands',
            'MWI': 'Malawi', 'KEN': 'Kenya', 'HAI': 'Haiti',
            'GUA': 'Guatemala', 'GHA': 'Ghana', 'FIN': 'Finland',
            'BER': 'Bermuda', 'CIV': 'Ivory Coast', 'ANG': 'Angola',
            'TTO': 'Trinidad and Tobago', 'WSM': 'Samoa'
        }
        
    def generate_nation_id(self, nation_code):
        """Generate consistent hex nation_id"""
        hash_input = f"nation_{nation_code}".encode('utf-8')
        hash_obj = hashlib.md5(hash_input)
        hex_hash = hash_obj.hexdigest()[:8]
        return f"na_{hex_hash}"
        
    def fix_duplicates(self):
        """Fix nation duplicates and consolidate data"""
        print("🔧 Fixing nation duplicates...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Get all current nations
        current_nations = conn.execute('''
            SELECT n.nation_id, n.nation_code, COUNT(p.player_id) as player_count
            FROM nation n
            LEFT JOIN player p ON n.nation_id = p.nation_id  
            GROUP BY n.nation_id, n.nation_code
            ORDER BY player_count DESC
        ''').fetchall()
        
        print(f"   📊 Found {len(current_nations)} nations to process")
        
        # Create consolidated nation table
        consolidated_nations = {}
        migration_map = {}  # old_nation_id -> new_nation_id
        
        for nation_id, nation_code, player_count in current_nations:
            # Skip invalid single names
            if nation_code in self.invalid_nations:
                print(f"   🗑️ Removing invalid nation: {nation_code} ({player_count} players)")
                migration_map[nation_id] = None  # Clear these players
                continue
                
            # Get canonical code
            canonical_code = self.canonical_nations.get(nation_code, nation_code)
            canonical_id = self.generate_nation_id(canonical_code)
            
            # Track migration
            migration_map[nation_id] = canonical_id
            
            # Add to consolidated list
            if canonical_code not in consolidated_nations:
                consolidated_nations[canonical_code] = {
                    'nation_id': canonical_id,
                    'nation_code': canonical_code,
                    'nation_name': self.nation_names.get(canonical_code, canonical_code),
                    'player_count': 0
                }
                
            consolidated_nations[canonical_code]['player_count'] += player_count
            
            if nation_code != canonical_code:
                print(f"   🔄 {nation_code} → {canonical_code} ({player_count} players)")
                
        print(f"   ✅ Consolidated to {len(consolidated_nations)} unique nations")
        
        # Clear and rebuild nation table
        conn.execute('DELETE FROM nation')
        
        for nation_data in consolidated_nations.values():
            conn.execute('''
                INSERT INTO nation (nation_id, nation_code, nation_name)
                VALUES (?, ?, ?)
            ''', (nation_data['nation_id'], nation_data['nation_code'], nation_data['nation_name']))
            
        # Update player nation_id references
        updated_players = 0
        cleared_players = 0
        
        for old_nation_id, new_nation_id in migration_map.items():
            if new_nation_id is None:
                # Clear invalid nation references
                result = conn.execute('''
                    UPDATE player SET nation_id = NULL WHERE nation_id = ?
                ''', (old_nation_id,))
                cleared_players += result.rowcount
            else:
                # Update to consolidated nation_id
                result = conn.execute('''
                    UPDATE player SET nation_id = ? WHERE nation_id = ?
                ''', (new_nation_id, old_nation_id))
                updated_players += result.rowcount
                
        conn.commit()
        conn.close()
        
        print(f"   🔄 Updated {updated_players} player references")
        print(f"   🧹 Cleared {cleared_players} invalid nation references")
        
        return len(consolidated_nations)
        
    def remove_old_nationality_column(self):
        """Remove the old nationality column"""
        print("\n🗑️ Removing old nationality column...")
        
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
        
    def verify_final_results(self):
        """Verify the final nation table"""
        print("\n🔍 Verifying final nation table...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Get final stats
        nation_count = conn.execute('SELECT COUNT(*) FROM nation').fetchone()[0]
        players_with_nation = conn.execute('SELECT COUNT(*) FROM player WHERE nation_id IS NOT NULL').fetchone()[0]
        total_players = conn.execute('SELECT COUNT(*) FROM player').fetchone()[0]
        
        print(f"   🌍 Final nation count: {nation_count}")
        print(f"   👥 Players with nation: {players_with_nation}/{total_players}")
        
        # Top nations
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

def main():
    fixer = NationDuplicateFixer()
    
    # Fix duplicates
    nation_count = fixer.fix_duplicates()
    
    # Remove old column
    fixer.remove_old_nationality_column()
    
    # Verify results
    fixer.verify_final_results()
    
    print(f"\n🎉 Nation normalization complete!")
    print(f"   ✅ {nation_count} unique nations with clean hex IDs")

if __name__ == "__main__":
    main()