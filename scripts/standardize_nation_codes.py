#!/usr/bin/env python3
"""
Standardize nation table to use proper ISO Alpha-3 codes and full country names
"""

import sqlite3
import hashlib

class NationStandardizer:
    def __init__(self, db_path='data/processed/nwsldata.db'):
        self.db_path = db_path
        
        # Mapping from current messy codes to proper ISO Alpha-3 codes
        self.code_standardization = {
            # Current messy → Standard Alpha-3
            'USA': 'USA',  # Already correct
            'CAN': 'CAN',  # Already correct
            'MEX': 'MEX',  # Already correct
            'AUS': 'AUS',  # Already correct
            'BRA': 'BRA',  # Already correct
            'ESP': 'ESP',  # Already correct
            'GER': 'DEU',  # Germany
            'ENG': 'GBR',  # England → Great Britain
            'JPN': 'JPN',  # Already correct
            'FRA': 'FRA',  # Already correct
            'NGA': 'NGA',  # Already correct
            'ZAM': 'ZMB',  # Zambia
            'DEN': 'DNK',  # Denmark
            'SWE': 'SWE',  # Already correct
            'COL': 'COL',  # Already correct
            'VEN': 'VEN',  # Already correct
            'KOR': 'KOR',  # Already correct
            'SCO': 'GBR',  # Scotland → Great Britain
            'WAL': 'GBR',  # Wales → Great Britain
            'IRL': 'IRL',  # Already correct
            'POL': 'POL',  # Already correct
            'NOR': 'NOR',  # Already correct
            'JAM': 'JAM',  # Already correct
            'CMR': 'CMR',  # Already correct
            'BIH': 'BIH',  # Already correct
            'AUT': 'AUT',  # Already correct
            'SUI': 'CHE',  # Switzerland
            'POR': 'PRT',  # Portugal
            'NED': 'NLD',  # Netherlands
            'MWI': 'MWI',  # Already correct
            'KEN': 'KEN',  # Already correct
            'HAI': 'HTI',  # Haiti
            'GUA': 'GTM',  # Guatemala
            'GHA': 'GHA',  # Already correct
            'FIN': 'FIN',  # Already correct
            'BER': 'BMU',  # Bermuda
            'CIV': 'CIV',  # Already correct
            'ANG': 'AGO',  # Angola
            'TTO': 'TTO',  # Already correct
            'WSM': 'WSM',  # Already correct
            'NZL': 'NZL',  # Already correct
            'ENG': 'GBR',  # England → Great Britain
        }
        
        # Full country names for Alpha-3 codes
        self.country_names = {
            'USA': 'United States',
            'CAN': 'Canada',
            'MEX': 'Mexico',
            'AUS': 'Australia',
            'BRA': 'Brazil',
            'ESP': 'Spain',
            'DEU': 'Germany',
            'GBR': 'United Kingdom',
            'JPN': 'Japan',
            'FRA': 'France',
            'NGA': 'Nigeria',
            'ZMB': 'Zambia',
            'DNK': 'Denmark',
            'SWE': 'Sweden',
            'COL': 'Colombia',
            'VEN': 'Venezuela',
            'KOR': 'South Korea',
            'IRL': 'Ireland',
            'POL': 'Poland',
            'NOR': 'Norway',
            'JAM': 'Jamaica',
            'CMR': 'Cameroon',
            'BIH': 'Bosnia and Herzegovina',
            'AUT': 'Austria',
            'CHE': 'Switzerland',
            'PRT': 'Portugal',
            'NLD': 'Netherlands',
            'MWI': 'Malawi',
            'KEN': 'Kenya',
            'HTI': 'Haiti',
            'GTM': 'Guatemala',
            'GHA': 'Ghana',
            'FIN': 'Finland',
            'BMU': 'Bermuda',
            'CIV': 'Côte d\'Ivoire',
            'AGO': 'Angola',
            'TTO': 'Trinidad and Tobago',
            'WSM': 'Samoa',
            'NZL': 'New Zealand',
        }
        
    def generate_nation_id(self, nation_code):
        """Generate consistent hex nation_id from standard Alpha-3 code"""
        hash_input = f"nation_{nation_code}".encode('utf-8')
        hash_obj = hashlib.md5(hash_input)
        hex_hash = hash_obj.hexdigest()[:8]
        return f"na_{hex_hash}"
        
    def standardize_nation_table(self):
        """Standardize the nation table to use proper ISO codes"""
        print("🌍 Standardizing nation table to ISO Alpha-3 codes...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Get current nations
        current_nations = conn.execute('''
            SELECT n.nation_id, n.nation_code, n.nation_name, COUNT(p.player_id) as player_count
            FROM nation n
            LEFT JOIN player p ON n.nation_id = p.nation_id
            GROUP BY n.nation_id, n.nation_code, n.nation_name
            ORDER BY player_count DESC
        ''').fetchall()
        
        print(f"   📊 Processing {len(current_nations)} current nations...")
        
        # Build standardized nation list
        standardized_nations = {}
        migration_mapping = {}  # old_nation_id -> new_nation_id
        
        for nation_id, nation_code, nation_name, player_count in current_nations:
            # Get standard Alpha-3 code
            standard_code = self.code_standardization.get(nation_code, nation_code)
            standard_name = self.country_names.get(standard_code, standard_code)
            standard_id = self.generate_nation_id(standard_code)
            
            # Track migration
            migration_mapping[nation_id] = standard_id
            
            # Add to standardized list (consolidate duplicates)
            if standard_code not in standardized_nations:
                standardized_nations[standard_code] = {
                    'nation_id': standard_id,
                    'nation_code': standard_code,
                    'nation_name': standard_name,
                    'player_count': 0
                }
                
            standardized_nations[standard_code]['player_count'] += player_count
            
            if nation_code != standard_code:
                print(f"   🔄 {nation_code} → {standard_code} ({standard_name}) [{player_count} players]")
            else:
                print(f"   ✅ {nation_code} ({standard_name}) [{player_count} players]")
                
        print(f"\n   📈 Consolidated from {len(current_nations)} to {len(standardized_nations)} unique nations")
        
        # Clear and rebuild nation table
        conn.execute('DELETE FROM nation')
        
        for nation_data in standardized_nations.values():
            conn.execute('''
                INSERT INTO nation (nation_id, nation_code, nation_name)
                VALUES (?, ?, ?)
            ''', (nation_data['nation_id'], nation_data['nation_code'], nation_data['nation_name']))
            
        print(f"   ✅ Rebuilt nation table with {len(standardized_nations)} standardized nations")
        
        # Update player references
        updated_players = 0
        for old_nation_id, new_nation_id in migration_mapping.items():
            result = conn.execute('''
                UPDATE player 
                SET nation_id = ? 
                WHERE nation_id = ?
            ''', (new_nation_id, old_nation_id))
            updated_players += result.rowcount
            
        conn.commit()
        conn.close()
        
        print(f"   🔄 Updated {updated_players} player nation references")
        
        return len(standardized_nations)
        
    def verify_standardization(self):
        """Verify the standardized nation table"""
        print(f"\n🔍 Verifying standardized nation table...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Get final statistics
        nation_count = conn.execute('SELECT COUNT(*) FROM nation').fetchone()[0]
        players_with_nation = conn.execute('SELECT COUNT(*) FROM player WHERE nation_id IS NOT NULL').fetchone()[0]
        total_players = conn.execute('SELECT COUNT(*) FROM player').fetchone()[0]
        
        print(f"   🌍 Standardized nations: {nation_count}")
        print(f"   👥 Players with nation: {players_with_nation}/{total_players}")
        
        # Check for proper Alpha-3 format
        invalid_codes = conn.execute('''
            SELECT nation_code, nation_name 
            FROM nation 
            WHERE LENGTH(nation_code) != 3 OR nation_code != UPPER(nation_code)
        ''').fetchall()
        
        if invalid_codes:
            print(f"   ⚠️ Found {len(invalid_codes)} nations with invalid Alpha-3 codes:")
            for code, name in invalid_codes:
                print(f"      {code} ({name})")
        else:
            print(f"   ✅ All nation codes are valid 3-letter Alpha-3 format")
            
        # Show top nations
        top_nations = conn.execute('''
            SELECT n.nation_code, n.nation_name, COUNT(p.player_id) as player_count
            FROM nation n
            LEFT JOIN player p ON n.nation_id = p.nation_id
            GROUP BY n.nation_id, n.nation_code, n.nation_name
            ORDER BY player_count DESC
            LIMIT 15
        ''').fetchall()
        
        print(f"\n   🏆 Top nations by player count:")
        for nation_code, nation_name, count in top_nations:
            print(f"      {nation_code} ({nation_name}): {count} players")
            
        conn.close()
        
    def show_sample_records(self):
        """Show sample standardized records"""
        print(f"\n📋 Sample standardized nation records:")
        
        conn = sqlite3.connect(self.db_path)
        
        samples = conn.execute('''
            SELECT nation_id, nation_code, nation_name
            FROM nation
            ORDER BY nation_code
            LIMIT 10
        ''').fetchall()
        
        for nation_id, nation_code, nation_name in samples:
            print(f"   {nation_id}: {nation_code} ({nation_name})")
            
        conn.close()

def main():
    standardizer = NationStandardizer()
    
    # Standardize nation codes
    nation_count = standardizer.standardize_nation_table()
    
    # Verify results
    standardizer.verify_standardization()
    
    # Show samples
    standardizer.show_sample_records()
    
    print(f"\n🎉 Nation standardization complete!")
    print(f"   ✅ {nation_count} nations with proper ISO Alpha-3 codes")
    print(f"   ✅ Clean hex nation_ids with na_ prefix")
    print(f"   ✅ Full country names properly spelled out")

if __name__ == "__main__":
    main()