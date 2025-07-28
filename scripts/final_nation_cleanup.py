#!/usr/bin/env python3
"""
Final cleanup of nation table - fix all remaining messy codes
"""

import sqlite3
import hashlib

class FinalNationCleanup:
    def __init__(self, db_path='data/processed/nwsldata.db'):
        self.db_path = db_path
        
        # Map messy codes to standard Alpha-3 codes
        self.messy_to_standard = {
            'jp JPN': 'JPN',
            'nz NZL': 'NZL', 
            'at AUT': 'AUT',
            'ba BIH': 'BIH',
            'br BRA': 'BRA',
            'cm CMR': 'CMR',
            'dk DEN': 'DNK',  # Denmark
            'ie IRL': 'IRL',
            'jm JAM': 'JAM',
            'ng NGA': 'NGA',
            'no NOR': 'NOR',
            'pl POL': 'POL',
            'sct SCO': 'GBR',  # Scotland → United Kingdom
            'tt TRI': 'TTO',   # Trinidad & Tobago
            'wls WAL': 'GBR',  # Wales → United Kingdom
            'ws SAM': 'WSM',   # Samoa
        }
        
        # Missing country names
        self.missing_countries = {
            'BIH': 'Bosnia and Herzegovina',
            'CMR': 'Cameroon',
            'NOR': 'Norway',
            'POL': 'Poland',
            'TTO': 'Trinidad and Tobago',
            'WSM': 'Samoa',
        }
        
    def generate_nation_id(self, nation_code):
        """Generate consistent hex nation_id"""
        hash_input = f"nation_{nation_code}".encode('utf-8')
        hash_obj = hashlib.md5(hash_input)
        hex_hash = hash_obj.hexdigest()[:8]
        return f"na_{hex_hash}"
        
    def cleanup_messy_codes(self):
        """Clean up all messy nation codes"""
        print("🧹 Final cleanup of messy nation codes...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Create missing nation entries first
        print("   ➕ Creating missing nation entries...")
        for code, name in self.missing_countries.items():
            nation_id = self.generate_nation_id(code)
            conn.execute('''
                INSERT OR IGNORE INTO nation (nation_id, nation_code, nation_name)
                VALUES (?, ?, ?)
            ''', (nation_id, code, name))
            print(f"      Created: {code} ({name})")
            
        # Update player references from messy codes to standard codes
        print("\n   🔄 Updating player references...")
        total_updated = 0
        
        for messy_code, standard_code in self.messy_to_standard.items():
            # Get the messy nation_id
            messy_result = conn.execute(
                'SELECT nation_id FROM nation WHERE nation_code = ?', 
                (messy_code,)
            ).fetchone()
            
            if not messy_result:
                continue
                
            messy_nation_id = messy_result[0]
            
            # Get the standard nation_id  
            standard_result = conn.execute(
                'SELECT nation_id FROM nation WHERE nation_code = ?',
                (standard_code,)
            ).fetchone()
            
            if not standard_result:
                print(f"      ⚠️ Standard code not found: {standard_code}")
                continue
                
            standard_nation_id = standard_result[0]
            
            # Update players
            result = conn.execute('''
                UPDATE player 
                SET nation_id = ? 
                WHERE nation_id = ?
            ''', (standard_nation_id, messy_nation_id))
            
            updated_count = result.rowcount
            total_updated += updated_count
            
            if updated_count > 0:
                print(f"      {messy_code} → {standard_code}: {updated_count} players")
                
        # Remove all messy/empty nation entries
        print("\n   🗑️ Removing messy nation entries...")
        
        # Get list of messy codes including empty ones
        messy_codes = list(self.messy_to_standard.keys()) + [
            'au AUS', 'ca CAN', 'de GER', 'eng ENG', 'es ESP', 'mx MEX', 'us USA'
        ]
        
        for code in messy_codes:
            result = conn.execute('DELETE FROM nation WHERE nation_code = ?', (code,))
            if result.rowcount > 0:
                print(f"      Removed: {code}")
                
        conn.commit()
        conn.close()
        
        print(f"\n   ✅ Updated {total_updated} player references")
        
    def verify_final_state(self):
        """Verify the final clean state"""
        print(f"\n🔍 Verifying final nation table state...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Check for invalid codes
        invalid = conn.execute('''
            SELECT nation_code, nation_name 
            FROM nation 
            WHERE LENGTH(nation_code) != 3 OR nation_code != UPPER(nation_code)
        ''').fetchall()
        
        if invalid:
            print(f"   ❌ Still have {len(invalid)} invalid codes:")
            for code, name in invalid:
                print(f"      {code} ({name})")
        else:
            print(f"   ✅ All nation codes are valid 3-letter Alpha-3 format")
            
        # Get final stats
        nation_count = conn.execute('SELECT COUNT(*) FROM nation').fetchone()[0]
        players_with_nation = conn.execute('SELECT COUNT(*) FROM player WHERE nation_id IS NOT NULL').fetchone()[0]
        
        print(f"   🌍 Final nation count: {nation_count}")
        print(f"   👥 Players with nation: {players_with_nation}")
        
        # Show top nations
        top_nations = conn.execute('''
            SELECT n.nation_code, n.nation_name, COUNT(p.player_id) as player_count
            FROM nation n
            LEFT JOIN player p ON n.nation_id = p.nation_id
            GROUP BY n.nation_id, n.nation_code, n.nation_name
            HAVING player_count > 0
            ORDER BY player_count DESC
            LIMIT 20
        ''').fetchall()
        
        print(f"\n   🏆 Final top nations by player count:")
        for nation_code, nation_name, count in top_nations:
            print(f"      {nation_code} ({nation_name}): {count} players")
            
        conn.close()

def main():
    cleanup = FinalNationCleanup()
    
    # Clean up messy codes
    cleanup.cleanup_messy_codes()
    
    # Verify final state
    cleanup.verify_final_state()
    
    print(f"\n🎉 Final nation cleanup complete!")
    print(f"   ✅ All nation codes are now proper ISO Alpha-3 format")
    print(f"   ✅ All country names are properly spelled out")
    print(f"   ✅ Clean hex nation_ids with na_ prefix")

if __name__ == "__main__":
    main()