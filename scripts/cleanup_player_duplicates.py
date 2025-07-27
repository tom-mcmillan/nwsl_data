#!/usr/bin/env python3
"""
Clean up player table duplicates using verified correct player_ids
"""

import sqlite3

class PlayerDuplicateCleanup:
    def __init__(self, db_path='data/processed/nwsldata.db'):
        self.db_path = db_path
        
        # Verified correct player_ids from FBRef URLs
        self.correct_player_ids = {
            'Abby Erceg': 'c34e49ab',
            'Abi Kim': 'c5f5fc93', 
            'Addie McCain': '0da5f5b6',
            'Addisyn Merrick': '8844d594',
            # Note: Adriana has 2 different players - handled manually
            'Adrianna Franch': '2d2234cf',
            'Alex Morgan': '1a64f434',
            'Ali Krieger': '21922d46',
            'Ali Riley': 'c65fc5cf',
            'Allie Hess': 'ec30b0a3',
            'Allie Long': 'ab2811ad',
            'Ally Haran': 'd66d6128',
            'Ally Prisock': 'fb42a249',
            'Allysha Chapman': 'afcbf4d8',
            'Alyssa Mautz': '7a8dbc0f',
            'Amandine Henry': 'eb24542f',
            'Amber Brooks': 'f9f76078',
            'Amy Rodriguez': '3bc75a1a',
            'Amy Turner': 'a21edf38',
            'Andi Sullivan': '7e19bccd',
            # Second batch verified
            'Sofia Huerta': '570faa60',
            'Sophia Wilson': 'a4568174',
            'Stephanie Ochs': 'a048cb2f',
            'Sydney Leroux': '7d7d1c00',
            'Tatumn Milazzo': 'db1957b4',
            'Taylor Aylmer': '9768ff7b',
            'Taylor Leach': '7e23ee28',
            'Taylor Lytle': '2811e7f3',
            'Taylor Otto': 'd4cf6b21',
            'Taylor Porter': '3fd67fb6',
            'Taylor Smith': '97f414ac',
            'Tegan McGrady': '72927bcb',
            'Thembi Kgatlana': '93c3ff48',
            'Toni Pressley': '3d2972d6',
            'Tori Huster': '3b1156ac',
            'Tziarra King': '679eda9b',
            'Veronica Latsko': '2c4a9c2f',
            'Victoria Pickett': '28197d4c',
            'Yūki Nagasato': '5b97f5ec',
            'Zoe Morse': '981cf9db',
            # Third batch verified
            'Sabrina Flores': 'e43b277b',
            'Sam Kerr': '6a435e8d',
            'Sam Mewis': '9faa1610',
            'Sam Witteman': '7cfc8b0b',
            'Sarah Bouhaddi': 'a6a9dbe0',
            'Sarah Hagen': '04f6fcee',
            'Sarah Woldmoe': 'fc112dbc',
            'Satara Murray': 'c519dc3a',
            'Shirley Cruz': '69a5c1ae',
            'Simone Charley': '78400c25',
            'Sinclaire Miramontez': '29355aaf',
            'Sinead Farrelly': '34e16d35',
            # Fourth batch verified
            'Andrea Rán Hauksdóttir': '01e11f77',
            'Andressinha': '097a7693',
            'Angela Salem': '3ad6a01a',
            'Anna Heilferty': 'b7dd09b9',
            'Ashlyn Harris': '4c7ad183',
            'Becky Sauerbrunn': 'd2612e86',
            # Fifth batch verified
            'Poliana': '9f58ec12',
            'Quinn': 'beede7f7',
            'Rachel Corsie': '49fd636f',
            'Rachel Daly': '7fa35ec9',
            'Rebekah Stott': 'c2b6ba64',
            'Rumi Utsugi': '8897a49d',
            # Sixth batch verified
            'Bethany Balcer': '0d502444',
            'Beverly Yanez': 'c899177a',
            'Brianna Visalli': 'bbdfa558',
            # Seventh batch verified
            'Brooke Hendrix': 'ab662bbc',
            'Bruna': '2ffdd658',
            'Cali Farquharson': '4b7864fd',
            'Cami Privett': '484c8938',
        }
        
    def cleanup_verified_duplicates(self):
        """Clean up the verified duplicate players"""
        print("🧹 Cleaning up verified player duplicates...")
        
        conn = sqlite3.connect(self.db_path)
        
        total_removed = 0
        total_preserved = 0
        
        for player_name, correct_id in self.correct_player_ids.items():
            # Get all player_ids for this name
            player_records = conn.execute('''
                SELECT player_id, player_name, nation_id, dob 
                FROM player 
                WHERE player_name = ?
                ORDER BY player_id
            ''', (player_name,)).fetchall()
            
            if len(player_records) <= 1:
                print(f"   ⚠️ {player_name}: Only 1 record found, skipping")
                continue
                
            print(f"   🔄 {player_name}: {len(player_records)} records found")
            
            # Find the correct record and any incorrect ones
            correct_record = None
            incorrect_records = []
            
            for record in player_records:
                player_id, name, nation_id, dob = record
                if player_id == correct_id:
                    correct_record = record
                    print(f"      ✅ Keeping: {player_id} (verified correct)")
                else:
                    incorrect_records.append(record)
                    print(f"      ❌ Removing: {player_id} (incorrect)")
                    
            if not correct_record:
                print(f"      ⚠️ WARNING: Correct ID {correct_id} not found in database!")
                continue
                
            # If correct record is missing nation_id, try to get it from incorrect records
            correct_player_id, correct_name, correct_nation_id, correct_dob = correct_record
            
            if not correct_nation_id:
                for incorrect_record in incorrect_records:
                    _, _, nation_id, _ = incorrect_record
                    if nation_id:
                        print(f"      🔄 Updating correct record with nation_id: {nation_id}")
                        conn.execute('''
                            UPDATE player 
                            SET nation_id = ? 
                            WHERE player_id = ?
                        ''', (nation_id, correct_player_id))
                        break
                        
            # Remove incorrect records
            for incorrect_record in incorrect_records:
                incorrect_id = incorrect_record[0]
                result = conn.execute('''
                    DELETE FROM player WHERE player_id = ?
                ''', (incorrect_id,))
                total_removed += result.rowcount
                
            total_preserved += 1
            
        conn.commit()
        conn.close()
        
        print(f"\n   ✅ Cleanup complete:")
        print(f"      Preserved: {total_preserved} correct players")
        print(f"      Removed: {total_removed} duplicate records")
        
        return total_preserved, total_removed
        
    def cleanup_remaining_duplicates(self):
        """Clean up any remaining duplicates not in the verified list"""
        print(f"\n🔍 Checking for remaining duplicates...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Find remaining duplicates
        remaining_dups = conn.execute('''
            SELECT player_name, COUNT(DISTINCT player_id) as id_count, GROUP_CONCAT(player_id) as player_ids
            FROM player 
            WHERE player_name IS NOT NULL 
            GROUP BY player_name
            HAVING COUNT(DISTINCT player_id) > 1
            ORDER BY player_name
        ''').fetchall()
        
        if remaining_dups:
            print(f"   ⚠️ Found {len(remaining_dups)} remaining duplicate names:")
            for name, count, ids in remaining_dups[:10]:  # Show first 10
                print(f"      {name}: {ids}")
            if len(remaining_dups) > 10:
                print(f"      ... and {len(remaining_dups) - 10} more")
        else:
            print(f"   ✅ No remaining duplicates found!")
            
        conn.close()
        return len(remaining_dups)
        
    def verify_cleanup(self):
        """Verify the cleanup results"""
        print(f"\n🔍 Verifying cleanup results...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Get final player count
        total_players = conn.execute('SELECT COUNT(*) FROM player').fetchone()[0]
        players_with_nation = conn.execute('SELECT COUNT(*) FROM player WHERE nation_id IS NOT NULL').fetchone()[0]
        
        print(f"   📊 Final player count: {total_players}")
        print(f"   🌍 Players with nation: {players_with_nation}")
        
        # Check a few verified players
        print(f"\n   ✅ Verified players check:")
        for name, correct_id in list(self.correct_player_ids.items())[:5]:
            result = conn.execute('''
                SELECT player_id, player_name, nation_id
                FROM player 
                WHERE player_name = ? AND player_id = ?
            ''', (name, correct_id)).fetchone()
            
            if result:
                player_id, player_name, nation_id = result
                nation_status = f"nation: {nation_id}" if nation_id else "no nation"
                print(f"      ✅ {player_name}: {player_id} ({nation_status})")
            else:
                print(f"      ❌ {name}: {correct_id} NOT FOUND!")
                
        conn.close()

def main():
    cleanup = PlayerDuplicateCleanup()
    
    # Clean up verified duplicates
    preserved, removed = cleanup.cleanup_verified_duplicates()
    
    # Check for remaining duplicates
    remaining = cleanup.cleanup_remaining_duplicates()
    
    # Verify results
    cleanup.verify_cleanup()
    
    print(f"\n🎉 Player duplicate cleanup complete!")
    print(f"   ✅ {preserved} players verified and preserved")
    print(f"   🗑️ {removed} duplicate records removed")
    if remaining > 0:
        print(f"   ⚠️ {remaining} duplicates still need verification")
    else:
        print(f"   ✅ All duplicates resolved!")

if __name__ == "__main__":
    main()