#!/usr/bin/env python3
"""
Import 2014 NWSL matches to match_inventory
Based on pasted match data from user
"""

import sqlite3
from datetime import datetime

class Import2014Matches:
    def __init__(self, db_path='data/processed/nwsldata.db'):
        self.db_path = db_path
        
    def import_2014_matches(self):
        """Import 2014 match data to match_inventory table"""
        print("📊 Starting 2014 NWSL match import...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Get team mapping for lookups
        team_mapping = self._build_team_mapping(conn)
        
        # 2014 match data extracted from the image
        matches_2014 = [
            # Regular Season matches
            ('Regular Season', '4/12/14', 'Sky Blue FC', 'Thorns', '94d623e7'),
            ('Regular Season', '4/13/14', 'Houston Dash', 'Thorns', '84aec461'),
            ('Regular Season', '4/13/14', 'Red Stars', 'WNY Flash', '3d6216fe'),
            ('Regular Season', '4/19/14', 'Reign', 'Dash', '844843e4'),
            ('Regular Season', '4/19/14', 'Sky Blue FC', 'Thorns', 'f740da0f'),
            ('Regular Season', '4/19/14', 'Spirit', 'Red Stars', 'b27d4424'),
            ('Regular Season', '4/19/14', 'Red Stars', 'WNY Flash', 'bd1600aa'),
            ('Regular Season', '4/20/14', 'Reign', 'Dash', 'c53e15d1'),
            ('Regular Season', '4/26/14', 'Boston Breakers', 'Sky Blue FC', '63bb7671'),
            ('Regular Season', '4/27/14', 'Boston Breakers', 'Sky Blue FC', '53ab0324'),
            ('Regular Season', '4/30/14', 'Boston Breakers', 'Reign', '8d840a81'),
            ('Regular Season', '4/26/14', 'Thorns', 'Kansas City', 'cb3aa3b5'),
            ('Regular Season', '4/27/14', 'Spirit', 'Dash', 'e83abc63'),
            ('Regular Season', '5/3/14', 'WNY Flash', 'Spirit', '5dcdb2a1'),
            ('Regular Season', '5/3/14', 'Kansas City', 'Red Stars', '35503f82'),
            ('Regular Season', '5/4/14', 'WNY Flash', 'Thorns', '345e43d3'),
            ('Regular Season', '5/3/14', 'Sky Blue FC', 'Boston Breakers', '80750fe8'),
            ('Regular Season', '5/4/14', 'Red Stars', 'Reign', '118dd976'),
            ('Regular Season', '5/7/14', 'Kansas City', 'Spirit', 'e916ff74'),
            ('Regular Season', '4/7/14', 'Sky Blue FC', 'Red Stars', '2d5d94b0'),
            ('Regular Season', '5/7/14', 'WNY Flash', 'Dash', 'ed7d05d2'),
            ('Regular Season', '5/10/14', 'Thorns', 'Reign', '2ac75a24'),
            ('Regular Season', '5/11/14', 'WNY Flash', 'Sky Blue FC', '7fb6a049'),
            ('Regular Season', '5/11/14', 'Kansas City', 'Spirit', 'fad6ba09'),
            ('Regular Season', '5/11/14', 'Boston Breakers', 'Dash', '805e88f8'),
            ('Regular Season', '5/14/14', 'Red Stars', 'Thorns', 'f7b18932'),
            ('Regular Season', '5/3/14', 'Boston Breakers', 'Red Stars', '62c0bc69'),
            ('Regular Season', '5/14/14', 'Kansas City', 'Thorns', '98f4b3b5'),
            ('Regular Season', '5/17/14', 'Spirit', 'WNY Flash', '6317805b'),
            ('Regular Season', '5/18/14', 'Boston Breakers', 'Red Stars', '34f5023d'),
            ('Regular Season', '5/21/14', 'Dash', 'Thorns', '38a3ac6b'),
            ('Regular Season', '5/21/14', 'Spirit', 'Sky Blue FC', 'ee6f455b'),
            ('Regular Season', '5/21/14', 'Kansas City', 'Reign', '35b0f6a6'),
            ('Regular Season', '5/21/14', 'Thorns', 'WNY Flash', 'ac37fd63'),
            ('Regular Season', '5/24/14', 'Kansas City', 'Reign', '370619d4'),
            ('Regular Season', '5/24/14', 'Boston Breakers', 'Spirit', '84ba2285'),
            ('Regular Season', '5/24/14', 'Thorns', 'Sky Blue FC', '2569998b'),
            ('Regular Season', '5/24/14', 'Spirit', 'Dash', '1bd17d98'),
            ('Regular Season', '5/24/14', 'Spirit', 'WNY Flash', '98e13a73'),
            ('Regular Season', '5/28/14', 'Sky Blue FC', 'Red Stars', 'adb539f0'),
            ('Regular Season', '5/31/14', 'Dash', 'Reign', '866872a5'),
            ('Regular Season', '5/31/14', 'WNY Flash', 'Dash', 'f31dd276'),
            ('Regular Season', '6/1/14', 'Red Stars', 'Dash', '8c272eb1'),
            ('Regular Season', '6/1/14', 'Sky Blue FC', 'Reign', '2248637a'),
            ('Regular Season', '6/4/14', 'Boston Breakers', 'Spirit', '41f59838'),
            ('Regular Season', '6/4/14', 'Kansas City', 'WNY Flash', '701e111e'),
            ('Regular Season', '6/7/14', 'Kansas City', 'Red Stars', '71908db8'),
            ('Regular Season', '6/7/14', 'Spirit', 'Boston Breakers', '59e84c4a'),
            ('Regular Season', '6/7/14', 'Thorns', 'WNY Flash', '53a26a07'),
            ('Regular Season', '6/8/14', 'Sky Blue FC', 'Dash', '576d0fc5'),
            ('Regular Season', '6/11/14', 'Boston Breakers', 'Spirit', '0509fb03'),
            ('Regular Season', '6/11/14', 'Kansas City', 'WNY Flash', '05b40e4'),
            ('Regular Season', '6/14/14', 'Boston Breakers', 'Dash', '4f4e27bd'),
            ('Regular Season', '6/15/14', 'Red Stars', 'Sky Blue FC', '2d83a36'),
            ('Regular Season', '6/15/14', 'Spirit', 'Reign', '584543b3'),
            ('Regular Season', '6/18/14', 'WNY Flash', 'Red Stars', '5cc76c48'),
            ('Regular Season', '6/18/14', 'Kansas City', 'Reign', '66cd3978'),
            ('Regular Season', '6/18/14', 'Dash', 'Boston Breakers', '3b8f7ecb'),
            ('Regular Season', '6/21/14', 'Kansas City', 'Red Stars', '8e0632f5'),
            ('Regular Season', '6/22/14', 'Spirit', 'Sky Blue FC', '95b30364'),
            ('Regular Season', '6/25/14', 'WNY Flash', 'Sky Blue FC', 'a4542d2f'),
            ('Regular Season', '6/25/14', 'Boston Breakers', 'Thorns', '615de374'),
            ('Regular Season', '6/28/14', 'Dash', 'Red Stars', '4d8f5a84'),
            ('Regular Season', '6/28/14', 'Reign', 'Sky Blue FC', '12da8dd9'),
            ('Regular Season', '6/28/14', 'Kansas City', 'Thorns', '72a84ab8'),
            ('Regular Season', '6/28/14', 'Red Stars', 'Spirit', '58f1cof24'),
            ('Regular Season', '7/2/14', 'WNY Flash', 'Boston Breakers', '7f904c87'),
            ('Regular Season', '7/2/14', 'WNY Flash', 'Red Stars', '4438e0f2'),
            ('Regular Season', '7/5/14', 'Thorns', 'Boston Breakers', '8a5360c9'),
            ('Regular Season', '7/6/14', 'Sky Blue FC', 'WNY Flash', '4763955b'),
            ('Regular Season', '7/6/14', 'Red Stars', 'Boston Breakers', 'ac14e507'),
            ('Regular Season', '7/9/14', 'Red Stars', 'Thorns', '142999f4'),
            ('Regular Season', '7/12/14', 'WNY Flash', 'Spirit', '82775cb4'),
            ('Regular Season', '7/12/14', 'Kansas City', 'Dash', '7b434e66'),
            ('Regular Season', '7/12/14', 'Red Stars', 'Reign', '4d031116'),
            ('Regular Season', '7/13/14', 'Boston Breakers', 'Kansas City', '3de0f8e8'),
            ('Regular Season', '7/15/14', 'WNY Flash', 'Kansas City', '638635be'),
            ('Regular Season', '7/19/14', 'Boston Breakers', 'Thorns', '3045d5ef'),
            ('Regular Season', '7/20/14', 'Red Stars', 'Kansas City', '9a7b7e5c'),
            ('Regular Season', '7/20/14', 'Sky Blue FC', 'Spirit', 'a459966d'),
            ('Regular Season', '7/20/14', 'Boston Breakers', 'Red Stars', 'c9d20db2'),
            ('Regular Season', '7/20/14', 'Reign', 'Red Stars', '56e01c0e'),
            ('Regular Season', '7/23/14', 'Spirit', 'Thorns', '5d6f29f8'),
            ('Regular Season', '7/26/14', 'Dash', 'Kansas City', '5bac10b2'),
            ('Regular Season', '7/26/14', 'Red Stars', 'Dash', '79461c81'),
            ('Regular Season', '7/27/14', 'Kansas City', 'Sky Blue FC', '8cd95210'),
            ('Regular Season', '7/27/14', 'Reign', 'Thorns', '0a5d3f27'),
            ('Regular Season', '7/30/14', 'Spirit', 'Kansas City', '4920af8'),
            ('Regular Season', '7/30/14', 'Dash', 'Thorns', '37e5b82c'),
            ('Regular Season', '8/2/14', 'Sky Blue FC', 'WNY Flash', '36e0fedd'),
            ('Regular Season', '8/2/14', 'Boston Breakers', 'Red Stars', '65a4dc16'),
            ('Regular Season', '8/2/14', 'Boston Breakers', 'WNY Flash', '6c6f5931'),
            ('Regular Season', '8/2/14', 'Thorns', 'Reign', '5c293dc5'),
            ('Regular Season', '8/3/14', 'Thorns', 'Boston Breakers', '6d335588'),
            ('Regular Season', '8/5/14', 'Kansas City', 'Boston Breakers', 'a8df8e4'),
            ('Regular Season', '8/6/14', 'Spirit', 'Dash', '5168ce24'),
            ('Regular Season', '8/6/14', 'Red Stars', 'Sky Blue FC', '81278ba2'),
            ('Regular Season', '8/9/14', 'Kansas City', 'Kansas City', '21860596'),
            ('Regular Season', '8/9/14', 'Reign', 'Spirit', 'a8354914'),
            ('Regular Season', '8/10/14', 'WNY Flash', 'Dash', '436092f0'),
            ('Regular Season', '8/13/14', 'Boston Breakers', 'Thorns', '7ed3d2b8'),
            ('Regular Season', '8/13/14', 'Red Stars', 'Boston Breakers', 'd2c6069a'),
            ('Regular Season', '8/13/14', 'Spirit', 'Sky Blue FC', '5d6cb339'),
            ('Regular Season', '8/16/14', 'Red Stars', 'WNY Flash', '8b9788bb'),
            ('Regular Season', '8/17/14', 'Boston Breakers', 'Dash', 'c535f09'),
            ('Regular Season', '8/17/14', 'Dash', 'Spirit', '50d0c3c7'),
            ('Regular Season', '8/20/14', 'Sky Blue FC', 'Dash', 'ae8cc3be'),
            ('Regular Season', '8/23/14', 'Kansas City', 'Dash', '217ede8'),
            # Playoffs
            ('Semifinals', '8/24/14', 'Reign', 'Spirit', 'ff811dc1'),
            ('Semifinals', '8/31/14', 'Thorns', 'Kansas City', '6877dd16')
        ]
        
        successful = 0
        failed = 0
        
        for match_type, date_str, home_team, away_team, match_id in matches_2014:
            try:
                # Parse date
                match_date = self._parse_date(date_str)
                
                # Map team names to IDs
                home_team_id = self._map_team_name_to_id(home_team, team_mapping)
                away_team_id = self._map_team_name_to_id(away_team, team_mapping)
                
                if not home_team_id or not away_team_id:
                    print(f"⚠️ Could not map teams: {home_team} vs {away_team}")
                    failed += 1
                    continue
                
                # Determine match type ID
                if match_type == 'Regular Season':
                    match_type_id = 'mt_a1b2c3d4'
                    match_type_name = 'Regular Season'
                elif match_type in ['Semifinals', 'Final']:
                    match_type_id = 'mt_e5f6g7h8'
                    match_type_name = 'Playoff'
                else:
                    match_type_id = 'mt_a1b2c3d4'  # Default to regular season
                    match_type_name = 'Regular Season'
                
                # Insert into match_inventory
                conn.execute('''
                    INSERT OR REPLACE INTO match_inventory 
                    (match_id, match_date, home_team_id, away_team_id, season_id, 
                     match_type_id, match_type_name, filename, extraction_status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    match_id,
                    match_date,
                    home_team_id,
                    away_team_id,
                    2014,  # season_id
                    match_type_id,
                    match_type_name,
                    f"2014_{match_type.lower().replace(' ', '_')}_{match_id}.manual",
                    'manual_entry'
                ))
                
                successful += 1
                home_name = self._get_team_name(conn, home_team_id)
                away_name = self._get_team_name(conn, away_team_id)
                print(f"  ✅ {match_id}: {home_name} vs {away_name} ({match_date}) - {match_type_name}")
                
            except Exception as e:
                failed += 1
                print(f"  ❌ Error adding {match_id}: {e}")
        
        conn.commit()
        conn.close()
        
        print(f"\n🎉 2014 match import complete!")
        print(f"   ✅ Successfully imported: {successful}")
        print(f"   ❌ Failed: {failed}")
        
        return successful
    
    def _parse_date(self, date_str):
        """Parse date from M/D/YY format to YYYY-MM-DD"""
        try:
            month, day, year = date_str.split('/')
            if len(year) == 2:
                year = f"20{year}"
            date_obj = datetime(int(year), int(month), int(day))
            return date_obj.strftime('%Y-%m-%d')
        except Exception as e:
            print(f"⚠️ Date parsing error for '{date_str}': {e}")
            return None
    
    def _build_team_mapping(self, conn):
        """Build mapping from team names to team_ids"""
        teams = conn.execute('SELECT team_id, team_name FROM teams').fetchall()
        
        mapping = {}
        for team_id, team_name in teams:
            mapping[team_name] = team_id
        
        # Add 2014-specific team name mappings
        mapping.update({
            'Red Stars': 'd976a235',      # Chicago Red Stars -> Chicago Stars FC
            'Reign': '257fad2b',          # Seattle Reign FC
            'Sky Blue FC': '8e306dc6',    # -> Gotham FC
            'WNY Flash': '5f911568',      # Western New York Flash
            'Boston Breakers': 'ab757728',
            'Spirit': 'e442aad0',         # Washington Spirit
            'Kansas City': '6f666306',    # FC Kansas City -> Kansas City Current
            'Thorns': 'df9a10a1',         # Portland Thorns FC
            'Houston Dash': 'e813709a',   # Houston Dash
            'Dash': 'e813709a'            # Houston Dash (short form)
        })
        
        return mapping
    
    def _map_team_name_to_id(self, team_name, team_mapping):
        """Map team name to team_id"""
        if team_name in team_mapping:
            return team_mapping[team_name]
        
        # Fuzzy match
        for mapped_name, team_id in team_mapping.items():
            if team_name in mapped_name or mapped_name in team_name:
                return team_id
        
        return None
    
    def _get_team_name(self, conn, team_id):
        """Get team name from team_id"""
        result = conn.execute('SELECT team_name FROM teams WHERE team_id = ?', (team_id,)).fetchone()
        return result[0] if result else team_id
    
    def verify_2014_matches(self):
        """Verify 2014 matches import"""
        print("🔍 Verifying 2014 matches...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Count by match type for 2014
        counts = conn.execute('''
            SELECT match_type_name, COUNT(*) as count
            FROM match_inventory 
            WHERE season_id = 2014
            GROUP BY match_type_name
            ORDER BY match_type_name
        ''').fetchall()
        
        total_2014 = sum(count for _, count in counts)
        
        print(f"   📊 2014 Season Summary:")
        for type_name, count in counts:
            print(f"      {type_name}: {count} matches")
        print(f"      Total: {total_2014} matches")
        
        # Show overall inventory
        all_counts = conn.execute('''
            SELECT season_id, match_type_name, COUNT(*) as count
            FROM match_inventory 
            GROUP BY season_id, match_type_name
            ORDER BY season_id, match_type_name
        ''').fetchall()
        
        print(f"\n   📈 Complete Match Inventory:")
        current_season = None
        for season, type_name, count in all_counts:
            if season != current_season:
                if current_season is not None:
                    print()
                print(f"      {season}:")
                current_season = season
            print(f"         {type_name}: {count} matches")
        
        conn.close()

if __name__ == "__main__":
    importer = Import2014Matches()
    importer.import_2014_matches()
    importer.verify_2014_matches()