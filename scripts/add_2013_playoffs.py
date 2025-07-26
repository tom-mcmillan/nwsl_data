#!/usr/bin/env python3
"""
Add 2013 NWSL Playoff matches to match_inventory
"""

import sqlite3
from datetime import datetime

class Add2013Playoffs:
    def __init__(self, db_path='data/processed/nwsldata.db'):
        self.db_path = db_path
        
    def add_2013_playoff_matches(self):
        """Add the 3 playoff matches from 2013"""
        print("🏆 Adding 2013 NWSL Playoff matches...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Get team mapping for lookups
        team_mapping = self._build_team_mapping(conn)
        
        # 2013 Playoff matches data
        playoff_matches = [
            {
                'match_id': 'f481adf8',
                'date': '8/24/13',
                'home': 'Kansas City',
                'away': 'Thorns',
                'match_url': 'https://fbref.com/en/matches/f481adf8/FC-Kansas-City-Portland-Thorns-FC-August-24-2013-NWSL'
            },
            {
                'match_id': '6c5b0538',
                'date': '8/24/13', 
                'home': 'WNY Flash',
                'away': 'Sky Blue FC',
                'match_url': 'https://fbref.com/en/matches/6c5b0538/Western-New-York-Flash-Sky-Blue-FC-August-24-2013-NWSL'
            },
            {
                'match_id': 'ba3972b',
                'date': '8/31/13',
                'home': 'WNY Flash',
                'away': 'Thorns', 
                'match_url': 'https://fbref.com/en/matches/ba3972b/Western-New-York-Flash-Portland-Thorns-FC-August-31-2013-NWSL'
            }
        ]
        
        successful = 0
        failed = 0
        
        for match in playoff_matches:
            try:
                # Parse date
                match_date = self._parse_date(match['date'])
                
                # Map team names to IDs
                home_team_id = self._map_team_name_to_id(match['home'], team_mapping)
                away_team_id = self._map_team_name_to_id(match['away'], team_mapping)
                
                if not home_team_id or not away_team_id:
                    print(f"⚠️ Could not map teams: {match['home']} vs {match['away']}")
                    failed += 1
                    continue
                
                # Insert into match_inventory
                conn.execute('''
                    INSERT OR REPLACE INTO match_inventory 
                    (match_id, match_date, home_team_id, away_team_id, season_id, 
                     match_type_id, match_type_name, filename, extraction_status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    match['match_id'],
                    match_date,
                    home_team_id,
                    away_team_id,
                    2013,  # season_id
                    'mt_e5f6g7h8',  # Playoff match_type_id
                    'Playoff',      # match_type_name
                    f"2013_playoff_{match['match_id']}.csv",  # filename reference
                    'manual_entry'  # extraction_status
                ))
                
                successful += 1
                home_name = self._get_team_name(conn, home_team_id)
                away_name = self._get_team_name(conn, away_team_id)
                print(f"  ✅ {match['match_id']}: {home_name} vs {away_name} ({match_date}) - Playoff")
                
            except Exception as e:
                failed += 1
                print(f"  ❌ Error adding {match['match_id']}: {e}")
        
        conn.commit()
        conn.close()
        
        print(f"\n🎉 2013 Playoff matches added!")
        print(f"   ✅ Successfully added: {successful}")
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
        
        # Add 2013-specific team name mappings
        mapping.update({
            'Kansas City': '6f666306',  # FC Kansas City -> Kansas City Current
            'Thorns': 'df9a10a1',      # Portland Thorns FC
            'WNY Flash': '5f911568',    # Western New York Flash
            'Sky Blue FC': '8e306dc6'   # -> Gotham FC
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
    
    def verify_2013_matches(self):
        """Verify all 2013 matches including playoffs"""
        print("🔍 Verifying 2013 matches with playoffs...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Count by match type
        counts = conn.execute('''
            SELECT match_type_name, COUNT(*) as count
            FROM match_inventory 
            WHERE season_id = 2013
            GROUP BY match_type_name
            ORDER BY match_type_name
        ''').fetchall()
        
        total = sum(count for _, count in counts)
        
        print(f"   📊 2013 Season Summary:")
        for type_name, count in counts:
            print(f"      {type_name}: {count} matches")
        print(f"      Total: {total} matches")
        
        # Show sample playoff matches
        playoffs = conn.execute('''
            SELECT match_id, match_date, 
                   (SELECT team_name FROM teams WHERE team_id = home_team_id) as home_team,
                   (SELECT team_name FROM teams WHERE team_id = away_team_id) as away_team,
                   match_type_name
            FROM match_inventory 
            WHERE season_id = 2013 AND match_type_name = 'Playoff'
            ORDER BY match_date
        ''').fetchall()
        
        if playoffs:
            print(f"\n   🏆 2013 Playoff matches:")
            for match_id, date, home, away, type_name in playoffs:
                print(f"      {match_id}: {home} vs {away} ({date}) - {type_name}")
        
        conn.close()

if __name__ == "__main__":
    adder = Add2013Playoffs()
    adder.add_2013_playoff_matches()
    adder.verify_2013_matches()