#!/usr/bin/env python3
"""
Import 2013 NWSL Regular Season CSV to match_inventory
"""

import sqlite3
import csv
from datetime import datetime

class Import2013CSV:
    def __init__(self, db_path='data/processed/nwsldata.db'):
        self.db_path = db_path
        
    def import_csv_to_match_inventory(self, csv_path='data/excel_sheets/2013_scores_and_fixtures.csv'):
        """Import CSV data to match_inventory table"""
        print("📊 Starting 2013 CSV import to match_inventory...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Get team mapping for lookups
        team_mapping = self._build_team_mapping(conn)
        
        successful = 0
        failed = 0
        
        with open(csv_path, 'r', encoding='utf-8-sig') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row in reader:
                try:
                    # Extract and clean data
                    match_id = row['Match ID'].strip()
                    date_str = row['Date'].strip()
                    home_team = row['Home'].strip()
                    away_team = row['Away'].strip()
                    
                    # Convert date from M/D/YY to YYYY-MM-DD
                    match_date = self._parse_date(date_str)
                    
                    # Map team names to IDs
                    home_team_id = self._map_team_name_to_id(home_team, team_mapping)
                    away_team_id = self._map_team_name_to_id(away_team, team_mapping)
                    
                    if not home_team_id or not away_team_id:
                        print(f"⚠️ Could not map teams: {home_team} vs {away_team}")
                        failed += 1
                        continue
                    
                    # Insert into match_inventory
                    conn.execute('''
                        INSERT OR REPLACE INTO match_inventory 
                        (match_id, match_date, home_team_id, away_team_id, season_id, filename, extraction_status)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        match_id,
                        match_date,
                        home_team_id,
                        away_team_id,
                        2013,  # season_id
                        f"2013_csv_{match_id}.csv",  # filename reference
                        'csv_import'  # extraction_status
                    ))
                    
                    successful += 1
                    print(f"  ✅ {match_id}: {home_team} vs {away_team} ({match_date})")
                    
                except Exception as e:
                    failed += 1
                    print(f"  ❌ Error importing row: {e}")
                    print(f"     Row data: {row}")
        
        conn.commit()
        conn.close()
        
        print(f"\n🎉 2013 CSV import complete!")
        print(f"   ✅ Successfully imported: {successful}")
        print(f"   ❌ Failed: {failed}")
        print(f"   📊 Total 2013 matches in inventory: {successful}")
        
        return successful
    
    def _parse_date(self, date_str):
        """Parse date from M/D/YY format to YYYY-MM-DD"""
        try:
            # Handle formats like "4/14/13" or "4/13/13"
            month, day, year = date_str.split('/')
            
            # Convert 2-digit year to 4-digit (13 -> 2013)
            if len(year) == 2:
                year = f"20{year}"
            
            # Create datetime object and format
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
            'Red Stars': 'd976a235',  # Chicago Red Stars -> Chicago Stars FC
            'Reign': '257fad2b',      # Seattle Reign FC
            'Sky Blue FC': '8e306dc6', # -> Gotham FC
            'WNY Flash': '5f911568',   # Western New York Flash
            'Boston Breakers': 'ab757728',
            'Spirit': 'e442aad0',     # Washington Spirit
            'Kansas City': '6f666306', # FC Kansas City -> Kansas City Current
            'Thorns': 'df9a10a1'     # Portland Thorns FC
        })
        
        return mapping
    
    def _map_team_name_to_id(self, team_name, team_mapping):
        """Map team name to team_id"""
        # Direct match
        if team_name in team_mapping:
            return team_mapping[team_name]
        
        # Fuzzy match for variations
        for mapped_name, team_id in team_mapping.items():
            if team_name in mapped_name or mapped_name in team_name:
                return team_id
        
        return None
    
    def verify_import(self):
        """Verify the imported data"""
        print("🔍 Verifying 2013 import...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Count 2013 matches
        count = conn.execute('SELECT COUNT(*) FROM match_inventory WHERE season_id = 2013').fetchone()[0]
        print(f"   📊 Total 2013 matches: {count}")
        
        # Show sample matches
        samples = conn.execute('''
            SELECT match_id, match_date, 
                   (SELECT team_name FROM teams WHERE team_id = home_team_id) as home_team,
                   (SELECT team_name FROM teams WHERE team_id = away_team_id) as away_team
            FROM match_inventory 
            WHERE season_id = 2013
            ORDER BY match_date
            LIMIT 5
        ''').fetchall()
        
        print(f"   🏆 Sample 2013 matches:")
        for match_id, date, home, away in samples:
            print(f"      {match_id}: {home} vs {away} ({date})")
        
        conn.close()

if __name__ == "__main__":
    importer = Import2013CSV()
    importer.import_csv_to_match_inventory()
    importer.verify_import()