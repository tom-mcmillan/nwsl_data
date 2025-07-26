#!/usr/bin/env python3
"""
Import 2022 NWSL Challenge Cup CSV to match_inventory
"""

import sqlite3
import csv
from datetime import datetime

class Import2022ChallengeCupCSV:
    def __init__(self, db_path='data/processed/nwsldata.db'):
        self.db_path = db_path
        
    def import_csv_to_match_inventory(self, csv_path='data/excel_sheets/2022_challengeCup.csv'):
        """Import 2022 Challenge Cup CSV data to match_inventory table"""
        print("🏆 Starting 2022 Challenge Cup CSV import to match_inventory...")
        
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
                    match_subtype_name = row['match type'].strip()
                    match_id = row['Match ID'].strip()
                    date_str = row['Date'].strip()
                    home_team = row['Home'].strip()
                    away_team = row['Away'].strip()
                    
                    # Extract xG data (available in 2022)
                    xg_home = self._parse_xg(row.get('xg_home', ''))
                    xg_away = self._parse_xg(row.get('xg_away', ''))
                    
                    # Convert date from M/D/YY to YYYY-MM-DD
                    match_date = self._parse_date(date_str)
                    if not match_date:
                        print(f"⚠️ Could not parse date: {date_str}")
                        failed += 1
                        continue
                    
                    # Map team names to IDs
                    home_team_id = self._map_team_name_to_id(home_team, team_mapping)
                    away_team_id = self._map_team_name_to_id(away_team, team_mapping)
                    
                    if not home_team_id or not away_team_id:
                        print(f"⚠️ Could not map teams: {home_team} vs {away_team}")
                        failed += 1
                        continue
                    
                    # Set Challenge Cup match type
                    match_type_id = 'mt_i9j0k1l2'  # Challenge Cup
                    match_type_name = 'Challenge Cup'
                    
                    # Map match subtype name to subtype_id (2022 format)
                    match_subtype_id = self._map_subtype_name_to_id(match_subtype_name)
                    if not match_subtype_id:
                        print(f"⚠️ Unknown Challenge Cup subtype: {match_subtype_name}")
                        failed += 1
                        continue
                    
                    # Insert into match_inventory with Challenge Cup details
                    conn.execute('''
                        INSERT OR REPLACE INTO match_inventory 
                        (match_id, match_date, home_team_id, away_team_id, season_id, 
                         match_type_id, match_type_name, match_subtype_id, match_subtype_name, filename, extraction_status, xg_home, xg_away)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        match_id,
                        match_date,
                        home_team_id,
                        away_team_id,
                        2022,  # season_id
                        match_type_id,
                        match_type_name,
                        match_subtype_id,
                        match_subtype_name,
                        f"2022_challengecup_{match_id}.csv",  # filename reference
                        'csv_import',  # extraction_status
                        xg_home,
                        xg_away
                    ))
                    
                    successful += 1
                    
                    # Get team names for display
                    home_name = self._get_team_name(conn, home_team_id)
                    away_name = self._get_team_name(conn, away_team_id)
                    xg_display = f"xG: {xg_home}-{xg_away}" if xg_home is not None and xg_away is not None else ""
                    print(f"  ✅ {match_id}: {home_name} vs {away_name} ({match_date}) - {match_subtype_name} {xg_display}")
                    
                except Exception as e:
                    failed += 1
                    print(f"  ❌ Error importing row: {e}")
                    print(f"     Row data: {row}")
        
        conn.commit()
        conn.close()
        
        print(f"\n🎉 2022 Challenge Cup CSV import complete!")
        print(f"   ✅ Successfully imported: {successful}")
        print(f"   ❌ Failed: {failed}")
        print(f"   🏆 Total 2022 Challenge Cup matches in inventory: {successful}")
        
        return successful
    
    def _parse_date(self, date_str):
        """Parse date from M/D/YY format to YYYY-MM-DD"""
        try:
            # Handle formats like "3/18/22"
            month, day, year = date_str.split('/')
            
            # Convert 2-digit year to 4-digit (22 -> 2022)
            if len(year) == 2:
                year = f"20{year}"
            
            # Create datetime object and format
            date_obj = datetime(int(year), int(month), int(day))
            return date_obj.strftime('%Y-%m-%d')
            
        except Exception as e:
            print(f"⚠️ Date parsing error for '{date_str}': {e}")
            return None
    
    def _parse_xg(self, xg_str):
        """Parse xG value to float, handle empty/missing values"""
        if not xg_str or xg_str.strip() == '':
            return None
        try:
            return float(xg_str.strip())
        except (ValueError, TypeError):
            print(f"⚠️ Could not parse xG value: '{xg_str}'")
            return None
    
    def _map_subtype_name_to_id(self, subtype_name):
        """Map 2022 Challenge Cup subtype name to subtype_id"""
        # Map 2022 Challenge Cup subtype names to our hex subtype_ids
        # 2022 format: Group stage + Semifinals + Final
        mapping = {
            'Group stage': 'mst_p1q2r3s4',  # Challenge Cup Group Stage
            'Semifinals': 'mst_y5z6a7b8',   # Challenge Cup Semifinals
            'Final': 'mst_c9d0e1f2'        # Challenge Cup Final
        }
        
        return mapping.get(subtype_name)
    
    def _build_team_mapping(self, conn):
        """Build mapping from team names to team_ids using teams table and aliases"""
        # Get all teams with their aliases
        teams = conn.execute('''
            SELECT team_id, team_name, team_name_short, team_name_alias_1, team_name_alias_2 
            FROM teams
        ''').fetchall()
        
        mapping = {}
        for team_id, team_name, short_name, alias1, alias2 in teams:
            # Add full team name
            mapping[team_name] = team_id
            
            # Add short name if it exists
            if short_name:
                mapping[short_name] = team_id
            
            # Add alias 1 if it exists
            if alias1:
                mapping[alias1] = team_id
            
            # Add alias 2 if it exists
            if alias2:
                mapping[alias2] = team_id
        
        # Add 2022-specific team mappings (expansion teams + rebrands)
        mapping['Louisville'] = 'da19ebd1'    # Racing Louisville
        mapping['Current'] = '6f666306'       # Kansas City Current
        mapping['Reign'] = '257fad2b'         # OL Reign/Seattle Reign FC
        mapping['OL Reign'] = '257fad2b'      # OL Reign/Seattle Reign FC
        mapping['Thorns'] = '3b8ddb74'        # Portland Thorns FC
        mapping['Courage'] = '1a7ba321'       # North Carolina Courage
        mapping['Gotham FC'] = '395c0cec'     # Gotham FC 
        mapping['Pride'] = 'e41b4b89'         # Orlando Pride
        mapping['Spirit'] = '7dae8ad0'        # Washington Spirit
        mapping['Angel City'] = 'ae38d267'    # Angel City FC (new 2022)
        mapping['Wave'] = 'bf961da0'          # San Diego Wave FC (new 2022)
        mapping['Dash'] = '9aa0a3b3'          # Houston Dash
        mapping['Red Stars'] = 'ee1e0cbb'     # Chicago Red Stars
        
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
    
    def _get_team_name(self, conn, team_id):
        """Get team name from team_id"""
        result = conn.execute('SELECT team_name FROM teams WHERE team_id = ?', (team_id,)).fetchone()
        return result[0] if result else team_id
    
    def verify_import(self):
        """Verify the imported Challenge Cup data"""
        print("🔍 Verifying 2022 Challenge Cup import...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Count 2022 Challenge Cup matches by subtype
        counts = conn.execute('''
            SELECT ms.subtype_name, COUNT(*) as count, ms.display_order
            FROM match_inventory mi
            LEFT JOIN match_subtype ms ON mi.match_subtype_id = ms.subtype_id
            WHERE mi.season_id = 2022 AND mi.match_type_name = 'Challenge Cup'
            GROUP BY ms.subtype_name, ms.display_order
            ORDER BY 
                CASE ms.subtype_name 
                    WHEN 'Group Stage' THEN 1
                    WHEN 'Semifinals' THEN 2
                    WHEN 'Final' THEN 3
                    ELSE 4
                END
        ''').fetchall()
        
        total_cc = sum(count for _, count, _ in counts)
        
        print(f"   🏆 2022 Challenge Cup Summary:")
        for subtype_name, count, _ in counts:
            if subtype_name:
                print(f"      {subtype_name}: {count} matches")
            else:
                print(f"      Unknown subtype: {count} matches")
        print(f"      Total: {total_cc} matches")
        
        # Show sample matches from each subtype
        for subtype_name, count, _ in counts:
            if subtype_name and count > 0:
                samples = conn.execute('''
                    SELECT mi.match_id, mi.match_date, 
                           (SELECT team_name FROM teams WHERE team_id = mi.home_team_id) as home_team,
                           (SELECT team_name FROM teams WHERE team_id = mi.away_team_id) as away_team,
                           mi.xg_home, mi.xg_away
                    FROM match_inventory mi
                    LEFT JOIN match_subtype ms ON mi.match_subtype_id = ms.subtype_id
                    WHERE mi.season_id = 2022 AND mi.match_type_name = 'Challenge Cup' 
                          AND ms.subtype_name = ?
                    ORDER BY mi.match_date
                    LIMIT 3
                ''', (subtype_name,)).fetchall()
                
                print(f"\n   🏆 Sample {subtype_name} matches:")
                for match_id, date, home, away, xg_h, xg_a in samples:
                    xg_display = f" (xG: {xg_h}-{xg_a})" if xg_h is not None and xg_a is not None else ""
                    print(f"      {match_id}: {home} vs {away} ({date}){xg_display}")
        
        # Show xG statistics for 2022 Challenge Cup
        xg_stats = conn.execute('''
            SELECT COUNT(*) as total_matches,
                   COUNT(xg_home) as matches_with_xg,
                   AVG(xg_home) as avg_xg_home,
                   AVG(xg_away) as avg_xg_away,
                   MAX(xg_home) as max_xg_home,
                   MAX(xg_away) as max_xg_away
            FROM match_inventory 
            WHERE season_id = 2022 AND match_type_name = 'Challenge Cup'
        ''').fetchone()
        
        total, with_xg, avg_h, avg_a, max_h, max_a = xg_stats
        print(f"\n   📈 2022 Challenge Cup xG Statistics:")
        print(f"      Total matches: {total}")
        print(f"      Matches with xG data: {with_xg}")
        if avg_h and avg_a:
            print(f"      Average xG per match: {avg_h:.2f} (home) vs {avg_a:.2f} (away)")
            print(f"      Highest xG in a match: {max_h:.1f} (home), {max_a:.1f} (away)")
        
        # Show updated complete Challenge Cup inventory
        all_cc_counts = conn.execute('''
            SELECT season_id, COUNT(*) as count
            FROM match_inventory 
            WHERE match_type_name = 'Challenge Cup'
            GROUP BY season_id
            ORDER BY season_id
        ''').fetchall()
        
        print(f"\n   📈 All Challenge Cup tournaments:")
        total_cc_all = 0
        for season, count in all_cc_counts:
            print(f"      {season}: {count} matches")
            total_cc_all += count
        print(f"      Total Challenge Cup matches: {total_cc_all}")
        
        conn.close()

if __name__ == "__main__":
    importer = Import2022ChallengeCupCSV()
    importer.import_csv_to_match_inventory()
    importer.verify_import()