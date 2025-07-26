#!/usr/bin/env python3
"""
Import 2021 NWSL Challenge Cup Excel to match_inventory
"""

import sqlite3
import pandas as pd
from datetime import datetime

class Import2021ChallengeCupExcel:
    def __init__(self, db_path='data/processed/nwsldata.db'):
        self.db_path = db_path
        
    def import_excel_to_match_inventory(self, excel_path='data/excel_sheets/2021_challengeCup.xlsx'):
        """Import 2021 Challenge Cup Excel data to match_inventory table"""
        print("🏆 Starting 2021 Challenge Cup Excel import to match_inventory...")
        
        # Read Excel file
        df = pd.read_excel(excel_path)
        print(f"📊 Loaded {len(df)} matches from Excel file")
        
        conn = sqlite3.connect(self.db_path)
        
        # Get team mapping for lookups
        team_mapping = self._build_team_mapping(conn)
        
        successful = 0
        failed = 0
        
        for index, row in df.iterrows():
            try:
                # Extract and clean data
                match_subtype_name = str(row['match type']).strip()
                match_id = str(row['Match ID']).strip()
                date_obj = row['Date']
                home_team = str(row['Home']).strip()
                away_team = str(row['Away']).strip()
                
                # Extract xG data (available in 2021)
                xg_home = self._parse_xg(row.get('xg_home', ''))
                xg_away = self._parse_xg(row.get('xg_away', ''))
                
                # Handle date - it's already a datetime object from Excel
                if pd.isna(date_obj):
                    print(f"⚠️ Could not parse date for row {index}")
                    failed += 1
                    continue
                
                match_date = date_obj.strftime('%Y-%m-%d')
                
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
                
                # Map match subtype name to subtype_id (2021 format)
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
                    2021,  # season_id
                    match_type_id,
                    match_type_name,
                    match_subtype_id,
                    match_subtype_name,
                    f"2021_challengecup_{match_id}.xlsx",  # filename reference
                    'excel_import',  # extraction_status
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
                print(f"  ❌ Error importing row {index}: {e}")
                print(f"     Row data: {row.to_dict()}")
        
        conn.commit()
        conn.close()
        
        print(f"\n🎉 2021 Challenge Cup Excel import complete!")
        print(f"   ✅ Successfully imported: {successful}")
        print(f"   ❌ Failed: {failed}")
        print(f"   🏆 Total 2021 Challenge Cup matches in inventory: {successful}")
        
        return successful
    
    def _parse_xg(self, xg_value):
        """Parse xG value to float, handle empty/missing values"""
        if pd.isna(xg_value) or str(xg_value).strip() == '':
            return None
        try:
            return float(str(xg_value).strip())
        except (ValueError, TypeError):
            print(f"⚠️ Could not parse xG value: '{xg_value}'")
            return None
    
    def _map_subtype_name_to_id(self, subtype_name):
        """Map 2021 Challenge Cup subtype name to subtype_id"""
        # Map 2021 Challenge Cup subtype names to our hex subtype_ids
        # Note: 2021 had a different format - Group stage + Final only
        mapping = {
            'Group stage': 'mst_p1q2r3s4',  # Challenge Cup Group Stage (2021 format)
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
        
        # Add 2021-specific team mappings
        mapping['Courage'] = '1a7ba321'       # North Carolina Courage
        mapping['Spirit'] = '7dae8ad0'        # Washington Spirit
        mapping['Louisville'] = 'da19ebd1'    # Racing Louisville
        mapping['Pride'] = 'e41b4b89'         # Orlando Pride
        mapping['Thorns'] = '3b8ddb74'        # Portland Thorns FC
        mapping['Kansas City'] = '6f666306'   # Kansas City (Current)
        mapping['Dash'] = '9aa0a3b3'          # Houston Dash
        mapping['Red Stars'] = 'ee1e0cbb'     # Chicago Red Stars
        mapping['Gotham FC'] = '395c0cec'     # Gotham FC (formerly Sky Blue FC)
        mapping['Reign'] = '257fad2b'         # OL Reign/Seattle Reign FC
        mapping['OL Reign'] = '257fad2b'      # OL Reign/Seattle Reign FC
        
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
        print("🔍 Verifying 2021 Challenge Cup import...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Count 2021 Challenge Cup matches by subtype
        counts = conn.execute('''
            SELECT ms.subtype_name, COUNT(*) as count
            FROM match_inventory mi
            LEFT JOIN match_subtype ms ON mi.match_subtype_id = ms.subtype_id
            WHERE mi.season_id = 2021 AND mi.match_type_name = 'Challenge Cup'
            GROUP BY ms.subtype_name
            ORDER BY 
                CASE ms.subtype_name 
                    WHEN 'Group Stage' THEN 1
                    WHEN 'Final' THEN 2
                    ELSE 3
                END
        ''').fetchall()
        
        total_cc = sum(count for _, count in counts)
        
        print(f"   🏆 2021 Challenge Cup Summary:")
        for subtype_name, count in counts:
            if subtype_name:
                print(f"      {subtype_name}: {count} matches")
            else:
                print(f"      Unknown subtype: {count} matches")
        print(f"      Total: {total_cc} matches")
        
        # Show sample matches from each subtype
        for subtype_name, _ in counts:
            if subtype_name and _ > 0:
                samples = conn.execute('''
                    SELECT mi.match_id, mi.match_date, 
                           (SELECT team_name FROM teams WHERE team_id = mi.home_team_id) as home_team,
                           (SELECT team_name FROM teams WHERE team_id = mi.away_team_id) as away_team,
                           mi.xg_home, mi.xg_away
                    FROM match_inventory mi
                    LEFT JOIN match_subtype ms ON mi.match_subtype_id = ms.subtype_id
                    WHERE mi.season_id = 2021 AND mi.match_type_name = 'Challenge Cup' 
                          AND ms.subtype_name = ?
                    ORDER BY mi.match_date
                    LIMIT 3
                ''', (subtype_name,)).fetchall()
                
                print(f"\n   🏆 Sample {subtype_name} matches:")
                for match_id, date, home, away, xg_h, xg_a in samples:
                    xg_display = f" (xG: {xg_h}-{xg_a})" if xg_h is not None and xg_a is not None else ""
                    print(f"      {match_id}: {home} vs {away} ({date}){xg_display}")
        
        # Show xG statistics for 2021 Challenge Cup
        xg_stats = conn.execute('''
            SELECT COUNT(*) as total_matches,
                   COUNT(xg_home) as matches_with_xg,
                   AVG(xg_home) as avg_xg_home,
                   AVG(xg_away) as avg_xg_away,
                   MAX(xg_home) as max_xg_home,
                   MAX(xg_away) as max_xg_away
            FROM match_inventory 
            WHERE season_id = 2021 AND match_type_name = 'Challenge Cup'
        ''').fetchone()
        
        total, with_xg, avg_h, avg_a, max_h, max_a = xg_stats
        print(f"\n   📈 2021 Challenge Cup xG Statistics:")
        print(f"      Total matches: {total}")
        print(f"      Matches with xG data: {with_xg}")
        if avg_h and avg_a:
            print(f"      Average xG per match: {avg_h:.2f} (home) vs {avg_a:.2f} (away)")
            print(f"      Highest xG in a match: {max_h:.1f} (home), {max_a:.1f} (away)")
        
        # Show updated complete inventory with both Challenge Cups
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
    importer = Import2021ChallengeCupExcel()
    importer.import_excel_to_match_inventory()
    importer.verify_import()