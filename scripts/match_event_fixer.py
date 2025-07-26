#!/usr/bin/env python3
"""
Match Event Fixer
Fixes referential integrity issues by mapping event match_ids to real match table IDs
"""

import sqlite3
import os
import glob
from bs4 import BeautifulSoup
import re
from datetime import datetime

class MatchEventFixer:
    def __init__(self, db_path='data/processed/nwsldata.db'):
        self.db_path = db_path
        
    def fix_match_event_references(self):
        """Fix match_event table to reference actual matches from match table"""
        print("🔧 Starting match event reference fixing...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Get all matches from match table for mapping
        matches = conn.execute('''
            SELECT match_id, home_team_id, away_team_id, match_date, filename 
            FROM match 
            WHERE match_id IS NOT NULL AND match_id != ''
            ORDER BY match_date DESC
        ''').fetchall()
        
        print(f"📊 Found {len(matches)} valid matches in match table")
        
        # Get all raw HTML files
        raw_pages_dir = 'data/raw_match_pages'
        html_files = glob.glob(os.path.join(raw_pages_dir, '*.html'))
        print(f"📂 Found {len(html_files)} HTML files")
        
        # Map HTML files to existing matches based on content
        mapping_results = []
        
        for html_file in html_files:
            try:
                match_data = self._extract_match_data_from_html(html_file)
                if match_data:
                    # Find corresponding match in database
                    matched_id = self._find_matching_database_match(conn, match_data, matches)
                    if matched_id:
                        # Get current event match_id from filename
                        current_event_match_id = self._get_match_id_from_filename(html_file)
                        mapping_results.append({
                            'file': os.path.basename(html_file),
                            'old_match_id': current_event_match_id,
                            'new_match_id': matched_id,
                            'teams': f"{match_data['home_team']} vs {match_data['away_team']}",
                            'date': match_data.get('date', 'unknown')
                        })
            except Exception as e:
                print(f"⚠️ Error processing {os.path.basename(html_file)}: {e}")
        
        print(f"✅ Successfully mapped {len(mapping_results)} files to database matches")
        
        # Update match_event table with correct match_ids
        updated_count = 0
        for mapping in mapping_results:
            try:
                result = conn.execute('''
                    UPDATE match_event 
                    SET match_id = ? 
                    WHERE match_id = ?
                ''', (mapping['new_match_id'], mapping['old_match_id']))
                
                rows_affected = result.rowcount
                if rows_affected > 0:
                    updated_count += rows_affected
                    print(f"  📝 Updated {rows_affected} events: {mapping['old_match_id']} → {mapping['new_match_id']} ({mapping['teams']})")
                    
            except Exception as e:
                print(f"⚠️ Error updating events for {mapping['file']}: {e}")
        
        conn.commit()
        conn.close()
        
        print(f"\n🎉 Match event fixing complete!")
        print(f"   ✅ Updated {updated_count} event records")
        print(f"   📊 Mapped {len(mapping_results)} HTML files to database matches")
        
        return updated_count
    
    def _extract_match_data_from_html(self, html_file_path):
        """Extract team names and date from HTML file"""
        try:
            with open(html_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            soup = BeautifulSoup(content, 'html.parser')
            
            # Extract teams from title
            title_element = soup.find('title')
            if not title_element:
                return None
                
            title_text = title_element.get_text()
            
            # Parse title: "Team A vs. Team B Match Report – Date | FBref.com"
            if ' vs. ' in title_text:
                teams_part = title_text.split(' Match Report')[0]
                if ' vs. ' in teams_part:
                    teams = teams_part.split(' vs. ')
                    if len(teams) >= 2:
                        home_team = teams[0].strip()
                        away_team = teams[1].strip()
                        
                        # Try to extract date from title
                        date_match = re.search(r'–\s*(.*?)\s*\d{4}', title_text)
                        date_str = None
                        if date_match:
                            date_part = date_match.group(1).strip()
                            # Try to parse the date
                            try:
                                # Handle format like "Saturday June 7, 2025"
                                date_match_full = re.search(r'(\w+\s+\w+\s+\d+,\s+\d{4})', title_text)
                                if date_match_full:
                                    date_str = date_match_full.group(1)
                            except:
                                pass
                        
                        return {
                            'home_team': home_team,
                            'away_team': away_team,
                            'date': date_str,
                            'title': title_text
                        }
            
            return None
            
        except Exception as e:
            print(f"⚠️ Error extracting data from {os.path.basename(html_file_path)}: {e}")
            return None
    
    def _find_matching_database_match(self, conn, match_data, matches):
        """Find matching match_id in database based on team names"""
        
        # Get team name mappings from database
        team_mapping = {}
        teams_result = conn.execute('SELECT team_id, team_name FROM teams').fetchall()
        for team_id, team_name in teams_result:
            team_mapping[team_name] = team_id
        
        # Try to find team IDs for the HTML teams
        home_team_id = None
        away_team_id = None
        
        # Direct name matching
        for team_name, team_id in team_mapping.items():
            if team_name == match_data['home_team']:
                home_team_id = team_id
            if team_name == match_data['away_team']:
                away_team_id = team_id
        
        # Fuzzy matching for common variations
        if not home_team_id:
            home_team_id = self._fuzzy_match_team(match_data['home_team'], team_mapping)
        if not away_team_id:
            away_team_id = self._fuzzy_match_team(match_data['away_team'], team_mapping)
        
        # Look for match with these team IDs
        if home_team_id and away_team_id:
            for match_id, db_home_id, db_away_id, match_date, filename in matches:
                if ((db_home_id == home_team_id and db_away_id == away_team_id) or
                    (db_home_id == away_team_id and db_away_id == home_team_id)):  # Handle reversed
                    return match_id
        
        return None
    
    def _fuzzy_match_team(self, team_name, team_mapping):
        """Fuzzy match team name to handle variations"""
        # Common team name variations
        variations = {
            'Seattle Reign FC': ['Reign', 'OL Reign', 'Seattle Reign'],
            'Kansas City Current': ['Current', 'Kansas City'],
            'Chicago Stars FC': ['Chicago Stars', 'Chicago Red Stars', 'Red Stars'],
            'Racing Louisville': ['Louisville'],
            'Utah Royals': ['Royals', 'Utah Royals FC'],
            'Gotham FC': ['Sky Blue FC'],
            'San Diego Wave FC': ['Wave'],
            'Angel City FC': ['Angel City'],
            'North Carolina Courage': ['Courage'],
            'Houston Dash': ['Dash'],
            'Washington Spirit': ['Spirit'],
            'Portland Thorns FC': ['Thorns'],
            'Orlando Pride': ['Pride'],
            'Boston Breakers': ['Breakers']
        }
        
        # Check if team_name matches any variation
        for canonical_name, aliases in variations.items():
            if team_name in aliases or canonical_name == team_name:
                # Return the team_id for the canonical name
                return team_mapping.get(canonical_name)
        
        # Direct partial matching
        for db_team_name, team_id in team_mapping.items():
            if team_name in db_team_name or db_team_name in team_name:
                return team_id
        
        return None
    
    def _get_match_id_from_filename(self, filename):
        """Extract match_id from filename (same logic as batch_event_scraper)"""
        basename = os.path.basename(filename)
        
        # Handle hex ID filenames like "0716dcc9.html" or "f09b4cef.html"
        if len(basename) == 13 and basename.endswith('.html') and len(basename[:-5]) == 8:
            return basename[:-5]
        
        # Handle match_report_YYYY_[hex].html format
        if basename.startswith('match_report_') and '_' in basename:
            parts = basename.replace('.html', '').split('_')
            if len(parts) >= 3:
                # Get the hex ID part (last part)
                hex_id = parts[-1]
                if len(hex_id) == 8:
                    return hex_id
        
        # Handle descriptive filenames - generate ID from hash
        import hashlib
        return hashlib.md5(basename.encode()).hexdigest()[:8]

if __name__ == "__main__":
    fixer = MatchEventFixer()
    fixer.fix_match_event_references()