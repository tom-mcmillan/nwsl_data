#!/usr/bin/env python3
"""
Create Match Inventory Table
Master reference table for all NWSL matches in history
"""

import sqlite3
import os
import glob
import re
from bs4 import BeautifulSoup
from datetime import datetime

class MatchInventoryCreator:
    def __init__(self, db_path='data/processed/nwsldata.db'):
        self.db_path = db_path
        
    def create_match_inventory_table(self):
        """Create the match_inventory table"""
        print("🏗️ Creating match_inventory table...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Drop existing table if it exists
        conn.execute('DROP TABLE IF EXISTS match_inventory')
        
        # Create new match_inventory table
        conn.execute('''
            CREATE TABLE match_inventory (
                match_id TEXT PRIMARY KEY,           -- Real FBRef match ID
                match_date DATE NOT NULL,            -- Match date
                home_team_id TEXT NOT NULL,          -- Home team ID
                away_team_id TEXT NOT NULL,          -- Away team ID
                season_id INTEGER,                   -- Season reference
                filename TEXT,                       -- Source HTML filename
                extraction_status TEXT DEFAULT 'pending',  -- Status tracking
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (home_team_id) REFERENCES teams(team_id),
                FOREIGN KEY (away_team_id) REFERENCES teams(team_id)
            )
        ''')
        
        conn.commit()
        conn.close()
        
        print("✅ match_inventory table created successfully")
        
    def populate_match_inventory(self):
        """Extract match data from all HTML files and populate inventory"""
        print("📊 Starting match inventory population...")
        
        raw_pages_dir = 'data/raw_match_pages'
        html_files = glob.glob(os.path.join(raw_pages_dir, '*.html'))
        
        # Filter out tiny files (blocked scrapes)
        valid_files = [f for f in html_files if os.path.getsize(f) > 5000]
        
        print(f"📂 Processing {len(valid_files)} valid HTML files")
        
        conn = sqlite3.connect(self.db_path)
        
        # Get team mapping for lookups
        team_mapping = self._build_team_mapping(conn)
        
        successful = 0
        failed = 0
        duplicates = 0
        
        for i, html_file in enumerate(valid_files):
            try:
                if i % 100 == 0:
                    print(f"   📍 Progress: {i}/{len(valid_files)} files processed...")
                
                match_data = self._extract_match_data(html_file, team_mapping)
                
                if match_data:
                    # Try to insert into database
                    try:
                        conn.execute('''
                            INSERT INTO match_inventory 
                            (match_id, match_date, home_team_id, away_team_id, season_id, filename, extraction_status)
                            VALUES (?, ?, ?, ?, ?, ?, 'success')
                        ''', (
                            match_data['match_id'],
                            match_data['match_date'],
                            match_data['home_team_id'],
                            match_data['away_team_id'],
                            match_data['season_id'],
                            match_data['filename']
                        ))
                        successful += 1
                        
                    except sqlite3.IntegrityError:
                        # Duplicate match_id
                        duplicates += 1
                        
                else:
                    failed += 1
                    # Log failed extraction
                    conn.execute('''
                        INSERT OR IGNORE INTO match_inventory 
                        (match_id, match_date, home_team_id, away_team_id, filename, extraction_status)
                        VALUES (?, ?, ?, ?, ?, 'failed')
                    ''', (
                        f"failed_{os.path.basename(html_file)[:8]}",  # Placeholder ID
                        '1900-01-01',  # Placeholder date
                        'unknown',     # Placeholder team
                        'unknown',     # Placeholder team
                        os.path.basename(html_file),
                    ))
                    
            except Exception as e:
                failed += 1
                print(f"⚠️ Error processing {os.path.basename(html_file)}: {e}")
        
        conn.commit()
        
        # Get final counts
        total_matches = conn.execute('SELECT COUNT(*) FROM match_inventory').fetchone()[0]
        successful_matches = conn.execute('SELECT COUNT(*) FROM match_inventory WHERE extraction_status = "success"').fetchone()[0]
        
        conn.close()
        
        print(f"\n🎉 Match inventory population complete!")
        print(f"   ✅ Successful extractions: {successful}")
        print(f"   🔄 Duplicate match IDs skipped: {duplicates}")
        print(f"   ❌ Failed extractions: {failed}")
        print(f"   📊 Total matches in inventory: {total_matches}")
        print(f"   🎯 Successfully parsed matches: {successful_matches}")
        
        return successful_matches
    
    def _extract_match_data(self, html_file_path, team_mapping):
        """Extract match data from single HTML file"""
        try:
            with open(html_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            soup = BeautifulSoup(content, 'html.parser')
            filename = os.path.basename(html_file_path)
            
            # Extract FBRef match ID
            match_id = self._extract_fbref_match_id(soup, content)
            if not match_id:
                return None
            
            # Extract teams and date from title
            title = soup.find('title')
            if not title:
                return None
            
            title_text = title.get_text()
            
            # Parse title format: "Team A vs. Team B Match Report – Date | FBref.com"
            if ' vs. ' not in title_text:
                return None
            
            # Extract teams
            teams_part = title_text.split(' Match Report')[0]
            teams = teams_part.split(' vs. ')
            if len(teams) < 2:
                return None
            
            home_team_name = teams[0].strip()
            away_team_name = teams[1].strip()
            
            # Map team names to IDs
            home_team_id = self._map_team_name_to_id(home_team_name, team_mapping)
            away_team_id = self._map_team_name_to_id(away_team_name, team_mapping)
            
            if not home_team_id or not away_team_id:
                return None
            
            # Extract date
            match_date = self._extract_match_date(title_text, filename)
            if not match_date:
                return None
            
            # Determine season from date
            season_id = self._determine_season_from_date(match_date)
            
            return {
                'match_id': match_id,
                'match_date': match_date,
                'home_team_id': home_team_id,
                'away_team_id': away_team_id,
                'season_id': season_id,
                'filename': filename
            }
            
        except Exception as e:
            return None
    
    def _extract_fbref_match_id(self, soup, content):
        """Extract FBRef match ID from HTML"""
        # Method 1: Canonical URL
        canonical = soup.find('link', rel='canonical')
        if canonical:
            href = canonical.get('href', '')
            match_id = self._extract_id_from_url(href)
            if match_id:
                return match_id
        
        # Method 2: Meta tags
        meta_tags = soup.find_all('meta')
        for meta in meta_tags:
            content_attr = meta.get('content', '')
            if 'matches/' in content_attr:
                match_id = self._extract_id_from_url(content_attr)
                if match_id:
                    return match_id
        
        # Method 3: Link hrefs
        links = soup.find_all('a')
        for link in links:
            href = link.get('href', '')
            if '/matches/' in href:
                match_id = self._extract_id_from_url(href)
                if match_id:
                    return match_id
        
        # Method 4: JavaScript patterns
        scripts = soup.find_all('script')
        for script in scripts:
            if script.string:
                match = re.search(r'match[_\-]?id["\']?\s*[:=]\s*["\']?([a-f0-9]{8})["\']?', script.string, re.IGNORECASE)
                if match:
                    return match.group(1)
        
        return None
    
    def _extract_id_from_url(self, url):
        """Extract 8-character hex ID from URL"""
        match = re.search(r'/matches/([a-f0-9]{8})(?:[/-]|$)', url)
        if match:
            return match.group(1)
        
        # Backup pattern
        match = re.search(r'([a-f0-9]{8})', url)
        if match and re.match(r'^[a-f0-9]{8}$', match.group(1)):
            return match.group(1)
        
        return None
    
    def _extract_match_date(self, title_text, filename):
        """Extract match date from title or filename"""
        # From title: "Team vs Team Match Report – Saturday June 7, 2025"
        date_match = re.search(r'–\s*\w+\s+(\w+\s+\d+,\s+\d{4})', title_text)
        if date_match:
            try:
                date_str = date_match.group(1)
                date_obj = datetime.strptime(date_str, '%B %d, %Y')
                return date_obj.strftime('%Y-%m-%d')
            except:
                pass
        
        # From filename: extract year
        year_match = re.search(r'(\d{4})', filename)
        if year_match:
            year = year_match.group(1)
            # Use January 1st as placeholder - we can refine later
            return f"{year}-01-01"
        
        return None
    
    def _determine_season_from_date(self, match_date):
        """Determine NWSL season from match date"""
        year = int(match_date.split('-')[0])
        
        # NWSL seasons generally run March-November
        # For simplicity, use calendar year as season
        if year >= 2013:  # NWSL started in 2013
            return year
        
        return None
    
    def _build_team_mapping(self, conn):
        """Build mapping from team names to team_ids"""
        teams = conn.execute('SELECT team_id, team_name FROM teams').fetchall()
        
        mapping = {}
        for team_id, team_name in teams:
            mapping[team_name] = team_id
        
        # Add common aliases
        aliases = {
            'OL Reign': '257fad2b',  # Seattle Reign FC
            'Reign': '257fad2b',
            'Seattle Reign': '257fad2b',
            'Kansas City': '6f666306',  # Kansas City Current
            'Current': '6f666306',
            'Chicago Red Stars': 'd976a235',  # Chicago Stars FC
            'Red Stars': 'd976a235',
            'Chicago Stars': 'd976a235',
            'Louisville': 'da19ebd1',  # Racing Louisville
            'Royals': 'd4c130bc',  # Utah Royals
            'Utah Royals FC': 'd4c130bc',
            'Sky Blue FC': '8e306dc6',  # Gotham FC
            'Angel City': 'ae38d267',  # Angel City FC
            'Wave': 'bf961da0',  # San Diego Wave FC
            'Courage': '85c458aa',  # North Carolina Courage
            'Dash': 'e813709a',  # Houston Dash
            'Spirit': 'e442aad0',  # Washington Spirit
            'Thorns': 'df9a10a1',  # Portland Thorns FC
            'Pride': '2a6178ac',  # Orlando Pride
            'Breakers': 'ab757728',  # Boston Breakers
            'WNY Flash': '5f911568',  # Western New York Flash
            'Flash': '5f911568'
        }
        
        mapping.update(aliases)
        return mapping
    
    def _map_team_name_to_id(self, team_name, team_mapping):
        """Map team name to team_id"""
        # Direct match
        if team_name in team_mapping:
            return team_mapping[team_name]
        
        # Fuzzy match
        for mapped_name, team_id in team_mapping.items():
            if team_name in mapped_name or mapped_name in team_name:
                return team_id
        
        return None
    
    def analyze_match_inventory(self):
        """Analyze the populated match inventory"""
        print("📊 Analyzing match inventory...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Basic counts
        total = conn.execute('SELECT COUNT(*) FROM match_inventory').fetchone()[0]
        successful = conn.execute('SELECT COUNT(*) FROM match_inventory WHERE extraction_status = "success"').fetchone()[0]
        
        print(f"   📋 Total records: {total}")
        print(f"   ✅ Successful extractions: {successful}")
        print(f"   📈 Success rate: {successful/total*100:.1f}%")
        
        # By season
        seasons = conn.execute('''
            SELECT season_id, COUNT(*) as match_count 
            FROM match_inventory 
            WHERE extraction_status = "success" AND season_id IS NOT NULL
            GROUP BY season_id 
            ORDER BY season_id
        ''').fetchall()
        
        print(f"\n   📅 Matches by season:")
        for season, count in seasons:
            print(f"      {season}: {count} matches")
        
        # Sample matches
        samples = conn.execute('''
            SELECT match_id, match_date, 
                   (SELECT team_name FROM teams WHERE team_id = home_team_id) as home_team,
                   (SELECT team_name FROM teams WHERE team_id = away_team_id) as away_team
            FROM match_inventory 
            WHERE extraction_status = "success" 
            ORDER BY match_date DESC 
            LIMIT 5
        ''').fetchall()
        
        print(f"\n   🏆 Sample recent matches:")
        for match_id, date, home, away in samples:
            print(f"      {match_id}: {home} vs {away} ({date})")
        
        conn.close()

if __name__ == "__main__":
    creator = MatchInventoryCreator()
    creator.create_match_inventory_table()
    creator.populate_match_inventory()
    creator.analyze_match_inventory()