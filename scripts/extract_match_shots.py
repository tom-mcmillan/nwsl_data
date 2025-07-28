#!/usr/bin/env python3
"""
Extract shot data from FBRef HTML match reports
Based on FBRef shot table structure with flattened headers
"""

import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
import os
import secrets
import re
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

class MatchShotExtractor:
    def __init__(self, db_path='data/processed/nwsldata.db', html_dir='data/raw_match_pages'):
        self.db_path = db_path
        self.html_dir = html_dir
        self.driver = None
        
    def setup_selenium(self):
        """Setup Selenium driver for dynamic content"""
        if self.driver is None:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            
            try:
                self.driver = webdriver.Chrome(options=chrome_options)
            except Exception as e:
                print(f"   ⚠️ Selenium setup failed: {e}")
                return False
        return True
        
    def close_selenium(self):
        """Close Selenium driver"""
        if self.driver:
            self.driver.quit()
            self.driver = None
        
    def generate_shot_id(self):
        """Generate shot_id with s_ prefix + 8 character hex"""
        return f"s_{secrets.token_hex(4)}"
        
    def extract_shots_from_html(self, html_file_path, match_id):
        """Extract shot data from FBRef HTML file using BeautifulSoup + Selenium fallback"""
        shots = []
        
        # First try with BeautifulSoup (faster)
        shots = self._try_beautifulsoup_extraction(html_file_path, match_id)
        
        # If no shots found and tables seem to exist but empty, try Selenium
        if len(shots) == 0:
            print(f"   🔄 No shots with BeautifulSoup, trying Selenium for dynamic content...")
            shots = self._try_selenium_extraction(html_file_path, match_id)
            
        return shots
    
    def _try_beautifulsoup_extraction(self, html_file_path, match_id):
        """Try extraction with BeautifulSoup first"""
        shots = []
        
        try:
            with open(html_file_path, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f.read(), 'html.parser')
            
            # Look for shot tables - FBRef uses different IDs for home/away teams
            shot_tables = []
            
            # Common shot table patterns in FBRef
            possible_ids = [
                'shots_all',
                'shots_home', 
                'shots_away',
                'shots'
            ]
            
            for table_id in possible_ids:
                table = soup.find('table', id=table_id)
                if table:
                    shot_tables.append(table)
            
            # Also look for tables with class that might contain shots
            shot_class_tables = soup.find_all('table', class_=lambda x: x and 'shots' in x.lower())
            shot_tables.extend(shot_class_tables)
            
            if not shot_tables:
                print(f"   ⚠️ No shot tables found in BeautifulSoup extraction")
                return shots
            
            for table in shot_tables:
                try:
                    # Convert table to DataFrame
                    df = pd.read_html(str(table))[0]
                    
                    # Handle multi-level column headers (FBRef often has these)
                    if isinstance(df.columns, pd.MultiIndex):
                        # Flatten multi-level columns
                        df.columns = ['_'.join(col).strip() for col in df.columns.values]
                    
                    # Clean column names
                    df.columns = [col.replace(' ', '_').lower() for col in df.columns]
                    
                    # Check if this looks like a shot table
                    if not any(col in str(df.columns).lower() for col in ['minute', 'player', 'xg']):
                        continue
                    
                    print(f"   📊 Found shot table with columns: {list(df.columns)}")
                    
                    # Process each row
                    for idx, row in df.iterrows():
                        # Skip empty rows or header rows
                        player_val = row.get('player', '') if 'player' in df.columns else row.get('unnamed:_1_level_0_player', '')
                        if pd.isna(player_val) or str(player_val).strip() == '' or str(player_val).strip() == 'Player':
                            continue
                            
                        shot_data = {
                            'shot_id': self.generate_shot_id(),
                            'match_id': match_id,
                            'minute': self._safe_int(row.get('minute', row.get('unnamed:_0_level_0_minute'))),
                            'player_name': str(player_val).strip(),
                            'player_id': None,  # Will need to resolve from player table
                            'squad': str(row.get('squad', row.get('unnamed:_2_level_0_squad', ''))).strip(),
                            'xg': self._safe_float(row.get('xg', row.get('unnamed:_3_level_0_xg'))),
                            'psxg': self._safe_float(row.get('psxg', row.get('unnamed:_4_level_0_psxg'))),
                            'outcome': str(row.get('outcome', row.get('unnamed:_5_level_0_outcome', ''))).strip(),
                            'distance': self._safe_int(row.get('distance', row.get('unnamed:_6_level_0_distance'))),
                            'body_part': str(row.get('body_part', row.get('unnamed:_7_level_0_body_part', ''))).strip(),
                            'notes': str(row.get('notes', row.get('unnamed:_8_level_0_notes', ''))).strip(),
                            'sca1_player_name': str(row.get('sca_1_player', '')).strip(),
                            'sca1_event': str(row.get('sca_1_event', '')).strip(),
                            'sca2_player_name': str(row.get('sca_2_player', '')).strip(),
                            'sca2_event': str(row.get('sca_2_event', '')).strip(),
                        }
                        
                        shots.append(shot_data)
                        
                except Exception as e:
                    print(f"   ❌ Error processing BeautifulSoup shot table: {e}")
                    continue
                    
        except Exception as e:
            print(f"   ❌ Error reading HTML file {html_file_path}: {e}")
            
        return shots
    
    def _try_selenium_extraction(self, html_file_path, match_id):
        """Try extraction with Selenium for dynamically loaded content"""
        shots = []
        
        if not self.setup_selenium():
            return shots
            
        try:
            # Convert file path to file:// URL for Selenium
            file_url = f"file://{os.path.abspath(html_file_path)}"
            self.driver.get(file_url)
            
            # Wait for page to load
            time.sleep(3)
            
            # Look for shot tables with Selenium
            shot_table_ids = ['shots_all', 'shots_home', 'shots_away', 'shots']
            
            for table_id in shot_table_ids:
                try:
                    wait = WebDriverWait(self.driver, 5)
                    table_element = wait.until(
                        EC.presence_of_element_located((By.ID, table_id))
                    )
                    
                    # Get the table HTML
                    html_table = table_element.get_attribute('outerHTML')
                    
                    # Convert to DataFrame
                    df = pd.read_html(html_table)[0]
                    
                    # Handle multi-level column headers
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = ['_'.join(col).strip() for col in df.columns.values]
                    
                    # Clean column names
                    df.columns = [col.replace(' ', '_').lower() for col in df.columns]
                    
                    print(f"   🚀 Selenium found shot table '{table_id}' with columns: {list(df.columns)}")
                    
                    # Process each row
                    for idx, row in df.iterrows():
                        player_val = row.get('player', row.get('unnamed:_1_level_0_player', ''))
                        if pd.isna(player_val) or str(player_val).strip() == '' or str(player_val).strip() == 'Player':
                            continue
                            
                        shot_data = {
                            'shot_id': self.generate_shot_id(),
                            'match_id': match_id,
                            'minute': self._safe_int(row.get('minute', row.get('unnamed:_0_level_0_minute'))),
                            'player_name': str(player_val).strip(),
                            'player_id': None,
                            'squad': str(row.get('squad', row.get('unnamed:_2_level_0_squad', ''))).strip(),
                            'xg': self._safe_float(row.get('xg', row.get('unnamed:_3_level_0_xg'))),
                            'psxg': self._safe_float(row.get('psxg', row.get('unnamed:_4_level_0_psxg'))),
                            'outcome': str(row.get('outcome', row.get('unnamed:_5_level_0_outcome', ''))).strip(),
                            'distance': self._safe_int(row.get('distance', row.get('unnamed:_6_level_0_distance'))),
                            'body_part': str(row.get('body_part', row.get('unnamed:_7_level_0_body_part', ''))).strip(),
                            'notes': str(row.get('notes', row.get('unnamed:_8_level_0_notes', ''))).strip(),
                            'sca1_player_name': str(row.get('sca_1_player', '')).strip(),
                            'sca1_event': str(row.get('sca_1_event', '')).strip(),
                            'sca2_player_name': str(row.get('sca_2_player', '')).strip(),
                            'sca2_event': str(row.get('sca_2_event', '')).strip(),
                        }
                        
                        shots.append(shot_data)
                        
                except Exception as e:
                    # This is expected if table doesn't exist
                    continue
                    
        except Exception as e:
            print(f"   ❌ Selenium extraction failed: {e}")
            
        return shots
    
    def _extract_match_id_from_canonical_url(self, html_file_path):
        """Extract match_id from canonical URL in HTML file"""
        try:
            with open(html_file_path, 'r', encoding='utf-8') as f:
                # Read more content to ensure we get canonical URL (50KB should be enough)
                content = f.read(50000)
            
            # Check if this is an error page (rate limited, etc.)
            if 'sports-reference.com/429.html' in content:
                print(f"   ⚠️ File is a rate-limited error page - skipping")
                return None
            
            # Look for FBRef canonical URL pattern
            pattern = r'<link\s+rel=["\']canonical["\']\s+href=["\']https://fbref\.com/en/matches/([a-f0-9]{8})/'
            match = re.search(pattern, content, re.IGNORECASE)
            
            if match:
                match_id = match.group(1)
                print(f"   🔍 Found match_id: {match_id}")
                return match_id
            
            # Debug: print what canonical URL we found instead
            canonical_match = re.search(r'<link\s+rel=["\']canonical["\']\s+href=["\']([^"\']+)["\']', content, re.IGNORECASE)
            if canonical_match:
                canonical_url = canonical_match.group(1)
                if 'sports-reference.com' in canonical_url:
                    print(f"   🔍 Found sports-reference.com canonical URL (pre-FBRef era)")
                else:
                    print(f"   🔍 Found canonical URL but not expected FBRef format: {canonical_url[:100]}")
            else:
                print(f"   🔍 No canonical URL found in first 50KB")
            
            return None
            
        except Exception as e:
            print(f"   ❌ Error reading {html_file_path.name}: {e}")
            return None
    
    def _safe_int(self, value):
        """Safely convert value to integer"""
        if pd.isna(value) or value == '':
            return None
        try:
            # Handle cases like "45+4" for injury time
            if '+' in str(value):
                return int(str(value).split('+')[0])
            return int(float(str(value)))
        except (ValueError, TypeError):
            return None
    
    def _safe_float(self, value):
        """Safely convert value to float"""
        if pd.isna(value) or value == '':
            return None
        try:
            return float(str(value))
        except (ValueError, TypeError):
            return None
    
    def resolve_player_ids(self, shots):
        """Resolve player names to player_ids from the player table"""
        if not shots:
            return shots
            
        conn = sqlite3.connect(self.db_path)
        
        for shot in shots:
            player_name = shot['player_name']
            if player_name:
                # Try exact match first
                result = conn.execute(
                    'SELECT player_id FROM player WHERE player_name = ? LIMIT 1',
                    (player_name,)
                ).fetchone()
                
                if result:
                    shot['player_id'] = result[0]
                else:
                    # Try partial match for different name formats
                    result = conn.execute(
                        'SELECT player_id FROM player WHERE player_name LIKE ? LIMIT 1',
                        (f'%{player_name}%',)
                    ).fetchone()
                    
                    if result:
                        shot['player_id'] = result[0]
                    else:
                        print(f"   ⚠️ Could not resolve player: {player_name}")
        
        conn.close()
        return shots
    
    def save_shots_to_db(self, shots):
        """Save shot data to database"""
        if not shots:
            return 0
            
        conn = sqlite3.connect(self.db_path)
        
        insert_sql = '''
            INSERT INTO match_shot (
                shot_id, match_id, minute, player_name, player_id, squad,
                xg, psxg, outcome, distance, body_part, notes,
                sca1_player_name, sca1_event, sca2_player_name, sca2_event
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        shot_tuples = [
            (
                shot['shot_id'], shot['match_id'], shot['minute'], shot['player_name'],
                shot['player_id'], shot['squad'], shot['xg'], shot['psxg'],
                shot['outcome'], shot['distance'], shot['body_part'], shot['notes'],
                shot['sca1_player_name'], shot['sca1_event'], 
                shot['sca2_player_name'], shot['sca2_event']
            )
            for shot in shots
        ]
        
        conn.executemany(insert_sql, shot_tuples)
        conn.commit()
        
        inserted_count = conn.total_changes
        conn.close()
        
        return inserted_count
    
    def extract_pilot_batch(self, limit=None):
        """Extract shots from ALL HTML files that might contain shot data"""
        if limit:
            print(f"🎯 Starting shot extraction from {limit} HTML files...")
        else:
            print(f"🎯 Starting shot extraction from ALL HTML files...")
        
        html_files = []
        html_dir_path = Path(self.html_dir)
        
        # Get all HTML files that might contain match data
        for file_path in html_dir_path.glob('*.html'):
            # Process files with these patterns:
            # 1. [match_id].html (8-character hex)
            # 2. match_report_YEAR_*.html (2019+)
            if (re.match(r'^[a-f0-9]{8}\.html$', file_path.name) or 
                re.match(r'^match_report_(201[9]|202[0-9])_.*\.html$', file_path.name)):
                html_files.append(file_path)
                if limit and len(html_files) >= limit:
                    break
        
        if not html_files:
            print(f"   ❌ No properly named HTML files found in {self.html_dir}")
            return
        
        print(f"   📁 Found {len(html_files)} properly named files")
        
        # Verify match_ids exist in database
        conn = sqlite3.connect(self.db_path)
        
        total_shots = 0
        
        for html_file in html_files:
            print(f"\n📄 Processing: {html_file.name}")
            
            # Extract match_id from filename or canonical URL
            if re.match(r'^[a-f0-9]{8}\.html$', html_file.name):
                # Already properly named
                match_id = html_file.stem
            else:
                # Extract from canonical URL in the HTML file
                match_id = self._extract_match_id_from_canonical_url(html_file)
                if not match_id:
                    print(f"   ⚠️ Could not extract match_id from canonical URL - skipping")
                    continue
            
            # Verify match exists in database
            result = conn.execute('SELECT 1 FROM match WHERE match_id = ?', (match_id,)).fetchone()
            if not result:
                print(f"   ⚠️ Match {match_id} not found in database - skipping")
                continue
            
            shots = self.extract_shots_from_html(html_file, match_id)
            
            if shots:
                shots = self.resolve_player_ids(shots)
                inserted = self.save_shots_to_db(shots)
                total_shots += inserted
                print(f"   ✅ Extracted {inserted} shots for match {match_id}")
            else:
                print(f"   ⚠️ No shots found in match {match_id}")
        
        conn.close()
        self.close_selenium()  # Clean up Selenium driver
        
        print(f"\n🎉 Pilot extraction complete!")
        print(f"   📊 Total shots extracted: {total_shots}")
        print(f"   📁 Files processed: {len(html_files)}")
        
        return total_shots

def main():
    extractor = MatchShotExtractor()
    
    # Extract shots from all HTML files with shot data (2019+)
    print("🎯 Extracting shots from ALL HTML files with potential shot data")
    print("Processing files from 2019+ to populate match_shot table...")
    
    # Process all available HTML files with shot data
    extractor.extract_pilot_batch()

if __name__ == "__main__":
    main()