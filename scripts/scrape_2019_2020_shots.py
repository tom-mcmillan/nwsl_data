#!/usr/bin/env python3
"""
Scrape 2019/2020 shot data from live FBRef URLs
"""

import sqlite3
import time
import random
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from bs4 import BeautifulSoup
import secrets

class Historical2019ShotScraper:
    def __init__(self, db_path='data/processed/nwsldata.db'):
        self.db_path = db_path
        self.driver = None
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
    def setup_selenium(self):
        """Setup Selenium driver"""
        if self.driver is None:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            
            try:
                self.driver = webdriver.Chrome(options=chrome_options)
                return True
            except Exception as e:
                print(f"   ❌ Selenium setup failed: {e}")
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
    
    def get_matches_needing_shots(self):
        """Get 2019/2020 matches that don't have shot data"""
        conn = sqlite3.connect(self.db_path)
        
        query = """
        SELECT 
            m.match_id, 
            m.match_date, 
            h.team_name as home_team,
            a.team_name as away_team
        FROM match m
        LEFT JOIN teams h ON m.home_team_id = h.team_id
        LEFT JOIN teams a ON m.away_team_id = a.team_id
        WHERE SUBSTR(m.match_date, 1, 4) IN ('2019', '2020')
        AND m.match_id NOT IN (SELECT DISTINCT match_id FROM match_shot)
        ORDER BY m.match_date
        """
        
        matches = conn.execute(query).fetchall()
        conn.close()
        
        return matches
    
    def construct_fbref_url(self, match_id, home_team, away_team, match_date):
        """Construct FBRef URL for a match"""
        # Clean team names for URL
        home_clean = home_team.replace(' ', '-').replace('FC', '').strip('-')
        away_clean = away_team.replace(' ', '-').replace('FC', '').strip('-')
        
        # Format date
        from datetime import datetime
        date_obj = datetime.strptime(match_date, '%Y-%m-%d')
        date_str = date_obj.strftime('%B-%d-%Y')
        
        url = f"https://fbref.com/en/matches/{match_id}/{home_clean}-{away_clean}-{date_str}-NWSL"
        return url
    
    def scrape_shots_from_url(self, url, match_id, use_selenium=False):
        """Scrape shots from FBRef URL"""
        shots = []
        
        try:
            if use_selenium:
                # Use Selenium for dynamic content
                if not self.setup_selenium():
                    return shots
                    
                self.driver.get(url)
                time.sleep(8)  # Wait for dynamic loading
                
                container = self.driver.find_element(By.ID, 'all_shots')
                html_content = container.get_attribute('innerHTML')
                soup = BeautifulSoup(html_content, 'html.parser')
                tables = soup.find_all('table')
                
            else:
                # Use requests + BeautifulSoup (faster)
                response = self.session.get(url)
                if response.status_code != 200:
                    return shots
                    
                soup = BeautifulSoup(response.text, 'html.parser')
                container = soup.find('div', id='all_shots')
                if not container:
                    return shots
                tables = container.find_all('table')
            
            # Process tables
            for table in tables:
                try:
                    df = pd.read_html(str(table))[0]
                    
                    # Handle multi-level columns
                    if isinstance(df.columns, pd.MultiIndex):
                        # Flatten columns
                        df.columns = ['_'.join(col).strip() for col in df.columns.values]
                    
                    # Clean column names
                    df.columns = [col.replace(' ', '_').lower() for col in df.columns]
                    
                    # Check if this is a shot table
                    col_str = str(df.columns).lower()
                    if not any(indicator in col_str for indicator in ['minute', 'player', 'xg']):
                        continue
                    
                    print(f"   📊 Found shot table with {len(df)} rows")
                    
                    # Process each shot
                    for idx, row in df.iterrows():
                        # Skip header rows
                        player_val = self._get_column_value(row, df.columns, ['player'])
                        if not player_val or str(player_val).strip() in ['Player', '']:
                            continue
                            
                        shot_data = {
                            'shot_id': self.generate_shot_id(),
                            'match_id': match_id,
                            'minute': self._safe_int(self._get_column_value(row, df.columns, ['minute'])),
                            'player_name': str(player_val).strip(),
                            'player_id': None,
                            'squad': str(self._get_column_value(row, df.columns, ['squad'])).strip(),
                            'xg': self._safe_float(self._get_column_value(row, df.columns, ['xg'])),
                            'psxg': self._safe_float(self._get_column_value(row, df.columns, ['psxg'])),
                            'outcome': str(self._get_column_value(row, df.columns, ['outcome'])).strip(),
                            'distance': self._safe_int(self._get_column_value(row, df.columns, ['distance'])),
                            'body_part': str(self._get_column_value(row, df.columns, ['body_part', 'bodypart'])).strip(),
                            'notes': str(self._get_column_value(row, df.columns, ['notes'])).strip(),
                            'sca1_player_name': str(self._get_column_value(row, df.columns, ['sca_1_player'])).strip(),
                            'sca1_event': str(self._get_column_value(row, df.columns, ['sca_1_event'])).strip(),
                            'sca2_player_name': str(self._get_column_value(row, df.columns, ['sca_2_player'])).strip(),
                            'sca2_event': str(self._get_column_value(row, df.columns, ['sca_2_event'])).strip(),
                        }
                        
                        shots.append(shot_data)
                        
                except Exception as e:
                    print(f"   ⚠️ Error processing table: {e}")
                    continue
                    
        except Exception as e:
            print(f"   ❌ Error scraping {url}: {e}")
            
        return shots
    
    def _get_column_value(self, row, columns, possible_names):
        """Get value from row by trying multiple possible column names"""
        for name in possible_names:
            for col in columns:
                if name in col.lower():
                    return row.get(col, '')
        return ''
    
    def _safe_int(self, value):
        """Safely convert value to integer"""
        if pd.isna(value) or value == '' or value == 'nan':
            return None
        try:
            if '+' in str(value):
                return int(str(value).split('+')[0])
            return int(float(str(value)))
        except (ValueError, TypeError):
            return None
    
    def _safe_float(self, value):
        """Safely convert value to float"""
        if pd.isna(value) or value == '' or value == 'nan':
            return None
        try:
            return float(str(value))
        except (ValueError, TypeError):
            return None
    
    def resolve_player_ids(self, shots):
        """Resolve player names to player_ids"""
        if not shots:
            return shots
            
        conn = sqlite3.connect(self.db_path)
        
        for shot in shots:
            player_name = shot['player_name']
            if player_name:
                # Try exact match
                result = conn.execute(
                    'SELECT player_id FROM player WHERE player_name = ? LIMIT 1',
                    (player_name,)
                ).fetchone()
                
                if result:
                    shot['player_id'] = result[0]
                else:
                    # Try partial match
                    result = conn.execute(
                        'SELECT player_id FROM player WHERE player_name LIKE ? LIMIT 1',
                        (f'%{player_name}%',)
                    ).fetchone()
                    
                    if result:
                        shot['player_id'] = result[0]
        
        conn.close()
        return shots
    
    def save_shots_to_db(self, shots):
        """Save shots to database"""
        if not shots:
            return 0
            
        conn = sqlite3.connect(self.db_path)
        
        insert_sql = """
            INSERT INTO match_shot (
                shot_id, match_id, minute, player_name, player_id, squad,
                xg, psxg, outcome, distance, body_part, notes,
                sca1_player_name, sca1_event, sca2_player_name, sca2_event
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
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
    
    def scrape_historical_shots(self, limit=None):
        """Scrape all 2019/2020 shots"""
        matches = self.get_matches_needing_shots()
        
        if limit:
            matches = matches[:limit]
            
        print(f"🎯 Scraping historical shot data for {len(matches)} matches...")
        
        total_shots = 0
        successful_matches = 0
        failed_matches = 0
        
        for i, (match_id, match_date, home_team, away_team) in enumerate(matches):
            print(f"\n📄 [{i+1}/{len(matches)}] {match_date}: {home_team} vs {away_team}")
            
            url = self.construct_fbref_url(match_id, home_team, away_team, match_date)
            print(f"   🔗 {url}")
            
            # Try BeautifulSoup first (faster)
            shots = self.scrape_shots_from_url(url, match_id, use_selenium=False)
            
            # If no shots found, try Selenium
            if not shots:
                print(f"   🔄 No shots with requests, trying Selenium...")
                shots = self.scrape_shots_from_url(url, match_id, use_selenium=True)
            
            if shots:
                shots = self.resolve_player_ids(shots)
                inserted = self.save_shots_to_db(shots)
                total_shots += inserted
                successful_matches += 1
                print(f"   ✅ Extracted {inserted} shots")
            else:
                failed_matches += 1
                print(f"   ⚠️ No shots found")
            
            # Rate limiting
            if i < len(matches) - 1:  # Don't sleep after last match
                sleep_time = random.uniform(3, 7)
                print(f"   😴 Sleeping {sleep_time:.1f}s...")
                time.sleep(sleep_time)
        
        self.close_selenium()
        
        print(f"\n🎉 Historical scraping complete!")
        print(f"   📊 Total shots extracted: {total_shots}")
        print(f"   ✅ Successful matches: {successful_matches}")
        print(f"   ⚠️ Failed matches: {failed_matches}")
        print(f"   📈 Success rate: {successful_matches/len(matches)*100:.1f}%")
        
        return total_shots

def main():
    scraper = Historical2019ShotScraper()
    
    print("🏈 Starting 2019/2020 NWSL shot data extraction...")
    
    # Process all remaining 2019/2020 matches
    scraper.scrape_historical_shots()

if __name__ == "__main__":
    main()