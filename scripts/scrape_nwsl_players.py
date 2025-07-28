#!/usr/bin/env python3
"""
Scrape NWSL player data from FBRef season stats pages
Builds comprehensive player registry with core facts: name, player_id, born, nation
"""

import requests
import pandas as pd
import sqlite3
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import re
from urllib.parse import urljoin

class NWSLPlayerScraper:
    def __init__(self, db_path='data/processed/nwsldata.db'):
        self.db_path = db_path
        self.base_url = "https://fbref.com"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # Setup Selenium (headless)
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
        except Exception as e:
            print(f"⚠️ Selenium setup failed: {e}")
            print("   Will use BeautifulSoup only")
            self.driver = None
        
        # NWSL season URLs
        self.season_urls = [
            "https://fbref.com/en/comps/182/2015/stats/2015-NWSL-Stats",
            "https://fbref.com/en/comps/182/2016/stats/2016-NWSL-Stats", 
            "https://fbref.com/en/comps/182/2017/stats/2017-NWSL-Stats",
            "https://fbref.com/en/comps/182/2018/stats/2018-NWSL-Stats",
            "https://fbref.com/en/comps/182/2019/stats/2019-NWSL-Stats",
            "https://fbref.com/en/comps/182/2020/stats/2020-NWSL-Stats",
            "https://fbref.com/en/comps/182/2021/stats/2021-NWSL-Stats",
            "https://fbref.com/en/comps/182/2022/stats/2022-NWSL-Stats",
            "https://fbref.com/en/comps/182/2023/stats/2023-NWSL-Stats",
            "https://fbref.com/en/comps/182/2024/stats/2024-NWSL-Stats",
            "https://fbref.com/en/comps/182/stats/NWSL-Stats"  # Current season
        ]
        
        self.all_players = {}  # player_id -> player_data
        
    def extract_player_id_from_url(self, player_url):
        """Extract player_id from FBRef player URL"""
        # URL format: /en/players/e64b3c35/Emeri-Adames
        match = re.search(r'/players/([a-f0-9]{8})/', player_url)
        return match.group(1) if match else None
        
    def scrape_players_beautiful_soup(self, url):
        """Try scraping with BeautifulSoup first (faster)"""
        try:
            print(f"🔍 Trying BeautifulSoup for: {url}")
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for player stats table - typically has id="stats_standard"
            table = soup.find('table', {'id': 'stats_standard'})
            if not table:
                # Try other possible table IDs
                table = soup.find('table', class_='stats_table')
                
            if not table:
                print(f"   ❌ No player table found with BeautifulSoup")
                return None
                
            players_found = []
            
            # Extract player data from table rows
            rows = table.find('tbody').find_all('tr') if table.find('tbody') else []
            
            for row in rows:
                # Skip header rows or empty rows
                if 'thead' in row.get('class', []) or not row.find('td'):
                    continue
                    
                cells = row.find_all('td')
                if len(cells) < 4:  # Need at least player, nation, pos, squad
                    continue
                    
                # Find player name cell (usually first with a link)
                player_cell = None
                for cell in cells:
                    link = cell.find('a')
                    if link and '/players/' in link.get('href', ''):
                        player_cell = cell
                        break
                        
                if not player_cell:
                    continue
                    
                # Extract player data
                player_link = player_cell.find('a')
                player_name = player_link.text.strip()
                player_url = urljoin(self.base_url, player_link.get('href'))
                player_id = self.extract_player_id_from_url(player_url)
                
                if not player_id:
                    continue
                    
                # Extract other fields (positions may vary)
                try:
                    nation = cells[1].text.strip() if len(cells) > 1 else ""
                    # Born date might be in different positions - look for year pattern
                    born = ""
                    for cell in cells:
                        text = cell.text.strip()
                        if re.match(r'^\d{4}$', text):  # 4-digit year
                            born = text
                            break
                            
                    players_found.append({
                        'player_id': player_id,
                        'player_name': player_name,
                        'player_url': player_url,
                        'nationality': nation,
                        'born': born
                    })
                    
                except Exception as e:
                    print(f"   ⚠️ Error parsing row: {e}")
                    continue
                    
            print(f"   ✅ BeautifulSoup found {len(players_found)} players")
            return players_found
            
        except Exception as e:
            print(f"   ❌ BeautifulSoup failed: {e}")
            return None
            
    def scrape_players_selenium(self, url):
        """Fallback to Selenium for dynamic content"""
        if not self.driver:
            print(f"   ❌ Selenium not available")
            return []
            
        try:
            print(f"🔍 Trying Selenium for: {url}")
            self.driver.get(url)
            
            # Wait for table to load
            wait = WebDriverWait(self.driver, 10)
            table = wait.until(EC.presence_of_element_located((By.ID, "stats_standard")))
            
            # Get table HTML and parse with BeautifulSoup
            table_html = table.get_attribute('outerHTML')
            soup = BeautifulSoup(table_html, 'html.parser')
            
            # Use same parsing logic as BeautifulSoup method
            players_found = []
            rows = soup.find_all('tr')
            
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) < 4:
                    continue
                    
                # Find player link
                player_cell = None
                for cell in cells:
                    link = cell.find('a')
                    if link and '/players/' in link.get('href', ''):
                        player_cell = cell
                        break
                        
                if not player_cell:
                    continue
                    
                player_link = player_cell.find('a')
                player_name = player_link.text.strip()
                player_url = urljoin(self.base_url, player_link.get('href'))
                player_id = self.extract_player_id_from_url(player_url)
                
                if not player_id:
                    continue
                    
                # Extract nation and born year
                try:
                    nation = cells[1].text.strip() if len(cells) > 1 else ""
                    born = ""
                    for cell in cells:
                        text = cell.text.strip()
                        if re.match(r'^\d{4}$', text):
                            born = text
                            break
                            
                    players_found.append({
                        'player_id': player_id,
                        'player_name': player_name,
                        'player_url': player_url,
                        'nationality': nation,
                        'born': born
                    })
                    
                except Exception as e:
                    continue
                    
            print(f"   ✅ Selenium found {len(players_found)} players")
            return players_found
            
        except Exception as e:
            print(f"   ❌ Selenium failed: {e}")
            return []
            
    def scrape_season_players(self, url):
        """Scrape players from a single season URL"""
        season_match = re.search(r'(\d{4})', url)
        season = season_match.group(1) if season_match else "current"
        
        print(f"\n🏆 Scraping {season} NWSL season...")
        
        # Try BeautifulSoup first, then Selenium
        players = self.scrape_players_beautiful_soup(url)
        if not players:
            players = self.scrape_players_selenium(url)
            
        if not players:
            print(f"   ❌ No players found for {season}")
            return 0
            
        # Add to master player registry (avoid duplicates)
        new_players = 0
        for player in players:
            player_id = player['player_id']
            if player_id not in self.all_players:
                self.all_players[player_id] = player
                new_players += 1
                
        print(f"   ➕ Added {new_players} new players (total: {len(self.all_players)})")
        return new_players
        
    def scrape_all_seasons(self):
        """Scrape players from all NWSL seasons"""
        print("🚀 Starting NWSL player scraping across all seasons...")
        
        total_new = 0
        for url in self.season_urls:
            try:
                new_count = self.scrape_season_players(url)
                total_new += new_count
                
                # Rate limiting
                time.sleep(3)
                
            except Exception as e:
                print(f"❌ Error scraping {url}: {e}")
                continue
                
        print(f"\n🎉 Scraping complete!")
        print(f"   📊 Total unique players found: {len(self.all_players)}")
        print(f"   ➕ New players discovered: {total_new}")
        
        return self.all_players
        
    def save_to_database(self):
        """Save scraped players to database"""
        if not self.all_players:
            print("❌ No players to save")
            return
            
        print(f"\n💾 Saving {len(self.all_players)} players to database...")
        
        conn = sqlite3.connect(self.db_path)
        
        # Insert new players only
        new_count = 0
        updated_count = 0
        
        for player_id, player_data in self.all_players.items():
            # Check if player exists
            existing = conn.execute(
                'SELECT player_id FROM player WHERE player_id = ?', 
                (player_id,)
            ).fetchone()
            
            if existing:
                # Update existing player if we have better data
                conn.execute('''
                    UPDATE player 
                    SET player_name = COALESCE(NULLIF(?, ''), player_name),
                        nationality = COALESCE(NULLIF(?, ''), nationality),
                        dob = COALESCE(NULLIF(?, ''), dob)
                    WHERE player_id = ?
                ''', (
                    player_data['player_name'],
                    player_data['nationality'], 
                    player_data['born'],
                    player_id
                ))
                updated_count += 1
            else:
                # Insert new player
                conn.execute('''
                    INSERT INTO player (player_id, player_name, nationality, dob)
                    VALUES (?, ?, ?, ?)
                ''', (
                    player_id,
                    player_data['player_name'],
                    player_data['nationality'],
                    player_data['born'] if player_data['born'] else None
                ))
                new_count += 1
                
        conn.commit()
        
        # Verify final count
        total_players = conn.execute('SELECT COUNT(*) FROM player').fetchone()[0]
        conn.close()
        
        print(f"   ✅ Inserted {new_count} new players")
        print(f"   🔄 Updated {updated_count} existing players")
        print(f"   📊 Total players in database: {total_players}")
        
    def save_to_csv(self, filename='data/scraped_nwsl_players.csv'):
        """Save scraped players to CSV for backup"""
        if not self.all_players:
            print("❌ No players to save to CSV")
            return
            
        print(f"\n📄 Saving players to CSV: {filename}")
        
        df = pd.DataFrame.from_dict(self.all_players, orient='index')
        df = df.reset_index(drop=True)
        
        # Reorder columns
        column_order = ['player_id', 'player_name', 'nationality', 'born', 'player_url']
        df = df[column_order]
        
        df.to_csv(filename, index=False)
        print(f"   ✅ Saved {len(df)} players to {filename}")
        
    def cleanup(self):
        """Clean up resources"""
        if self.driver:
            self.driver.quit()
            
def main():
    scraper = NWSLPlayerScraper()
    
    try:
        # Scrape all seasons
        players = scraper.scrape_all_seasons()
        
        # Save to database and CSV
        scraper.save_to_database()
        scraper.save_to_csv()
        
    finally:
        scraper.cleanup()
        
if __name__ == "__main__":
    main()