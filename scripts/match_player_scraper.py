#!/usr/bin/env python3
"""
NWSL Match Player Statistics Scraper
Based on techniques from scraping.md - uses BeautifulSoup first, Selenium as backup
"""

import requests
import pandas as pd
import sqlite3
import uuid
import time
from bs4 import BeautifulSoup
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# Optional selenium imports
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service as ChromeService
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    SELENIUM_AVAILABLE = True
    print("✅ Selenium available for dynamic content scraping")
except ImportError:
    SELENIUM_AVAILABLE = False
    print("⚠️ Selenium not available - will only use BeautifulSoup")

class MatchPlayerScraper:
    def __init__(self, db_path='data/processed/nwsldata.db'):
        self.db_path = db_path
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
    def scrape_match_player_stats(self, source, match_id):
        """
        Scrape comprehensive player statistics from FBRef match report
        Uses BeautifulSoup first, falls back to Selenium if needed
        source: can be URL or local file path
        """
        print(f"🔍 Scraping player stats from: {source}")
        
        # Try BeautifulSoup first (faster)
        try:
            player_stats = self._scrape_with_beautifulsoup(source)
            if player_stats:
                print("✅ Successfully scraped with BeautifulSoup")
                return self._save_player_stats(player_stats, match_id)
        except Exception as e:
            print(f"⚠️ BeautifulSoup failed: {e}")
        
        # Fallback to Selenium (if BeautifulSoup didn't work)
        print("⚠️ BeautifulSoup didn't find data, trying Selenium as backup...")
        try:
            player_stats = self._scrape_with_selenium(source)
            if player_stats:
                print("✅ Successfully scraped with Selenium")
                return self._save_player_stats(player_stats, match_id)
        except Exception as e:
            print(f"❌ Selenium also failed: {e}")
            return False
            
    def _scrape_with_beautifulsoup(self, source):
        """Use BeautifulSoup to scrape player statistics tables"""
        if source.startswith('http'):
            # Web URL
            response = requests.get(source, headers=self.headers)
            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}")
            html_content = response.text
        else:
            # Local file
            with open(source, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract team names from page title
        self.match_teams = self._extract_teams_from_title(soup)
        
        # Find all player stats tables - FBRef typically has multiple tables per team
        player_tables = []
        
        # Look for FBRef player statistics tables - they have specific ID patterns
        table_selectors = [
            'table[id*="stats_"][id*="_summary"]',  # Main summary tables like stats_231a532f_summary
            'table[id*="stats_"][id*="_misc"]',     # Miscellaneous stats tables
            'table[id*="stats_"][id*="_passing"]',  # Passing stats tables
            'table.stats_table'                     # Fallback for stats_table class
        ]
        
        for selector in table_selectors:
            tables = soup.select(selector)
            for table in tables:
                table_id = table.get('id', '')
                print(f"  Found table: {table_id}")
                
                # FBRef player stats tables have team IDs in the table ID
                if 'stats_' in table_id and ('_summary' in table_id or '_misc' in table_id):
                    if self._is_player_stats_table(table):
                        player_tables.append(table)
                        print(f"    ✅ Added player stats table: {table_id}")
        
        if not player_tables:
            raise Exception("No player statistics tables found")
            
        return self._parse_player_tables(player_tables)
    
    def _scrape_with_selenium(self, source):
        """Use Selenium for dynamically loaded content"""
        if not SELENIUM_AVAILABLE:
            print("⚠️ Selenium not available")
            return None
            
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        
        # You'll need to update this path to your chromedriver
        # service = ChromeService(executable_path="/path/to/chromedriver")
        # driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # For now, return None to indicate Selenium not configured
        print("⚠️ Selenium not configured - need chromedriver path")
        return None
        
    def _is_player_stats_table(self, table):
        """Check if table contains player statistics data"""
        # Look for common player stats column headers
        headers = []
        header_row = table.find('thead')
        if header_row:
            headers = [th.get_text().strip().lower() for th in header_row.find_all(['th', 'td'])]
        
        # Check for player stats indicators
        player_indicators = ['player', 'min', 'goals', 'assists', 'shots', 'touches', 'tackles']
        return any(indicator in ' '.join(headers) for indicator in player_indicators)
    
    def _extract_teams_from_title(self, soup):
        """Extract team names from the page title"""
        try:
            title_element = soup.find('title')
            if title_element:
                title_text = title_element.get_text()
                if ' vs. ' in title_text:
                    # Extract teams from title like "Portland Thorns FC vs. Washington Spirit Match Report"
                    teams_part = title_text.split(' Match Report')[0]
                    if ' vs. ' in teams_part:
                        teams = teams_part.split(' vs. ')
                        if len(teams) >= 2:
                            home_team = teams[0].strip()
                            away_team = teams[1].strip()
                            print(f"🏠 Extracted teams: {home_team} vs {away_team}")
                            return {'home': home_team, 'away': away_team}
            return None
        except Exception as e:
            print(f"⚠️ Could not extract teams from title: {e}")
            return None
    
    def _extract_team_from_table(self, table):
        """Extract team name from table ID using extracted match teams"""
        try:
            # Use the match teams extracted from title
            if not hasattr(self, 'match_teams') or not self.match_teams:
                return None
                
            table_id = table.get('id', '')
            if 'stats_' in table_id:
                # Extract the team ID part (e.g. df9a10a1 from stats_df9a10a1_summary)
                team_id_part = table_id.replace('stats_', '').split('_')[0]
                
                # Map team IDs to teams - this is heuristic based on table order
                # First unique team ID usually corresponds to home team
                if not hasattr(self, '_team_id_mapping'):
                    self._team_id_mapping = {}
                
                # Store team ID mapping for consistent assignment
                if team_id_part not in self._team_id_mapping:
                    # Assign teams based on order of appearance
                    current_teams = len(self._team_id_mapping)
                    if current_teams == 0:
                        self._team_id_mapping[team_id_part] = self.match_teams['home']
                    elif current_teams == 1:
                        self._team_id_mapping[team_id_part] = self.match_teams['away']
                    else:
                        # For additional team IDs, alternate or use contextual clues
                        self._team_id_mapping[team_id_part] = self.match_teams['home']
                
                return self._team_id_mapping.get(team_id_part)
            
            return None
            
        except Exception as e:
            print(f"⚠️ Could not extract team name from table: {e}")
            return None
    
    def _parse_player_tables(self, tables):
        """Parse player statistics from HTML tables"""
        all_players = []
        
        for i, table in enumerate(tables):
            try:
                print(f"🔄 Parsing table {i+1}/{len(tables)}...")
                
                # Extract team name from table structure
                team_name = self._extract_team_from_table(table)
                
                # Convert table to pandas DataFrame
                df = pd.read_html(str(table))[0]
                print(f"  DataFrame shape: {df.shape}")
                print(f"  Team: {team_name}")
                print(f"  Columns: {list(df.columns)[:10]}...")  # Show first 10 columns
                
                # Clean up multi-level column headers if present
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = ['_'.join(col).strip() for col in df.columns.values]
                    print(f"  Cleaned columns: {list(df.columns)[:10]}...")
                
                # Parse each row as a player
                players_added = 0
                for idx, row in df.iterrows():
                    player_data = self._extract_player_data(row)
                    if player_data:
                        # Add team name to player data
                        player_data['team_name'] = team_name
                        all_players.append(player_data)
                        players_added += 1
                        
                print(f"  ✅ Added {players_added} players from table {i+1}")
                        
            except Exception as e:
                print(f"⚠️ Failed to parse table {i+1}: {e}")
                import traceback
                traceback.print_exc()
                continue
                
        print(f"🎯 Total players extracted: {len(all_players)}")
        return all_players
    
    def _extract_player_data(self, row):
        """Extract player statistics from table row"""
        try:
            # Map FBRef column names to our database fields (based on actual pandas structure)
            column_mapping = {
                # Primary columns (after multi-level header cleanup)
                'Unnamed: 0_level_0_Player': 'player_name',
                'Unnamed: 1_level_0_#': 'jersey_number',
                'Unnamed: 2_level_0_Nation': 'nation', 
                'Unnamed: 3_level_0_Pos': 'position',
                'Unnamed: 4_level_0_Age': 'age',
                'Unnamed: 5_level_0_Min': 'minutes_played',
                
                # Performance stats
                'Performance_Gls': 'goals',
                'Performance_Ast': 'assists',
                'Performance_PK': 'penalties_made',
                'Performance_PKatt': 'penalties_attempted',
                'Performance_Sh': 'shots',
                'Performance_SoT': 'shots_on_target',
                'Performance_CrdY': 'yellow_cards',
                'Performance_CrdR': 'red_cards',
                'Performance_Touches': 'touches',
                'Performance_Tkl': 'tackles',
                'Performance_Int': 'interceptions',
                'Performance_Blocks': 'blocks',
                
                # Expected stats
                'Expected_xG': 'xg',
                'Expected_npxG': 'npxg',
                'Expected_xAG': 'xag',
                
                # Shot creation
                'SCA_SCA': 'sca',
                'SCA_GCA': 'gca',
                
                # Passing stats
                'Passes_Cmp': 'passes_completed',
                'Passes_Att': 'passes_attempted',
                'Passes_Cmp%': 'pass_completion_pct',
                'Passes_PrgP': 'progressive_passes',
                
                # Carries and take-ons
                'Carries_Carries': 'carries',
                'Carries_PrgC': 'progressive_carries',
                'Take-Ons_Att': 'take_ons_attempted',
                'Take-Ons_Succ': 'take_ons_successful'
            }
            
            player_data = {}
            
            # Extract data based on column mapping
            for fbref_col, db_field in column_mapping.items():
                if fbref_col in row.index:
                    value = row[fbref_col]
                    
                    # Handle different data types
                    if db_field == 'player_name':
                        # Clean up player name (remove extra spaces, links)
                        player_name = str(value).strip() if pd.notna(value) else None
                        if player_name and player_name not in ['nan', 'None', '']:
                            player_data[db_field] = player_name
                    elif db_field in ['jersey_number', 'minutes_played', 'goals', 'assists', 'shots', 'tackles', 'yellow_cards', 'red_cards']:
                        # Integer fields
                        try:
                            player_data[db_field] = int(float(value)) if pd.notna(value) and str(value) != '' else 0
                        except (ValueError, TypeError):
                            player_data[db_field] = 0
                    elif db_field in ['xg', 'npxg', 'xag', 'pass_completion_pct']:  
                        # Float fields
                        try:
                            player_data[db_field] = float(value) if pd.notna(value) and str(value) != '' else 0.0
                        except (ValueError, TypeError):
                            player_data[db_field] = 0.0
                    else:
                        # String fields
                        player_data[db_field] = str(value).strip() if pd.notna(value) and str(value) not in ['nan', 'None', ''] else None
            
            # Only return player data if we have a valid player name
            if player_data.get('player_name'):
                print(f"    Found player: {player_data['player_name']}")
                return player_data
            else:
                return None
            
        except Exception as e:
            print(f"⚠️ Failed to extract player data: {e}")
            return None
    
    def _save_player_stats(self, player_stats, match_id):
        """Save player statistics to database"""
        if not player_stats:
            print("⚠️ No player stats to save")
            return False
            
        conn = sqlite3.connect(self.db_path)
        
        print(f"💾 Saving {len(player_stats)} player records to database...")
        
        saved_count = 0
        for player_data in player_stats:
            try:
                # Skip invalid player names (like "16 Players" summary rows)
                player_name = player_data.get('player_name', '').strip()
                if not player_name or 'player' in player_name.lower() or len(player_name.split()) > 4:
                    continue
                    
                # Generate unique match player ID
                mp_id = f"mp_{str(uuid.uuid4())[:8]}"
                
                # Get player_id from database if they exist
                player_id = self._get_player_id(conn, player_name)
                
                # Determine team name from context - now includes team_name in player_data
                team_name = player_data.get('team_name') or self._determine_team_name(player_name, match_id)
                team_id = self._get_team_id(conn, team_name) if team_name else None
                
                # Insert player match record (with only the columns that exist in our schema)
                conn.execute('''
                    INSERT OR REPLACE INTO match_player (
                        id, match_id, player_id, player_name, team_id, team_name, jersey_number,
                        position, age, minutes_played, goals, assists, 
                        penalties_made, penalties_attempted, shots, shots_on_target,
                        yellow_cards, red_cards, touches, tackles,
                        interceptions, blocks, xg, npxg, xag, sca, gca,
                        passes_completed, passes_attempted, pass_completion_pct,
                        progressive_passes, carries, progressive_carries,
                        take_ons_attempted, take_ons_successful
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    mp_id, match_id, player_id, player_name, team_id, team_name,
                    player_data.get('jersey_number', 0), player_data.get('position'),
                    player_data.get('age'), player_data.get('minutes_played', 0),
                    player_data.get('goals', 0), player_data.get('assists', 0),
                    player_data.get('penalties_made', 0), player_data.get('penalties_attempted', 0),
                    player_data.get('shots', 0), player_data.get('shots_on_target', 0),
                    player_data.get('yellow_cards', 0), player_data.get('red_cards', 0),
                    player_data.get('touches', 0), player_data.get('tackles', 0),
                    player_data.get('interceptions', 0), player_data.get('blocks', 0), 
                    player_data.get('xg', 0.0), player_data.get('npxg', 0.0), player_data.get('xag', 0.0),
                    player_data.get('sca', 0), player_data.get('gca', 0),
                    player_data.get('passes_completed', 0), player_data.get('passes_attempted', 0),
                    player_data.get('pass_completion_pct', 0.0), player_data.get('progressive_passes', 0),
                    player_data.get('carries', 0), player_data.get('progressive_carries', 0),
                    player_data.get('take_ons_attempted', 0), player_data.get('take_ons_successful', 0)
                ))
                saved_count += 1
                
            except Exception as e:
                print(f"⚠️ Failed to save player {player_data.get('player_name', 'Unknown')}: {e}")
                
        conn.commit()
        conn.execute('PRAGMA wal_checkpoint')
        conn.close()
        
        print(f"✅ Successfully saved {saved_count} valid player records to database")
        return True
    
    def _determine_team_name(self, player_name, match_id):
        """Determine team name based on player name and match context"""
        # For match 0716dcc9 (Bay FC vs Orlando Pride), use known player mappings
        if match_id == '0716dcc9':
            bay_fc_players = [
                'Asisat Oshoala', 'Penelope Hocking', 'Racheal Kundananji', 'Karlie Lema',
                'Rachel Hill', 'Taylor Huff', 'Dorian Bailey', 'Hannah Bebar', 'Kiki Pickett',
                'Caroline Conti', 'Alyssa Malonson', 'Kelli Hubly', 'Abby Dahlkemper',
                'Caprice Dydasco', 'Emmie Allen'
            ]
            
            orlando_pride_players = [
                'Barbra Banda', 'Prisca Chilufya', 'Marta', 'Julie Doyle', 'Summer Yates',
                'Kerry Abello', 'Haley Hanson', 'Angelina', 'Ally Watt', 'Ally Lemos',
                'Carson Pickett', 'Oihane Hernández', 'Kylie Strom', 'Emily Sams',
                'Cori Dyke', 'Anna Moorhouse'
            ]
            
            if player_name in bay_fc_players:
                return 'Bay FC'
            elif player_name in orlando_pride_players:
                return 'Orlando Pride'
        
        return None  # Could not determine team
    
    def _get_team_id(self, conn, team_name):
        """Get team_id from database"""
        if not team_name:
            return None
            
        result = conn.execute('SELECT team_id FROM teams WHERE team_name = ?', (team_name,)).fetchone()
        return result[0] if result else None
    
    def _get_player_id(self, conn, player_name):
        """Get player_id from database, create if doesn't exist"""
        if not player_name:
            return None
            
        result = conn.execute('SELECT player_id FROM player WHERE player_name = ?', (player_name,)).fetchone()
        
        if result:
            return result[0]
        else:
            # Create new player record
            player_id = str(uuid.uuid4())[:8]
            conn.execute('INSERT INTO player (player_id, player_name) VALUES (?, ?)', (player_id, player_name))
            return player_id

# Example usage
if __name__ == "__main__":
    scraper = MatchPlayerScraper()
    
    # Example: scrape Bay FC vs Orlando Pride match
    # match_url = "https://fbref.com/en/matches/[match_id]/[match_details]" 
    # scraper.scrape_match_player_stats(match_url, "0716dcc9")
    
    print("🔧 Match Player Scraper created successfully!")
    print("📋 Usage: scraper.scrape_match_player_stats(match_url, match_id)")