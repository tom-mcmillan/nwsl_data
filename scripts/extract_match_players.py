#!/usr/bin/env python3
"""
Extract player match statistics from FBRef match reports.
Based on the comprehensive FBRef player stats structure with multiple tab categories.
"""

import sqlite3
import requests
from bs4 import BeautifulSoup
import time
import hashlib
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_stats_id(match_id: str, player_name: str, team_id: str) -> str:
    """Generate unique stats_id for player match stats."""
    content = f"{match_id}_{player_name}_{team_id}"
    hex_hash = hashlib.md5(content.encode()).hexdigest()[:8]
    return f"mp_{hex_hash}"

def setup_driver():
    """Set up Chrome driver with options for scraping."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
    
    return webdriver.Chrome(options=chrome_options)

def safe_float(value):
    """Safely convert value to float, return 0.0 if conversion fails."""
    if value is None or value == '' or value == '—':
        return 0.0
    try:
        return float(value)
    except (ValueError, TypeError):
        return 0.0

def safe_int(value):
    """Safely convert value to int, return 0 if conversion fails."""
    if value is None or value == '' or value == '—':
        return 0
    try:
        return int(float(value))  # Convert to float first to handle "1.0" -> 1
    except (ValueError, TypeError):
        return 0

def extract_player_stats_from_tables(soup, match_id):
    """Extract player statistics from all FBRef stat tables."""
    players_data = {}
    
    # Find all stat tables - they have IDs like 'stats_<team_id>_summary', 'stats_<team_id>_passing', etc.
    stat_tables = soup.find_all('table', {'id': re.compile(r'stats_[a-f0-9-]+_')})
    
    for table in stat_tables:
        table_id = table.get('id', '')
        logging.info(f"Processing table: {table_id}")
        
        # Extract team_id and stat_type from table ID
        match_team = re.search(r'stats_([a-f0-9-]+)_(\w+)', table_id)
        if not match_team:
            continue
            
        team_id = match_team.group(1)
        stat_type = match_team.group(2)
        
        # Get table headers
        thead = table.find('thead')
        if not thead:
            continue
            
        # Extract column headers (handling multi-level headers)
        header_rows = thead.find_all('tr')
        headers = []
        
        if len(header_rows) >= 2:
            # Use bottom row headers directly - they're the actual column names
            headers = [th.get_text(strip=True) for th in header_rows[1].find_all(['th', 'td'])]
        else:
            # Single level headers
            headers = [th.get_text(strip=True) for th in header_rows[0].find_all(['th', 'td'])]
        
        # Process player rows
        tbody = table.find('tbody')
        if not tbody:
            continue
            
        rows = tbody.find_all('tr')
        for row in rows:
            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                continue
                
            # Extract player name (usually first or second column)
            player_name = None
            for cell in cells[:3]:  # Check first 3 cells for player name
                text = cell.get_text(strip=True)
                if text and not text.isdigit() and text not in ['#', 'Pos', 'Nation']:
                    player_name = text
                    break
                    
            if not player_name:
                continue
                
            # Create unique key for this player
            player_key = f"{player_name}_{team_id}"
            
            if player_key not in players_data:
                players_data[player_key] = {
                    'stats_id': generate_stats_id(match_id, player_name, team_id),
                    'match_id': match_id,
                    'player_name': player_name,
                    'team_id': team_id,
                    'player_id': None,  # Will be resolved later
                }
            
            # Extract data based on stat_type and headers
            player_data = players_data[player_key]
            
            # Map columns to our database fields based on stat_type and exact header names
            for i, cell in enumerate(cells):
                if i >= len(headers):
                    break
                    
                header = headers[i]
                value = cell.get_text(strip=True)
                
                # Map FBRef columns to our database fields using exact header names
                if stat_type == 'summary':
                    if header == 'Min':
                        player_data['minutes_played'] = safe_int(value)
                    elif header == 'Gls':
                        player_data['goals'] = safe_int(value)
                    elif header == 'Ast':
                        player_data['assists'] = safe_int(value)
                    elif header == 'Sh':
                        player_data['shots'] = safe_int(value)
                    elif header == 'SoT':
                        player_data['shots_on_target'] = safe_int(value)
                    elif header == 'xG':
                        player_data['xg'] = safe_float(value)
                    elif header == 'npxG':
                        player_data['npxg'] = safe_float(value)
                    elif header == 'xAG':
                        player_data['xag'] = safe_float(value)
                    elif header == 'SCA':
                        player_data['shot_creating_actions'] = safe_int(value)
                    elif header == 'GCA':
                        player_data['goal_creating_actions'] = safe_int(value)
                    elif header == 'CrdY':
                        player_data['yellow_cards'] = safe_int(value)
                    elif header == 'CrdR':
                        player_data['red_cards'] = safe_int(value)
                    elif header == 'Touches':
                        player_data['touches'] = safe_int(value)
                    elif header == 'Tkl':
                        player_data['tackles'] = safe_int(value)
                    elif header == 'Int':
                        player_data['interceptions'] = safe_int(value)
                    elif header == 'Blocks':
                        player_data['blocks'] = safe_int(value)
                    elif header == 'Cmp' and stat_type == 'summary':  # Passes completed from summary
                        player_data['passes_completed'] = safe_int(value)
                    elif header == 'Att' and stat_type == 'summary':  # Passes attempted from summary
                        player_data['passes_attempted'] = safe_int(value)
                    elif header == 'Cmp%':
                        player_data['pass_accuracy'] = safe_float(value)
                    elif header == 'PrgP':
                        player_data['progressive_passes'] = safe_int(value)
                    elif header == 'Carries' and stat_type == 'summary':
                        player_data['carries'] = safe_int(value)
                    elif header == 'PrgC':
                        player_data['progressive_carries'] = safe_int(value)
                    elif header == 'Att' and i == 29:  # Take-on attempts (position 29 in summary)
                        player_data['dribbles_attempted'] = safe_int(value)
                    elif header == 'Succ':  # Take-on successes
                        player_data['dribbles_completed'] = safe_int(value)
                        
                elif stat_type == 'passing':
                    if header == 'Cmp' and i <= 3:  # Total passes completed (early in table)
                        player_data['passes_completed'] = safe_int(value)
                    elif header == 'Att' and i <= 4:  # Total passes attempted (early in table)
                        player_data['passes_attempted'] = safe_int(value)
                    elif header == 'Cmp%' and i <= 5:  # Total pass accuracy
                        player_data['pass_accuracy'] = safe_float(value)
                    elif header == 'PrgP':
                        player_data['progressive_passes'] = safe_int(value)
                    elif header == 'Cmp' and 6 <= i <= 10:  # Short passes
                        player_data['short_passes_completed'] = safe_int(value)
                    elif header == 'Att' and 7 <= i <= 11:  # Short passes attempted
                        player_data['short_passes_attempted'] = safe_int(value)
                    elif header == 'Cmp' and 11 <= i <= 15:  # Medium passes
                        player_data['medium_passes_completed'] = safe_int(value)
                    elif header == 'Att' and 12 <= i <= 16:  # Medium passes attempted
                        player_data['medium_passes_attempted'] = safe_int(value)
                    elif header == 'Cmp' and 16 <= i <= 20:  # Long passes
                        player_data['long_passes_completed'] = safe_int(value)
                    elif header == 'Att' and 17 <= i <= 21:  # Long passes attempted
                        player_data['long_passes_attempted'] = safe_int(value)
                        
                elif stat_type == 'defense':
                    if header == 'Tkl':
                        player_data['tackles'] = safe_int(value)
                    elif header == 'Int':
                        player_data['interceptions'] = safe_int(value)
                    elif header == 'Blocks':
                        player_data['blocks'] = safe_int(value)
                    elif header == 'Clr':
                        player_data['clearances'] = safe_int(value)
                    elif header == 'Won':  # Aerial duels won
                        player_data['aerial_duels_won'] = safe_int(value)
                    elif header == 'Lost':  # Aerial duels lost
                        player_data['aerial_duels_lost'] = safe_int(value)
                        
                elif stat_type == 'possession':
                    if header == 'Touches':
                        player_data['touches'] = safe_int(value)
                    elif header == 'Carries' and 'Prg' not in headers[i-1:i+2]:
                        player_data['carries'] = safe_int(value)
                    elif header == 'PrgC':
                        player_data['progressive_carries'] = safe_int(value)
                    elif header == 'Att' and 'Take' in str(headers[i-2:i]):
                        player_data['dribbles_attempted'] = safe_int(value)
                    elif header == 'Succ' and 'Take' in str(headers[i-3:i]):
                        player_data['dribbles_completed'] = safe_int(value)
                        
                elif stat_type == 'misc':
                    if header == 'Fls':  # Fouls committed
                        player_data['fouls_committed'] = safe_int(value)
                    elif header == 'Fld':  # Fouls drawn
                        player_data['fouls_drawn'] = safe_int(value)
                    elif header == 'CrdY':
                        player_data['yellow_cards'] = safe_int(value)
                    elif header == 'CrdR':
                        player_data['red_cards'] = safe_int(value)
    
    return list(players_data.values())

def scrape_player_stats(url, match_id):
    """Scrape player statistics from FBRef match page."""
    driver = setup_driver()
    
    try:
        logging.info(f"Scraping player stats from: {url}")
        driver.get(url)
        
        # Wait for content to load
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        
        # Click through tabs to load all player stat tables
        tab_buttons = driver.find_elements(By.CSS_SELECTOR, "div.tablist button")
        for button in tab_buttons:
            try:
                button.click()
                time.sleep(1)  # Allow content to load
            except:
                continue
        
        # Get page source after all tabs are loaded
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Extract player stats
        players = extract_player_stats_from_tables(soup, match_id)
        
        logging.info(f"Extracted stats for {len(players)} players")
        return players
        
    except Exception as e:
        logging.error(f"Error scraping {url}: {e}")
        return []
    
    finally:
        driver.quit()

def resolve_player_ids(players, db_path):
    """Resolve player_id for each player using the player table."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    for player in players:
        # Try exact name match first
        cursor.execute("""
            SELECT player_id FROM player 
            WHERE LOWER(TRIM(player_name)) = LOWER(TRIM(?))
            LIMIT 1
        """, (player['player_name'],))
        
        result = cursor.fetchone()
        if result:
            player['player_id'] = result[0]
        else:
            # Try fuzzy matching - split names and match parts
            name_parts = player['player_name'].split()
            if len(name_parts) >= 2:
                cursor.execute("""
                    SELECT player_id FROM player 
                    WHERE player_name LIKE ? OR player_name LIKE ?
                    LIMIT 1
                """, (f"%{name_parts[0]}%", f"%{name_parts[-1]}%"))
                
                result = cursor.fetchone()
                if result:
                    player['player_id'] = result[0]
    
    conn.close()

def save_player_stats(players, db_path):
    """Save player statistics to the database."""
    if not players:
        return
        
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # First, let's update the table schema to include all the new fields
    additional_columns = [
        "shot_creating_actions INTEGER DEFAULT 0",
        "goal_creating_actions INTEGER DEFAULT 0", 
        "short_passes_completed INTEGER DEFAULT 0",
        "short_passes_attempted INTEGER DEFAULT 0",
        "medium_passes_completed INTEGER DEFAULT 0", 
        "medium_passes_attempted INTEGER DEFAULT 0",
        "long_passes_completed INTEGER DEFAULT 0",
        "long_passes_attempted INTEGER DEFAULT 0",
        "aerial_duels_lost INTEGER DEFAULT 0"
    ]
    
    for column in additional_columns:
        try:
            cursor.execute(f"ALTER TABLE match_player ADD COLUMN {column}")
        except sqlite3.OperationalError:
            # Column already exists
            pass
    
    # Insert player stats
    for player in players:
        cursor.execute("""
            INSERT OR REPLACE INTO match_player (
                stats_id, match_id, player_id, player_name, team_id,
                minutes_played, goals, assists, shots, shots_on_target,
                xg, npxg, xag, passes_completed, passes_attempted, pass_accuracy,
                progressive_passes, tackles, interceptions, blocks, clearances,
                aerial_duels_won, aerial_duels_attempted, touches, carries,
                progressive_carries, dribbles_attempted, dribbles_completed,
                yellow_cards, red_cards, fouls_committed, fouls_drawn,
                shot_creating_actions, goal_creating_actions,
                short_passes_completed, short_passes_attempted,
                medium_passes_completed, medium_passes_attempted,
                long_passes_completed, long_passes_attempted, aerial_duels_lost
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            player['stats_id'], player['match_id'], player['player_id'], 
            player['player_name'], player['team_id'],
            player.get('minutes_played', 0), player.get('goals', 0), 
            player.get('assists', 0), player.get('shots', 0), 
            player.get('shots_on_target', 0), player.get('xg', 0.0), 
            player.get('npxg', 0.0), player.get('xag', 0.0),
            player.get('passes_completed', 0), player.get('passes_attempted', 0),
            player.get('pass_accuracy', 0.0), player.get('progressive_passes', 0),
            player.get('tackles', 0), player.get('interceptions', 0),
            player.get('blocks', 0), player.get('clearances', 0),
            player.get('aerial_duels_won', 0), player.get('aerial_duels_attempted', 0),
            player.get('touches', 0), player.get('carries', 0),
            player.get('progressive_carries', 0), player.get('dribbles_attempted', 0),
            player.get('dribbles_completed', 0), player.get('yellow_cards', 0),
            player.get('red_cards', 0), player.get('fouls_committed', 0),
            player.get('fouls_drawn', 0), player.get('shot_creating_actions', 0),
            player.get('goal_creating_actions', 0), player.get('short_passes_completed', 0),
            player.get('short_passes_attempted', 0), player.get('medium_passes_completed', 0),
            player.get('medium_passes_attempted', 0), player.get('long_passes_completed', 0),
            player.get('long_passes_attempted', 0), player.get('aerial_duels_lost', 0)
        ))
    
    conn.commit()
    conn.close()
    
    logging.info(f"Saved {len(players)} player stats to database")

def main():
    """Main function to test player stats extraction."""
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    
    # Test URL from user's example
    test_url = "https://fbref.com/en/matches/414d2972/Portland-Thorns-FC-Gotham-FC-April-22-2025-NWSL"
    test_match_id = "414d2972"
    
    # Extract player stats
    players = scrape_player_stats(test_url, test_match_id)
    
    if players:
        # Resolve player IDs
        resolve_player_ids(players, db_path)
        
        # Save to database
        save_player_stats(players, db_path)
        
        print(f"Successfully extracted and saved stats for {len(players)} players")
        
        # Show sample data
        for i, player in enumerate(players[:3]):
            print(f"\nPlayer {i+1}: {player['player_name']} ({player['team_id']})")
            print(f"  Minutes: {player.get('minutes_played', 0)}")
            print(f"  Goals: {player.get('goals', 0)}, Assists: {player.get('assists', 0)}")
            print(f"  Shots: {player.get('shots', 0)}, SoT: {player.get('shots_on_target', 0)}")
            print(f"  xG: {player.get('xg', 0.0)}, xA: {player.get('xag', 0.0)}")
    else:
        print("No player stats extracted")

if __name__ == "__main__":
    main()