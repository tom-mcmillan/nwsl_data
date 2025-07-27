#!/usr/bin/env python3
"""
Extract comprehensive player match statistics from FBRef match reports.
Maps all fields from 6 FBRef tabs to comprehensive match_player table.
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
    """Generate unique match_player_id."""
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
    """Safely convert value to float, return None if conversion fails."""
    if value is None or value == '' or value == '—':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def safe_int(value):
    """Safely convert value to int, return None if conversion fails."""
    if value is None or value == '' or value == '—':
        return None
    try:
        return int(float(value))  # Convert to float first to handle "1.0" -> 1
    except (ValueError, TypeError):
        return None

def extract_player_stats_from_tables(soup, match_id):
    """Extract comprehensive player statistics from all FBRef stat tables."""
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
            
        # Extract column headers (use bottom row for actual column names)
        header_rows = thead.find_all('tr')
        if len(header_rows) >= 2:
            headers = [th.get_text(strip=True) for th in header_rows[1].find_all(['th', 'td'])]
        else:
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
                
            # Extract player name (first column)
            player_name = cells[0].get_text(strip=True)
            if not player_name:
                continue
                
            # Create unique key for this player
            player_key = f"{player_name}_{team_id}"
            
            if player_key not in players_data:
                players_data[player_key] = {
                    'match_player_id': generate_stats_id(match_id, player_name, team_id),
                    'match_id': match_id,
                    'player_id': None,  # Will be resolved later
                }
            
            player_data = players_data[player_key]
            
            # Map each column to the appropriate field based on stat_type
            for i, cell in enumerate(cells):
                if i >= len(headers):
                    break
                    
                header = headers[i]
                value = cell.get_text(strip=True)
                
                # Summary tab mapping
                if stat_type == 'summary':
                    if header == 'Player':
                        player_data['player_name'] = value
                        player_data['team_id'] = team_id
                    elif header == '#':
                        player_data['shirt_number'] = safe_int(value)
                    elif header == 'Pos':
                        player_data['position'] = value
                    elif header == 'Min':
                        player_data['minutes_played'] = safe_int(value)
                    # Performance
                    elif header == 'Gls':
                        player_data['summary_perf_gls'] = safe_int(value)
                    elif header == 'Ast':
                        player_data['summary_perf_ast'] = safe_int(value)
                    elif header == 'PK':
                        player_data['summary_perf_pk'] = safe_int(value)
                    elif header == 'PKatt':
                        player_data['summary_perf_pkatt'] = safe_int(value)
                    elif header == 'Sh':
                        player_data['summary_perf_sh'] = safe_int(value)
                    elif header == 'SoT':
                        player_data['summary_perf_sot'] = safe_int(value)
                    elif header == 'CrdY':
                        player_data['summary_perf_crdy'] = safe_int(value)
                    elif header == 'CrdR':
                        player_data['summary_perf_crdr'] = safe_int(value)
                    elif header == 'Touches':
                        player_data['summary_perf_touches'] = safe_int(value)
                    elif header == 'Tkl':
                        player_data['summary_perf_tkl'] = safe_int(value)
                    elif header == 'Int':
                        player_data['summary_perf_int'] = safe_int(value)
                    elif header == 'Blocks':
                        player_data['summary_perf_blocks'] = safe_int(value)
                    # Expected
                    elif header == 'xG':
                        player_data['summary_exp_xg'] = safe_float(value)
                    elif header == 'npxG':
                        player_data['summary_exp_npxg'] = safe_float(value)
                    elif header == 'xAG':
                        player_data['summary_exp_xag'] = safe_float(value)
                    # SCA/GCA
                    elif header == 'SCA':
                        player_data['summary_sca_sca'] = safe_int(value)
                    elif header == 'GCA':
                        player_data['summary_sca_gca'] = safe_int(value)
                    # Passing snippet from summary
                    elif header == 'Cmp' and 23 <= i <= 25:
                        player_data['summary_pass_cmp'] = safe_int(value)
                    elif header == 'Att' and 24 <= i <= 26:
                        player_data['summary_pass_att'] = safe_int(value)
                    elif header == 'Cmp%' and 25 <= i <= 27:
                        player_data['summary_pass_cmp_pct'] = safe_float(value)
                    elif header == 'PrgP':
                        player_data['summary_pass_prgp'] = safe_int(value)
                    # Carries & Take-ons from summary
                    elif header == 'Carries' and 27 <= i <= 29:
                        player_data['summary_carry_carries'] = safe_int(value)
                    elif header == 'PrgC':
                        player_data['summary_carry_prgc'] = safe_int(value)
                    elif header == 'Att' and 29 <= i <= 31:
                        player_data['summary_take_att'] = safe_int(value)
                    elif header == 'Succ' and 30 <= i <= 32:
                        player_data['summary_take_succ'] = safe_int(value)
                
                # Passing tab mapping
                elif stat_type == 'passing':
                    # Totals
                    if header == 'Cmp' and 6 <= i <= 8:  # Total passes completed
                        player_data['passing_total_cmp'] = safe_int(value)
                    elif header == 'Att' and 7 <= i <= 9:  # Total passes attempted
                        player_data['passing_total_att'] = safe_int(value)
                    elif header == 'Cmp%' and 8 <= i <= 10:  # Total pass accuracy
                        player_data['passing_total_cmp_pct'] = safe_float(value)
                    elif header == 'TotDist':
                        player_data['passing_total_totdist'] = safe_int(value)
                    elif header == 'PrgDist':
                        player_data['passing_total_prgdist'] = safe_int(value)
                    # Short passes
                    elif header == 'Cmp' and 11 <= i <= 13:
                        player_data['passing_short_cmp'] = safe_int(value)
                    elif header == 'Att' and 12 <= i <= 14:
                        player_data['passing_short_att'] = safe_int(value)
                    elif header == 'Cmp%' and 13 <= i <= 15:
                        player_data['passing_short_cmp_pct'] = safe_float(value)
                    # Medium passes
                    elif header == 'Cmp' and 14 <= i <= 16:
                        player_data['passing_medium_cmp'] = safe_int(value)
                    elif header == 'Att' and 15 <= i <= 17:
                        player_data['passing_medium_att'] = safe_int(value)
                    elif header == 'Cmp%' and 16 <= i <= 18:
                        player_data['passing_medium_cmp_pct'] = safe_float(value)
                    # Long passes
                    elif header == 'Cmp' and 17 <= i <= 19:
                        player_data['passing_long_cmp'] = safe_int(value)
                    elif header == 'Att' and 18 <= i <= 20:
                        player_data['passing_long_att'] = safe_int(value)
                    elif header == 'Cmp%' and 19 <= i <= 21:
                        player_data['passing_long_cmp_pct'] = safe_float(value)
                    # Value add
                    elif header == 'Ast' and stat_type == 'passing':
                        player_data['passing_ast'] = safe_int(value)
                    elif header == 'xAG' and stat_type == 'passing':
                        player_data['passing_xag'] = safe_float(value)
                    elif header == 'xA':
                        player_data['passing_xa'] = safe_float(value)
                    elif header == 'KP':
                        player_data['passing_kp'] = safe_int(value)
                    elif header == '1/3':
                        player_data['passing_final_third'] = safe_int(value)
                    elif header == 'PPA':
                        player_data['passing_ppa'] = safe_int(value)
                    elif header == 'CrsPA':
                        player_data['passing_crspa'] = safe_int(value)
                    elif header == 'PrgP' and stat_type == 'passing':
                        player_data['passing_prgp'] = safe_int(value)
                
                # Pass Types tab mapping  
                elif stat_type == 'passing_types':
                    if header == 'Att' and 6 <= i <= 8:
                        player_data['pass_types_att'] = safe_int(value)
                    elif header == 'Live':
                        player_data['pass_types_live'] = safe_int(value)
                    elif header == 'Dead':
                        player_data['pass_types_dead'] = safe_int(value)
                    elif header == 'FK':
                        player_data['pass_types_fk'] = safe_int(value)
                    elif header == 'TB':
                        player_data['pass_types_tb'] = safe_int(value)
                    elif header == 'Sw':
                        player_data['pass_types_sw'] = safe_int(value)
                    elif header == 'Crs':
                        player_data['pass_types_crs'] = safe_int(value)
                    elif header == 'TI':
                        player_data['pass_types_ti'] = safe_int(value)
                    elif header == 'CK':
                        player_data['pass_types_ck'] = safe_int(value)
                    elif header == 'In':
                        player_data['corner_in'] = safe_int(value)
                    elif header == 'Out':
                        player_data['corner_out'] = safe_int(value)
                    elif header == 'Str':
                        player_data['corner_str'] = safe_int(value)
                    elif header == 'Cmp' and stat_type == 'passing_types':
                        player_data['pass_outcome_cmp'] = safe_int(value)
                    elif header == 'Off':
                        player_data['pass_outcome_off'] = safe_int(value)
                    elif header == 'Blocks' and stat_type == 'passing_types':
                        player_data['pass_outcome_blocks'] = safe_int(value)
                
                # Defensive Actions tab mapping
                elif stat_type == 'defense':
                    if header == 'Tkl' and 6 <= i <= 8:
                        player_data['def_tkl'] = safe_int(value)
                    elif header == 'TklW':
                        player_data['def_tklw'] = safe_int(value)
                    elif header == 'Def 3rd':
                        player_data['def_tkl_def_3rd'] = safe_int(value)
                    elif header == 'Mid 3rd':
                        player_data['def_tkl_mid_3rd'] = safe_int(value)
                    elif header == 'Att 3rd':
                        player_data['def_tkl_att_3rd'] = safe_int(value)
                    elif header == 'Tkl' and 13 <= i <= 15:  # Challenges section
                        player_data['def_chal_tkl'] = safe_int(value)
                    elif header == 'Att' and 14 <= i <= 16:  # Challenge attempts
                        player_data['def_chal_att'] = safe_int(value)
                    elif header == 'Tkl%':
                        player_data['def_chal_tkl_pct'] = safe_float(value)
                    elif header == 'Lost':
                        player_data['def_chal_lost'] = safe_int(value)
                    elif header == 'Blocks' and stat_type == 'defense':
                        player_data['def_blocks_total'] = safe_int(value)
                    elif header == 'Sh':
                        player_data['def_blocks_sh'] = safe_int(value)
                    elif header == 'Pass':
                        player_data['def_blocks_pass'] = safe_int(value)
                    elif header == 'Int' and stat_type == 'defense':
                        player_data['def_int'] = safe_int(value)
                    elif header == 'Tkl+Int':
                        player_data['def_tkl_int'] = safe_int(value)
                    elif header == 'Clr':
                        player_data['def_clr'] = safe_int(value)
                    elif header == 'Err':
                        player_data['def_err'] = safe_int(value)
                
                # Possession tab mapping
                elif stat_type == 'possession':
                    if header == 'Touches' and 6 <= i <= 8:
                        player_data['poss_touches'] = safe_int(value)
                    elif header == 'Def Pen':
                        player_data['poss_touches_def_pen'] = safe_int(value)
                    elif header == 'Def 3rd' and stat_type == 'possession':
                        player_data['poss_touches_def_3rd'] = safe_int(value)
                    elif header == 'Mid 3rd' and stat_type == 'possession':
                        player_data['poss_touches_mid_3rd'] = safe_int(value)
                    elif header == 'Att 3rd' and stat_type == 'possession':
                        player_data['poss_touches_att_3rd'] = safe_int(value)
                    elif header == 'Att Pen':
                        player_data['poss_touches_att_pen'] = safe_int(value)
                    elif header == 'Live' and stat_type == 'possession':
                        player_data['poss_touches_live'] = safe_int(value)
                    elif header == 'Att' and 13 <= i <= 15:  # Take-on attempts
                        player_data['poss_take_att'] = safe_int(value)
                    elif header == 'Succ' and 14 <= i <= 16:  # Take-on successes
                        player_data['poss_take_succ'] = safe_int(value)
                    elif header == 'Succ%':
                        player_data['poss_take_succ_pct'] = safe_float(value)
                    elif header == 'Tkld':
                        player_data['poss_take_tkld'] = safe_int(value)
                    elif header == 'Tkld%':
                        player_data['poss_take_tkld_pct'] = safe_float(value)
                    elif header == 'Carries' and stat_type == 'possession':
                        player_data['poss_carry_carries'] = safe_int(value)
                    elif header == 'TotDist' and stat_type == 'possession':
                        player_data['poss_carry_totdist'] = safe_int(value)
                    elif header == 'PrgDist' and stat_type == 'possession':
                        player_data['poss_carry_prgdist'] = safe_int(value)
                    elif header == 'PrgC' and stat_type == 'possession':
                        player_data['poss_carry_prgc'] = safe_int(value)
                    elif header == '1/3' and stat_type == 'possession':
                        player_data['poss_carry_final_third'] = safe_int(value)
                    elif header == 'CPA':
                        player_data['poss_carry_cpa'] = safe_int(value)
                    elif header == 'Mis':
                        player_data['poss_carry_mis'] = safe_int(value)
                    elif header == 'Dis':
                        player_data['poss_carry_dis'] = safe_int(value)
                    elif header == 'Rec':
                        player_data['poss_rec_rec'] = safe_int(value)
                    elif header == 'PrgR':
                        player_data['poss_rec_prgr'] = safe_int(value)
                
                # Miscellaneous Stats tab mapping
                elif stat_type == 'misc':
                    if header == 'CrdY' and stat_type == 'misc':
                        player_data['misc_crdy'] = safe_int(value)
                    elif header == 'CrdR' and stat_type == 'misc':
                        player_data['misc_crdr'] = safe_int(value)
                    elif header == '2CrdY':
                        player_data['misc_2crdy'] = safe_int(value)
                    elif header == 'Fls':
                        player_data['misc_fls'] = safe_int(value)
                    elif header == 'Fld':
                        player_data['misc_fld'] = safe_int(value)
                    elif header == 'Off' and stat_type == 'misc':
                        player_data['misc_off'] = safe_int(value)
                    elif header == 'Crs' and stat_type == 'misc':
                        player_data['misc_crs'] = safe_int(value)
                    elif header == 'Int' and stat_type == 'misc':
                        player_data['misc_int'] = safe_int(value)
                    elif header == 'TklW' and stat_type == 'misc':
                        player_data['misc_tklw'] = safe_int(value)
                    elif header == 'PKwon':
                        player_data['misc_pkwon'] = safe_int(value)
                    elif header == 'PKcon':
                        player_data['misc_pkcon'] = safe_int(value)
                    elif header == 'OG':
                        player_data['misc_og'] = safe_int(value)
                    elif header == 'Recov':
                        player_data['misc_recov'] = safe_int(value)
                    elif header == 'Won':
                        player_data['aerial_won'] = safe_int(value)
                    elif header == 'Lost' and stat_type == 'misc':
                        player_data['aerial_lost'] = safe_int(value)
                    elif header == 'Won%':
                        player_data['aerial_won_pct'] = safe_float(value)
    
    return list(players_data.values())

def scrape_player_stats(url, match_id):
    """Scrape comprehensive player statistics from FBRef match page."""
    driver = setup_driver()
    
    try:
        logging.info(f"Scraping comprehensive player stats from: {url}")
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
        
        logging.info(f"Extracted comprehensive stats for {len(players)} players")
        return players
        
    except Exception as e:
        logging.error(f"Error scraping {url}: {e}")
        return []
    
    finally:
        driver.quit()

def resolve_player_ids_and_team_names(players, db_path):
    """Resolve player_id and team_name for each player."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    for player in players:
        player_name = player.get('player_name', '')
        team_id = player.get('team_id', '')
        
        # Resolve player_id
        # Try exact name match first
        cursor.execute("""
            SELECT player_id FROM player 
            WHERE LOWER(TRIM(player_name)) = LOWER(TRIM(?))
            LIMIT 1
        """, (player_name,))
        
        result = cursor.fetchone()
        if result:
            player['player_id'] = result[0]
        else:
            # Try fuzzy matching - split names and match parts
            name_parts = player_name.split()
            if len(name_parts) >= 2:
                cursor.execute("""
                    SELECT player_id FROM player 
                    WHERE player_name LIKE ? OR player_name LIKE ?
                    LIMIT 1
                """, (f"%{name_parts[0]}%", f"%{name_parts[-1]}%"))
                
                result = cursor.fetchone()
                if result:
                    player['player_id'] = result[0]
        
        # Resolve team_name from team_id
        if team_id:
            cursor.execute("""
                SELECT team_name FROM teams 
                WHERE team_id = ?
                LIMIT 1
            """, (team_id,))
            
            result = cursor.fetchone()
            if result:
                player['team_name'] = result[0]
    
    conn.close()

def save_comprehensive_player_stats(players, db_path):
    """Save comprehensive player statistics to the database."""
    if not players:
        return
        
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Define all columns for INSERT
    columns = [
        'match_player_id', 'match_id', 'player_id', 'player_name', 'team_id', 'team_name',
        'shirt_number', 'position', 'minutes_played',
        # Summary tab
        'summary_perf_gls', 'summary_perf_ast', 'summary_perf_pk', 'summary_perf_pkatt',
        'summary_perf_sh', 'summary_perf_sot', 'summary_perf_crdy', 'summary_perf_crdr',
        'summary_perf_touches', 'summary_perf_tkl', 'summary_perf_int', 'summary_perf_blocks',
        'summary_exp_xg', 'summary_exp_npxg', 'summary_exp_xag',
        'summary_sca_sca', 'summary_sca_gca',
        'summary_pass_cmp', 'summary_pass_att', 'summary_pass_cmp_pct', 'summary_pass_prgp',
        'summary_carry_carries', 'summary_carry_prgc', 'summary_take_att', 'summary_take_succ',
        # Passing tab
        'passing_total_cmp', 'passing_total_att', 'passing_total_cmp_pct', 
        'passing_total_totdist', 'passing_total_prgdist',
        'passing_short_cmp', 'passing_short_att', 'passing_short_cmp_pct',
        'passing_medium_cmp', 'passing_medium_att', 'passing_medium_cmp_pct',
        'passing_long_cmp', 'passing_long_att', 'passing_long_cmp_pct',
        'passing_ast', 'passing_xag', 'passing_xa', 'passing_kp', 
        'passing_final_third', 'passing_ppa', 'passing_crspa', 'passing_prgp',
        # Pass Types tab
        'pass_types_att', 'pass_types_live', 'pass_types_dead', 'pass_types_fk',
        'pass_types_tb', 'pass_types_sw', 'pass_types_crs', 'pass_types_ti', 'pass_types_ck',
        'corner_in', 'corner_out', 'corner_str',
        'pass_outcome_cmp', 'pass_outcome_off', 'pass_outcome_blocks',
        # Defensive Actions tab
        'def_tkl', 'def_tklw', 'def_tkl_def_3rd', 'def_tkl_mid_3rd', 'def_tkl_att_3rd',
        'def_chal_tkl', 'def_chal_att', 'def_chal_tkl_pct', 'def_chal_lost',
        'def_blocks_total', 'def_blocks_sh', 'def_blocks_pass',
        'def_int', 'def_tkl_int', 'def_clr', 'def_err',
        # Possession tab
        'poss_touches', 'poss_touches_def_pen', 'poss_touches_def_3rd', 'poss_touches_mid_3rd',
        'poss_touches_att_3rd', 'poss_touches_att_pen', 'poss_touches_live',
        'poss_take_att', 'poss_take_succ', 'poss_take_succ_pct', 'poss_take_tkld', 'poss_take_tkld_pct',
        'poss_carry_carries', 'poss_carry_totdist', 'poss_carry_prgdist', 'poss_carry_prgc',
        'poss_carry_final_third', 'poss_carry_cpa', 'poss_carry_mis', 'poss_carry_dis',
        'poss_rec_rec', 'poss_rec_prgr',
        # Miscellaneous Stats tab
        'misc_crdy', 'misc_crdr', 'misc_2crdy', 'misc_fls', 'misc_fld', 'misc_off',
        'misc_crs', 'misc_int', 'misc_tklw', 'misc_pkwon', 'misc_pkcon', 'misc_og', 'misc_recov',
        'aerial_won', 'aerial_lost', 'aerial_won_pct'
    ]
    
    # Insert player stats
    for player in players:
        values = [player.get(col) for col in columns]
        placeholders = ','.join(['?' for _ in columns])
        
        cursor.execute(f"""
            INSERT OR REPLACE INTO match_player ({','.join(columns)}) 
            VALUES ({placeholders})
        """, values)
    
    conn.commit()
    conn.close()
    
    logging.info(f"Saved comprehensive stats for {len(players)} players to database")

def main():
    """Main function to test comprehensive player stats extraction."""
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    
    # Test URL from user's example
    test_url = "https://fbref.com/en/matches/414d2972/Portland-Thorns-FC-Gotham-FC-April-22-2025-NWSL"
    test_match_id = "414d2972"
    
    # Extract comprehensive player stats
    players = scrape_player_stats(test_url, test_match_id)
    
    if players:
        # Resolve player IDs and team names
        resolve_player_ids_and_team_names(players, db_path)
        
        # Save to database
        save_comprehensive_player_stats(players, db_path)
        
        print(f"Successfully extracted comprehensive stats for {len(players)} players")
        
        # Show sample data
        for i, player in enumerate(players[:2]):
            print(f"\nPlayer {i+1}: {player.get('player_name', 'Unknown')} ({player.get('team_id', 'Unknown')})")
            print(f"  Position: {player.get('position', 'N/A')}, Minutes: {player.get('minutes_played', 0)}")
            print(f"  Goals: {player.get('summary_perf_gls', 0)}, Assists: {player.get('summary_perf_ast', 0)}")
            print(f"  Shots: {player.get('summary_perf_sh', 0)}, SoT: {player.get('summary_perf_sot', 0)}")
            print(f"  xG: {player.get('summary_exp_xg', 0.0)}, Passes: {player.get('passing_total_cmp', 0)}/{player.get('passing_total_att', 0)}")
    else:
        print("No comprehensive player stats extracted")

if __name__ == "__main__":
    main()