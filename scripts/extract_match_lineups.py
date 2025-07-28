#!/usr/bin/env python3
"""
Extract match lineup information from summary CSV files.
Captures starting XI, bench players, formations, and tactical positions.
"""

import sqlite3
import pandas as pd
import os
import hashlib
import logging
from pathlib import Path
from collections import Counter

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_lineup_id(match_id: str, team_id: str, player_name: str) -> str:
    """Generate unique lineup_id."""
    content = f"{match_id}_{team_id}_{player_name}"
    hex_hash = hashlib.md5(content.encode()).hexdigest()[:8]
    return f"lineup_{hex_hash}"

def safe_int(value):
    """Safely convert value to int, return None if conversion fails."""
    if pd.isna(value) or value == '' or value == '—':
        return None
    try:
        return int(float(value))  # Convert to float first to handle "1.0" -> 1
    except (ValueError, TypeError):
        return None

def infer_formation(positions):
    """Infer formation from player positions."""
    # Count players by position category
    position_counts = {'GK': 0, 'DEF': 0, 'MID': 0, 'FWD': 0}
    
    for pos in positions:
        if pd.isna(pos) or pos == '':
            continue
        pos_str = str(pos).upper()
        
        if 'GK' in pos_str:
            position_counts['GK'] += 1
        elif any(def_pos in pos_str for def_pos in ['CB', 'LB', 'RB', 'WB', 'FB']):
            position_counts['DEF'] += 1
        elif any(mid_pos in pos_str for mid_pos in ['CM', 'DM', 'AM', 'RM', 'LM']):
            position_counts['MID'] += 1
        elif any(fwd_pos in pos_str for fwd_pos in ['FW', 'CF', 'LW', 'RW', 'ST']):
            position_counts['FWD'] += 1
    
    # Build formation string (DEF-MID-FWD, excluding GK)
    if position_counts['DEF'] > 0 and position_counts['MID'] > 0 and position_counts['FWD'] > 0:
        return f"{position_counts['DEF']}-{position_counts['MID']}-{position_counts['FWD']}"
    
    return None

def determine_starter_status(minutes_played, total_team_minutes=990):
    """Determine if player was likely a starter based on minutes played."""
    if minutes_played is None or minutes_played == 0:
        return False
    
    # Generally, starters play 60+ minutes unless substituted early due to tactics/injury
    # We'll use 45+ minutes as threshold to account for various game situations
    return minutes_played >= 45

def extract_lineup_from_match(match_id, tables_dir="/Users/thomasmcmillan/projects/nwsl_data/notebooks/match_html_files/tables"):
    """Extract lineup information from match summary CSV files."""
    
    match_dir = Path(tables_dir) / match_id
    if not match_dir.exists():
        logging.warning(f"No local data found for match {match_id} at {match_dir}")
        return []
    
    # Find all summary stat files for this match
    summary_files = list(match_dir.glob(f"{match_id}_stats_*_summary.csv"))
    
    if not summary_files:
        logging.info(f"No summary files found for match {match_id}")
        return []
    
    logging.info(f"Processing lineup data for match {match_id}")
    
    lineups = []
    
    for file_path in summary_files:
        filename = file_path.name
        # Parse filename: {match_id}_stats_{team_id}_summary.csv
        parts = filename.replace('.csv', '').split('_')
        if len(parts) < 4:
            continue
            
        team_id = parts[2]
        
        logging.info(f"Processing lineup for team {team_id}")
        
        try:
            # Read CSV with multi-level headers
            df = pd.read_csv(file_path, header=[0, 1])
            
            # Skip if no data
            if len(df) < 1:
                continue
            
            # Filter out the team totals row (usually last row)
            player_rows = df[df.iloc[:, 0].str.contains(r'^\d+\s+Players$', na=False) == False]
            if len(player_rows) == 0:
                player_rows = df  # Fallback if no totals row detected
            elif len(player_rows) < len(df):
                player_rows = df.iloc[:-1]  # Remove last row if it looks like totals
            
            team_players = []
            
            for _, row in player_rows.iterrows():
                # Extract player data from row
                player_name = str(row.iloc[0]).strip() if not pd.isna(row.iloc[0]) else None
                jersey_number = safe_int(row.iloc[1])
                position = str(row.iloc[3]).strip() if not pd.isna(row.iloc[3]) else None
                minutes_played = safe_int(row.iloc[5])
                
                # Skip invalid rows
                if not player_name or player_name in ['nan', '']:
                    continue
                
                # Determine if player was a starter
                is_starter = determine_starter_status(minutes_played)
                
                player_data = {
                    'player_name': player_name,
                    'jersey_number': jersey_number,
                    'position': position,
                    'minutes_played': minutes_played,
                    'is_starter': is_starter
                }
                
                team_players.append(player_data)
            
            # Infer formation from starting players
            starter_positions = [p['position'] for p in team_players if p['is_starter']]
            formation = infer_formation(starter_positions)
            
            # Create lineup records for each player
            for player in team_players:
                lineup_id = generate_lineup_id(match_id, team_id, player['player_name'])
                
                lineup_data = {
                    'lineup_id': lineup_id,
                    'match_id': match_id,
                    'team_id': team_id,
                    'player_id': None,  # Will be resolved later
                    'player_name': player['player_name'],
                    'position': player['position'],
                    'jersey_number': player['jersey_number'],
                    'is_starter': player['is_starter'],
                    'formation': formation if player['is_starter'] else None
                }
                
                lineups.append(lineup_data)
                
        except Exception as e:
            logging.error(f"Error processing lineup file {file_path}: {e}")
            continue
    
    logging.info(f"Extracted lineup data for {len(lineups)} players from match {match_id}")
    return lineups

def resolve_player_ids(lineups, db_path):
    """Resolve player_id for each lineup entry."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    for lineup in lineups:
        player_name = lineup.get('player_name', '')
        
        if not player_name:
            continue
            
        # Try exact name match first
        cursor.execute("""
            SELECT player_id FROM player 
            WHERE LOWER(TRIM(player_name)) = LOWER(TRIM(?))
            LIMIT 1
        """, (player_name,))
        
        result = cursor.fetchone()
        if result:
            lineup['player_id'] = result[0]
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
                    lineup['player_id'] = result[0]
    
    conn.close()

def save_lineups_to_database(lineups, db_path):
    """Save lineup data to the database."""
    if not lineups:
        return
        
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Define columns for INSERT
    columns = [
        'lineup_id', 'match_id', 'team_id', 'player_id', 'player_name', 
        'position', 'jersey_number', 'is_starter', 'formation'
    ]
    
    # Insert lineups
    for lineup in lineups:
        values = [lineup.get(col) for col in columns]
        placeholders = ','.join(['?' for _ in columns])
        
        cursor.execute(f"""
            INSERT OR REPLACE INTO match_lineups ({','.join(columns)}) 
            VALUES ({placeholders})
        """, values)
    
    conn.commit()
    conn.close()
    
    logging.info(f"Saved {len(lineups)} lineup entries to database")

def main():
    """Main function to test lineup extraction."""
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    
    # Test with a match that has local data
    test_match_id = "0fbbc32f"  # The match from the screenshot
    
    # Extract lineup data from local CSV files
    lineups = extract_lineup_from_match(test_match_id)
    
    if lineups:
        # Resolve player IDs
        resolve_player_ids(lineups, db_path)
        
        # Save to database
        save_lineups_to_database(lineups, db_path)
        
        print(f"Successfully extracted lineup data for {len(lineups)} players")
        
        # Show sample data
        starters = [l for l in lineups if l['is_starter']]
        bench = [l for l in lineups if not l['is_starter']]
        
        print(f"\nStarting XI: {len(starters)} players")
        for lineup in starters[:5]:
            print(f"  #{lineup.get('jersey_number', 'N/A')} {lineup.get('player_name', 'Unknown')} ({lineup.get('position', 'N/A')})")
        
        print(f"\nBench: {len(bench)} players")
        for lineup in bench[:3]:
            print(f"  #{lineup.get('jersey_number', 'N/A')} {lineup.get('player_name', 'Unknown')} ({lineup.get('position', 'N/A')})")
        
        if starters:
            formation = starters[0].get('formation')
            if formation:
                print(f"\nInferred Formation: {formation}")
    else:
        print("No lineup data extracted")

if __name__ == "__main__":
    main()