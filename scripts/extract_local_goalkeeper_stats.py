#!/usr/bin/env python3
"""
Extract comprehensive goalkeeper match statistics from local CSV files.
Reads from pre-extracted tables in notebooks/match_html_files/tables/ instead of scraping FBRef.
"""

import sqlite3
import pandas as pd
import os
import hashlib
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_goalkeeper_id(match_id: str, player_name: str, team_id: str) -> str:
    """Generate unique match_goalkeeper_id."""
    content = f"{match_id}_{player_name}_{team_id}"
    hex_hash = hashlib.md5(content.encode()).hexdigest()[:8]
    return f"gk_{hex_hash}"

def safe_float(value):
    """Safely convert value to float, return None if conversion fails."""
    if pd.isna(value) or value == '' or value == '—':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def safe_int(value):
    """Safely convert value to int, return None if conversion fails."""
    if pd.isna(value) or value == '' or value == '—':
        return None
    try:
        return int(float(value))  # Convert to float first to handle "1.0" -> 1
    except (ValueError, TypeError):
        return None

def extract_goalkeeper_stats_from_local_csvs(match_id, tables_dir="/Users/thomasmcmillan/projects/nwsl_data/notebooks/match_html_files/tables"):
    """Extract comprehensive goalkeeper statistics from local CSV files."""
    
    match_dir = Path(tables_dir) / match_id
    if not match_dir.exists():
        logging.warning(f"No local data found for match {match_id} at {match_dir}")
        return []
    
    logging.info(f"Reading local goalkeeper CSV files for match {match_id}")
    
    # Find all goalkeeper stat files for this match
    keeper_files = list(match_dir.glob(f"{match_id}_keeper_stats_*.csv"))
    
    if not keeper_files:
        logging.info(f"No goalkeeper files found for match {match_id}")
        return []
    
    goalkeepers = []
    
    for file_path in keeper_files:
        filename = file_path.name
        # Parse filename: {match_id}_keeper_stats_{team_id}.csv
        parts = filename.replace('.csv', '').split('_')
        if len(parts) < 4:
            continue
            
        team_id = parts[3]
        
        logging.info(f"Processing goalkeeper stats for team {team_id}")
        
        try:
            # Read CSV with multi-level headers
            df = pd.read_csv(file_path, header=[0, 1])
            
            # Skip if no data
            if len(df) < 1:
                continue
                
            # With multi-level headers, the actual data is in the dataframe rows
            # The column names are already processed by pandas
            data_rows = df
            
            for _, row in data_rows.iterrows():
                # Extract player name from first column
                player_name = str(row.iloc[0]).strip()
                if not player_name or player_name == 'nan' or player_name == '' or pd.isna(row.iloc[0]):
                    continue
                
                # Initialize goalkeeper data
                gk_data = {
                    'match_goalkeeper_id': generate_goalkeeper_id(match_id, player_name, team_id),
                    'match_id': match_id,
                    'player_id': None,  # Will be resolved later
                    'player_name': player_name,
                    'team_id': team_id,
                    'team_name': None,  # Will be resolved later
                    'season_id': None   # Will be resolved later
                }
                
                # Map each column to the appropriate field using column names
                for i, (col_name, value) in enumerate(zip(df.columns, row.values)):
                    # For multi-level headers, col_name is a tuple (level0, level1)
                    if isinstance(col_name, tuple):
                        header = str(col_name[1]).strip()  # Use level 1 (actual field name)
                        category = str(col_name[0]).strip()  # Use level 0 (category)
                    else:
                        header = str(col_name).strip()
                        category = ''
                    
                    # Basic info
                    if header == 'Nation':
                        gk_data['nation'] = str(value) if not pd.isna(value) else None
                    elif header == 'Age':
                        gk_data['age'] = str(value) if not pd.isna(value) else None
                    elif header == 'Min':
                        gk_data['minutes_played'] = safe_int(value)
                    
                    # Shot Stopping
                    elif header == 'SoTA':
                        gk_data['shots_on_target_against'] = safe_int(value)
                    elif header == 'GA':
                        gk_data['goals_against'] = safe_int(value)
                    elif header == 'Saves':
                        gk_data['saves'] = safe_int(value)
                    elif header == 'Save%':
                        gk_data['save_percentage'] = safe_float(value)
                    elif header == 'PSxG':
                        gk_data['post_shot_xg'] = safe_float(value)
                    
                    # Launched passes (under "Launched" category)
                    elif header == 'Cmp' and category == 'Launched':
                        gk_data['launched_cmp'] = safe_int(value)
                    elif header == 'Att' and category == 'Launched':
                        gk_data['launched_att'] = safe_int(value)
                    elif header == 'Cmp%' and category == 'Launched':
                        gk_data['launched_cmp_pct'] = safe_float(value)
                    
                    # Regular passes (under "Passes" category)
                    elif header == 'Att (GK)':
                        gk_data['passes_att'] = safe_int(value)
                    elif header == 'Thr':
                        gk_data['passes_thr'] = safe_int(value)
                    elif header == 'Launch%' and category == 'Passes':
                        gk_data['passes_launch_pct'] = safe_float(value)
                    elif header == 'AvgLen' and category == 'Passes':
                        gk_data['passes_avg_len'] = safe_float(value)
                    
                    # Goal Kicks (under "Goal Kicks" category)
                    elif header == 'Att' and category == 'Goal Kicks':
                        gk_data['goal_kicks_att'] = safe_int(value)
                    elif header == 'Launch%' and category == 'Goal Kicks':
                        gk_data['goal_kicks_launch_pct'] = safe_float(value)
                    elif header == 'AvgLen' and category == 'Goal Kicks':
                        gk_data['goal_kicks_avg_len'] = safe_float(value)
                    
                    # Crosses (under "Crosses" category)
                    elif header == 'Opp':
                        gk_data['crosses_opp'] = safe_int(value)
                    elif header == 'Stp':
                        gk_data['crosses_stp'] = safe_int(value)
                    elif header == 'Stp%':
                        gk_data['crosses_stp_pct'] = safe_float(value)
                    
                    # Sweeper (under "Sweeper" category)
                    elif header == '#OPA':
                        gk_data['sweeper_opa'] = safe_int(value)
                    elif header == 'AvgDist':
                        gk_data['sweeper_avg_dist'] = safe_float(value)
                
                goalkeepers.append(gk_data)
                logging.info(f"Extracted goalkeeper: {player_name} ({team_id})")
                            
        except Exception as e:
            logging.error(f"Error processing {file_path}: {e}")
            continue
    
    logging.info(f"Extracted stats for {len(goalkeepers)} goalkeepers from local CSV files")
    return goalkeepers

def resolve_goalkeeper_ids_and_details(goalkeepers, db_path):
    """Resolve player_id, team_name, and season_id for each goalkeeper."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    for gk in goalkeepers:
        player_name = gk.get('player_name', '')
        team_id = gk.get('team_id', '')
        match_id = gk.get('match_id', '')
        
        # Resolve player_id
        cursor.execute("""
            SELECT player_id FROM player 
            WHERE LOWER(TRIM(player_name)) = LOWER(TRIM(?))
            LIMIT 1
        """, (player_name,))
        
        result = cursor.fetchone()
        if result:
            gk['player_id'] = result[0]
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
                    gk['player_id'] = result[0]
        
        # Resolve team_name from team_id
        if team_id:
            cursor.execute("""
                SELECT team_name FROM teams 
                WHERE team_id = ?
                LIMIT 1
            """, (team_id,))
            
            result = cursor.fetchone()
            if result:
                gk['team_name'] = result[0]
        
        # Resolve season_id from match_id
        if match_id:
            cursor.execute("""
                SELECT season_id FROM match 
                WHERE match_id = ?
                LIMIT 1
            """, (match_id,))
            
            result = cursor.fetchone()
            if result:
                gk['season_id'] = result[0]
    
    conn.close()

def save_comprehensive_goalkeeper_stats(goalkeepers, db_path):
    """Save comprehensive goalkeeper statistics to the database."""
    if not goalkeepers:
        return
        
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Define all columns for INSERT
    columns = [
        'match_goalkeeper_id', 'match_id', 'player_id', 'player_name', 'team_id', 'team_name',
        'season_id', 'nation', 'age', 'minutes_played',
        # Shot Stopping
        'shots_on_target_against', 'goals_against', 'saves', 'save_percentage', 'post_shot_xg',
        # Launched Passes
        'launched_cmp', 'launched_att', 'launched_cmp_pct',
        # Regular Passes
        'passes_att', 'passes_thr', 'passes_launch_pct', 'passes_avg_len',
        # Goal Kicks
        'goal_kicks_att', 'goal_kicks_launch_pct', 'goal_kicks_avg_len',
        # Crosses
        'crosses_opp', 'crosses_stp', 'crosses_stp_pct',
        # Sweeper
        'sweeper_opa', 'sweeper_avg_dist'
    ]
    
    # Insert goalkeeper stats
    for gk in goalkeepers:
        values = [gk.get(col) for col in columns]
        placeholders = ','.join(['?' for _ in columns])
        
        cursor.execute(f"""
            INSERT OR REPLACE INTO match_goalkeeper ({','.join(columns)}) 
            VALUES ({placeholders})
        """, values)
    
    conn.commit()
    conn.close()
    
    logging.info(f"Saved comprehensive stats for {len(goalkeepers)} goalkeepers to database")

def main():
    """Main function to test local goalkeeper stats extraction."""
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    
    # Test with a match that has local data
    test_match_id = "008e301f"
    
    # Extract comprehensive goalkeeper stats from local CSV files
    goalkeepers = extract_goalkeeper_stats_from_local_csvs(test_match_id)
    
    if goalkeepers:
        # Resolve player IDs, team names, and season IDs
        resolve_goalkeeper_ids_and_details(goalkeepers, db_path)
        
        # Save to database
        save_comprehensive_goalkeeper_stats(goalkeepers, db_path)
        
        print(f"Successfully extracted comprehensive stats for {len(goalkeepers)} goalkeepers from local data")
        
        # Show sample data
        for i, gk in enumerate(goalkeepers):
            print(f"\nGoalkeeper {i+1}: {gk.get('player_name', 'Unknown')} ({gk.get('team_id', 'Unknown')})")
            print(f"  Minutes: {gk.get('minutes_played', 0)}")
            print(f"  Shots Faced: {gk.get('shots_on_target_against', 0)}, Goals Against: {gk.get('goals_against', 0)}")
            print(f"  Saves: {gk.get('saves', 0)}, Save%: {gk.get('save_percentage', 0.0)}")
            print(f"  Passes: {gk.get('passes_att', 0)}, Launched: {gk.get('launched_att', 0)}")
    else:
        print("No comprehensive goalkeeper stats extracted from local data")

if __name__ == "__main__":
    main()