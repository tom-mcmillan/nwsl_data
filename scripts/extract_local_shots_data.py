#!/usr/bin/env python3
"""
Extract comprehensive shot data from local CSV files.
Reads from pre-extracted tables in notebooks/match_html_files/tables/ for complete 2019-2025 coverage.
"""

import sqlite3
import pandas as pd
import os
import hashlib
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_shot_id(match_id: str, minute: int, player_name: str, sequence: int = 0) -> str:
    """Generate unique shot_id."""
    content = f"{match_id}_{minute}_{player_name}_{sequence}"
    hex_hash = hashlib.md5(content.encode()).hexdigest()[:8]
    return f"shot_{hex_hash}"

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

def map_outcome_to_id(outcome_text):
    """Map FBRef outcome text to database outcome_id."""
    if pd.isna(outcome_text) or outcome_text == '':
        return 'so_unknown'
    
    outcome_text = str(outcome_text).strip().lower()
    
    outcome_mapping = {
        'goal': 'so_goal',
        'saved': 'so_saved',
        'blocked': 'so_blocked',
        'off target': 'so_off_target',
        'woodwork': 'so_woodwork',
        'saved off target': 'so_saved_off_target'
    }
    
    return outcome_mapping.get(outcome_text, 'so_unknown')

def extract_shots_from_local_csv(match_id, tables_dir="/Users/thomasmcmillan/projects/nwsl_data/notebooks/match_html_files/tables"):
    """Extract shot data from local CSV files."""
    
    match_dir = Path(tables_dir) / match_id
    if not match_dir.exists():
        logging.warning(f"No local data found for match {match_id} at {match_dir}")
        return []
    
    # Look for shots_all.csv file
    shots_file = match_dir / f"{match_id}_shots_all.csv"
    if not shots_file.exists():
        logging.info(f"No shots file found for match {match_id}")
        return []
    
    logging.info(f"Reading shots CSV file for match {match_id}")
    
    try:
        # Read CSV with multi-level headers
        df = pd.read_csv(shots_file, header=[0, 1])
        
        # Skip if no data
        if len(df) < 1:
            return []
        
        shots = []
        
        for i, row in df.iterrows():
            # Extract shot data from row
            minute = safe_int(row.iloc[0])
            player_name = str(row.iloc[1]).strip() if not pd.isna(row.iloc[1]) else None
            squad = str(row.iloc[2]).strip() if not pd.isna(row.iloc[2]) else None
            xg = safe_float(row.iloc[3])
            psxg = safe_float(row.iloc[4])
            outcome_text = str(row.iloc[5]).strip() if not pd.isna(row.iloc[5]) else None
            distance = safe_int(row.iloc[6])
            body_part = str(row.iloc[7]).strip() if not pd.isna(row.iloc[7]) else None
            notes = str(row.iloc[8]).strip() if not pd.isna(row.iloc[8]) else None
            
            # Shot-creating actions
            sca1_player = str(row.iloc[9]).strip() if not pd.isna(row.iloc[9]) else None
            sca1_event = str(row.iloc[10]).strip() if not pd.isna(row.iloc[10]) else None
            sca2_player = str(row.iloc[11]).strip() if not pd.isna(row.iloc[11]) else None
            sca2_event = str(row.iloc[12]).strip() if not pd.isna(row.iloc[12]) else None
            
            # Skip invalid rows
            if not player_name or player_name in ['nan', '']:
                continue
            if minute is None:
                continue
                
            # Generate unique shot_id
            shot_id = generate_shot_id(match_id, minute or 0, player_name, i)
            
            shot_data = {
                'shot_id': shot_id,
                'match_id': match_id,
                'minute': minute,
                'player_name': player_name,
                'player_id': None,  # Will be resolved later
                'squad': squad,
                'xg': xg,
                'psxg': psxg,
                'outcome_id': map_outcome_to_id(outcome_text),
                'distance': distance,
                'body_part': body_part if body_part not in ['nan', ''] else None,
                'notes': notes if notes not in ['nan', ''] else None,
                'sca1_player_name': sca1_player if sca1_player not in ['nan', ''] else None,
                'sca1_event': sca1_event if sca1_event not in ['nan', ''] else None,
                'sca2_player_name': sca2_player if sca2_player not in ['nan', ''] else None,
                'sca2_event': sca2_event if sca2_event not in ['nan', ''] else None
            }
            
            shots.append(shot_data)
            
    except Exception as e:
        logging.error(f"Error processing shots file {shots_file}: {e}")
        return []
    
    logging.info(f"Extracted {len(shots)} shots from match {match_id}")
    return shots

def resolve_player_ids(shots, db_path):
    """Resolve player_id for each shot."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    for shot in shots:
        player_name = shot.get('player_name', '')
        
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
            shot['player_id'] = result[0]
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
                    shot['player_id'] = result[0]
    
    conn.close()

def save_shots_to_database(shots, db_path):
    """Save shot data to the database."""
    if not shots:
        return
        
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Define columns for INSERT
    columns = [
        'shot_id', 'match_id', 'minute', 'player_name', 'player_id', 'squad',
        'xg', 'psxg', 'outcome_id', 'distance', 'body_part', 'notes',
        'sca1_player_name', 'sca1_event', 'sca2_player_name', 'sca2_event'
    ]
    
    # Insert shots
    for shot in shots:
        values = [shot.get(col) for col in columns]
        placeholders = ','.join(['?' for _ in columns])
        
        cursor.execute(f"""
            INSERT OR REPLACE INTO match_shot ({','.join(columns)}) 
            VALUES ({placeholders})
        """, values)
    
    conn.commit()
    conn.close()
    
    logging.info(f"Saved {len(shots)} shots to database")

def main():
    """Main function to test local shots extraction."""
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    
    # Test with a match that has local data
    test_match_id = "24ddd8b3"
    
    # Extract shots from local CSV files
    shots = extract_shots_from_local_csv(test_match_id)
    
    if shots:
        # Resolve player IDs
        resolve_player_ids(shots, db_path)
        
        # Save to database
        save_shots_to_database(shots, db_path)
        
        print(f"Successfully extracted {len(shots)} shots from local data")
        
        # Show sample data
        for i, shot in enumerate(shots[:3]):
            print(f"\nShot {i+1}: {shot.get('player_name', 'Unknown')} at {shot.get('minute', 'N/A')}'")
            print(f"  Outcome: {shot.get('outcome_id', 'Unknown')}, xG: {shot.get('xg', 0.0)}")
            print(f"  Distance: {shot.get('distance', 'N/A')}, Body Part: {shot.get('body_part', 'N/A')}")
    else:
        print("No shots extracted from local data")

if __name__ == "__main__":
    main()