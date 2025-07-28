#!/usr/bin/env python3
"""
Extract team-level aggregate statistics from summary CSV files.
Populates match_team table with comprehensive team performance metrics.
"""

import sqlite3
import pandas as pd
import os
import hashlib
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_team_stats_id(match_id: str, team_id: str) -> str:
    """Generate unique team_stats_id."""
    content = f"{match_id}_{team_id}"
    hex_hash = hashlib.md5(content.encode()).hexdigest()[:8]
    return f"team_{hex_hash}"

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

def extract_team_stats_from_match(match_id, tables_dir="/Users/thomasmcmillan/projects/nwsl_data/notebooks/match_html_files/tables"):
    """Extract team aggregate statistics from match summary CSV files."""
    
    match_dir = Path(tables_dir) / match_id
    if not match_dir.exists():
        logging.warning(f"No local data found for match {match_id} at {match_dir}")
        return []
    
    # Find all summary stat files for this match
    summary_files = list(match_dir.glob(f"{match_id}_stats_*_summary.csv"))
    
    if not summary_files:
        logging.info(f"No summary files found for match {match_id}")
        return []
    
    logging.info(f"Processing team stats for match {match_id}")
    
    team_stats = []
    
    for file_path in summary_files:
        filename = file_path.name
        # Parse filename: {match_id}_stats_{team_id}_summary.csv
        parts = filename.replace('.csv', '').split('_')
        if len(parts) < 4:
            continue
            
        team_id = parts[2]
        
        logging.info(f"Processing team stats for team {team_id}")
        
        try:
            # Read CSV with multi-level headers
            df = pd.read_csv(file_path, header=[0, 1])
            
            # Skip if no data
            if len(df) < 1:
                continue
            
            # Find the team totals row (contains "Players" in first column)
            team_totals_row = None
            for idx, row in df.iterrows():
                first_col = str(row.iloc[0]).strip()
                if 'Players' in first_col and first_col.split()[0].isdigit():
                    team_totals_row = row
                    break
            
            if team_totals_row is None:
                logging.warning(f"No team totals row found for team {team_id} in match {match_id}")
                continue
            
            # Extract team statistics from the totals row
            # Based on the CSV structure we observed:
            # Player,#,Nation,Pos,Age,Min,Gls,Ast,PK,PKatt,Sh,SoT,CrdY,CrdR,Touches,Tkl,Int,Blocks,xG,npxG,xAG,SCA,GCA,Cmp,Att,Cmp%,PrgP,Carries,PrgC,Att,Succ
            
            team_stats_id = generate_team_stats_id(match_id, team_id)
            
            stats_data = {
                'team_stats_id': team_stats_id,
                'match_id': match_id,
                'team_id': team_id,
                'possession_pct': None,  # Not directly available in summary, would need possession CSV
                'shots': safe_int(team_totals_row.iloc[10]),  # Sh
                'shots_on_target': safe_int(team_totals_row.iloc[11]),  # SoT
                'passes_completed': safe_int(team_totals_row.iloc[23]),  # Cmp
                'passes_attempted': safe_int(team_totals_row.iloc[24]),  # Att
                'pass_accuracy': safe_float(team_totals_row.iloc[25]),  # Cmp%
                'corners': None,  # Not available in summary stats
                'offsides': None,  # Not available in summary stats
                'fouls': None,  # Not available in summary stats
                'yellow_cards': safe_int(team_totals_row.iloc[12]),  # CrdY
                'red_cards': safe_int(team_totals_row.iloc[13]),  # CrdR
            }
            
            team_stats.append(stats_data)
                
        except Exception as e:
            logging.error(f"Error processing team stats file {file_path}: {e}")
            continue
    
    logging.info(f"Extracted team stats for {len(team_stats)} teams from match {match_id}")
    return team_stats

def save_team_stats_to_database(team_stats, db_path):
    """Save team statistics to the database."""
    if not team_stats:
        return
        
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Define columns for INSERT to match actual table schema
    columns = [
        'team_stats_id', 'match_id', 'team_id', 'possession_pct', 'shots', 'shots_on_target',
        'passes_completed', 'passes_attempted', 'pass_accuracy', 'corners', 'offsides',
        'fouls', 'yellow_cards', 'red_cards'
    ]
    
    # Insert team stats
    for stats in team_stats:
        values = [stats.get(col) for col in columns]
        placeholders = ','.join(['?' for _ in columns])
        
        cursor.execute(f"""
            INSERT OR REPLACE INTO match_team ({','.join(columns)}) 
            VALUES ({placeholders})
        """, values)
    
    conn.commit()
    conn.close()
    
    logging.info(f"Saved {len(team_stats)} team stats entries to database")

def main():
    """Main function to test team stats extraction."""
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    
    # Test with a match that has local data
    test_match_id = "67a57f59"  # The match from the CSV we examined
    
    # Extract team stats from local CSV files
    team_stats = extract_team_stats_from_match(test_match_id)
    
    if team_stats:
        # Save to database
        save_team_stats_to_database(team_stats, db_path)
        
        print(f"Successfully extracted team stats for {len(team_stats)} teams")
        
        # Show sample data
        for stats in team_stats:
            print(f"\nTeam {stats['team_id']}:")
            print(f"  Shots: {stats['shots']} ({stats['shots_on_target']} on target)")
            if stats['passes_completed'] and stats['passes_attempted']:
                print(f"  Passes: {stats['passes_completed']}/{stats['passes_attempted']} ({stats['pass_accuracy']:.1f}%)")
            print(f"  Cards: {stats['yellow_cards']} yellow, {stats['red_cards']} red")
    else:
        print("No team stats extracted")

if __name__ == "__main__":
    main()