#!/usr/bin/env python3
"""
Create Team Summary from CSV Summary Files
Extracts team totals from the summary CSV files directly without complex header parsing.
"""

import sqlite3
import pandas as pd
import os
import hashlib
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_team_stats_id(match_id: str, team_id: str) -> str:
    """Generate unique team stats ID."""
    content = f"team_summary_{match_id}_{team_id}"
    hex_hash = hashlib.md5(content.encode()).hexdigest()[:8]
    return f"team_{hex_hash}"

def extract_team_totals_from_summary(csv_file_path: str) -> dict:
    """
    Extract team totals from summary CSV file.
    The team totals are in the last row (excluding empty rows).
    """
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file_path, header=[0, 1])
        
        # Find the totals row - it's usually the last non-empty row
        # Look for a row that contains "Players" in the first column
        totals_row = None
        for idx in range(len(df)-1, -1, -1):  # Start from the end
            first_col_val = str(df.iloc[idx, 0]).strip()
            if 'Players' in first_col_val or first_col_val.isdigit():
                totals_row = df.iloc[idx]
                break
        
        if totals_row is None:
            logging.warning(f"No totals row found in {csv_file_path}")
            return {}
        
        # Extract key statistics from the totals row
        team_stats = {}
        
        # Map column positions to stats (based on the summary CSV structure we saw)
        # Columns: Player,#,Nation,Pos,Age,Min,Gls,Ast,PK,PKatt,Sh,SoT,CrdY,CrdR,Touches,Tkl,Int,Blocks,xG,npxG,xAG,SCA,GCA,Cmp,Att,Cmp%,PrgP,Carries,PrgC,Att,Succ
        
        try:
            team_stats['goals'] = safe_int(totals_row.iloc[6])  # Gls
            team_stats['assists'] = safe_int(totals_row.iloc[7])  # Ast
            team_stats['shots'] = safe_int(totals_row.iloc[10])  # Sh
            team_stats['shots_on_target'] = safe_int(totals_row.iloc[11])  # SoT
            team_stats['yellow_cards'] = safe_int(totals_row.iloc[12])  # CrdY
            team_stats['red_cards'] = safe_int(totals_row.iloc[13])  # CrdR
            team_stats['touches'] = safe_int(totals_row.iloc[14])  # Touches
            team_stats['tackles'] = safe_int(totals_row.iloc[15])  # Tkl
            team_stats['interceptions'] = safe_int(totals_row.iloc[16])  # Int
            team_stats['blocks'] = safe_int(totals_row.iloc[17])  # Blocks
            team_stats['passes_completed'] = safe_int(totals_row.iloc[23])  # Cmp
            team_stats['passes_attempted'] = safe_int(totals_row.iloc[24])  # Att
            team_stats['progressive_passes'] = safe_int(totals_row.iloc[26])  # PrgP
            team_stats['carries'] = safe_int(totals_row.iloc[27])  # Carries
            team_stats['progressive_carries'] = safe_int(totals_row.iloc[28])  # PrgC
            team_stats['take_ons_attempted'] = safe_int(totals_row.iloc[29])  # Att (take-ons)
            team_stats['take_ons_successful'] = safe_int(totals_row.iloc[30])  # Succ
            
            # Calculate derived stats
            if team_stats['passes_attempted'] > 0:
                team_stats['pass_accuracy'] = round((team_stats['passes_completed'] / team_stats['passes_attempted']) * 100, 1)
            else:
                team_stats['pass_accuracy'] = 0.0
                
            if team_stats['shots'] > 0:
                team_stats['shot_accuracy'] = round((team_stats['shots_on_target'] / team_stats['shots']) * 100, 1)
            else:
                team_stats['shot_accuracy'] = 0.0
                
            if team_stats['take_ons_attempted'] > 0:
                team_stats['take_on_success_rate'] = round((team_stats['take_ons_successful'] / team_stats['take_ons_attempted']) * 100, 1)
            else:
                team_stats['take_on_success_rate'] = 0.0
        
        except Exception as e:
            logging.error(f"Error extracting stats from {csv_file_path}: {e}")
            return {}
        
        return team_stats
        
    except Exception as e:
        logging.error(f"Error reading {csv_file_path}: {e}")
        return {}

def safe_int(value, default=0):
    """Safely convert value to int."""
    if pd.isna(value) or value == '' or value is None:
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default

def create_team_summary_table(db_path: str):
    """Create the team summary table."""
    conn = sqlite3.connect(db_path)
    
    create_sql = """
    CREATE TABLE IF NOT EXISTS match_team_summary (
        team_stats_id TEXT PRIMARY KEY,
        match_id TEXT NOT NULL,
        team_id TEXT NOT NULL,
        match_date DATE,
        
        -- Basic Performance
        goals INTEGER DEFAULT 0,
        assists INTEGER DEFAULT 0,
        shots INTEGER DEFAULT 0,
        shots_on_target INTEGER DEFAULT 0,
        yellow_cards INTEGER DEFAULT 0,
        red_cards INTEGER DEFAULT 0,
        
        -- Core Passing
        passes_completed INTEGER DEFAULT 0,
        passes_attempted INTEGER DEFAULT 0,
        pass_accuracy REAL DEFAULT 0.0,
        progressive_passes INTEGER DEFAULT 0,
        
        -- Core Defensive
        tackles INTEGER DEFAULT 0,
        interceptions INTEGER DEFAULT 0,
        blocks INTEGER DEFAULT 0,
        
        -- Core Possession
        touches INTEGER DEFAULT 0,
        carries INTEGER DEFAULT 0,
        progressive_carries INTEGER DEFAULT 0,
        take_ons_attempted INTEGER DEFAULT 0,
        take_ons_successful INTEGER DEFAULT 0,
        
        -- Calculated Metrics
        shot_accuracy REAL DEFAULT 0.0,
        take_on_success_rate REAL DEFAULT 0.0,
        
        FOREIGN KEY (match_id) REFERENCES match(match_id),
        FOREIGN KEY (team_id) REFERENCES team(team_id),
        UNIQUE(match_id, team_id)
    );
    """
    
    conn.execute(create_sql)
    conn.commit()
    conn.close()
    
    logging.info("✅ Created match_team_summary table")

def process_csv_files(tables_dir: str, db_path: str):
    """Process all summary CSV files and insert into database."""
    
    tables_path = Path(tables_dir)
    processed_count = 0
    error_count = 0
    
    # Get all match directories
    match_dirs = [d for d in tables_path.iterdir() if d.is_dir()]
    logging.info(f"Found {len(match_dirs)} match directories")
    
    conn = sqlite3.connect(db_path)
    
    for match_dir in match_dirs:
        match_id = match_dir.name
        
        # Find all summary CSV files for this match
        summary_files = list(match_dir.glob(f"{match_id}_stats_*_summary.csv"))
        
        if not summary_files:
            continue
            
        logging.info(f"Processing match {match_id} ({len(summary_files)} teams)")
        
        for summary_file in summary_files:
            # Extract team_id from filename
            parts = summary_file.stem.split('_')
            if len(parts) >= 3:
                team_id = parts[2]
            else:
                logging.warning(f"Could not extract team_id from {summary_file}")
                continue
            
            try:
                # Extract team statistics
                team_stats = extract_team_totals_from_summary(summary_file)
                
                if not team_stats:
                    error_count += 1
                    continue
                
                # Get match date
                match_date_query = "SELECT match_date FROM match WHERE match_id = ?"
                match_date_result = conn.execute(match_date_query, (match_id,)).fetchone()
                match_date = match_date_result[0] if match_date_result else None
                
                # Generate team stats ID
                team_stats_id = generate_team_stats_id(match_id, team_id)
                
                # Insert into database
                insert_sql = """
                INSERT OR REPLACE INTO match_team_summary (
                    team_stats_id, match_id, team_id, match_date,
                    goals, assists, shots, shots_on_target, yellow_cards, red_cards,
                    passes_completed, passes_attempted, pass_accuracy, progressive_passes,
                    tackles, interceptions, blocks,
                    touches, carries, progressive_carries, take_ons_attempted, take_ons_successful,
                    shot_accuracy, take_on_success_rate
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                values = (
                    team_stats_id, match_id, team_id, match_date,
                    team_stats.get('goals', 0), team_stats.get('assists', 0),
                    team_stats.get('shots', 0), team_stats.get('shots_on_target', 0),
                    team_stats.get('yellow_cards', 0), team_stats.get('red_cards', 0),
                    team_stats.get('passes_completed', 0), team_stats.get('passes_attempted', 0),
                    team_stats.get('pass_accuracy', 0.0), team_stats.get('progressive_passes', 0),
                    team_stats.get('tackles', 0), team_stats.get('interceptions', 0),
                    team_stats.get('blocks', 0),
                    team_stats.get('touches', 0), team_stats.get('carries', 0),
                    team_stats.get('progressive_carries', 0), team_stats.get('take_ons_attempted', 0),
                    team_stats.get('take_ons_successful', 0),
                    team_stats.get('shot_accuracy', 0.0), team_stats.get('take_on_success_rate', 0.0)
                )
                
                conn.execute(insert_sql, values)
                processed_count += 1
                
            except Exception as e:
                logging.error(f"Error processing {summary_file}: {e}")
                error_count += 1
    
    conn.commit()
    conn.close()
    
    logging.info(f"✅ Processed {processed_count} team records")
    logging.info(f"❌ Errors: {error_count}")
    
    return processed_count, error_count

def validate_data(db_path: str):
    """Validate the created data."""
    conn = sqlite3.connect(db_path)
    
    # Get summary statistics
    summary_sql = """
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT match_id) as unique_matches,
        COUNT(DISTINCT team_id) as unique_teams,
        SUM(goals) as total_goals,
        ROUND(AVG(goals), 2) as avg_goals_per_team,
        ROUND(AVG(pass_accuracy), 1) as avg_pass_accuracy
    FROM match_team_summary;
    """
    
    summary = conn.execute(summary_sql).fetchone()
    
    logging.info("📊 SUMMARY STATISTICS:")
    logging.info(f"   Total team records: {summary[0]}")
    logging.info(f"   Unique matches: {summary[1]}")
    logging.info(f"   Unique teams: {summary[2]}")
    logging.info(f"   Total goals: {summary[3]}")
    logging.info(f"   Average goals per team: {summary[4]}")
    logging.info(f"   Average pass accuracy: {summary[5]}%")
    
    # Show sample data
    sample_sql = """
    SELECT match_id, team_id, match_date, goals, shots, passes_completed, pass_accuracy
    FROM match_team_summary 
    WHERE goals > 0
    ORDER BY goals DESC
    LIMIT 5;
    """
    
    sample_data = conn.execute(sample_sql).fetchall()
    
    logging.info("📋 TOP SCORING TEAMS:")
    for row in sample_data:
        logging.info(f"   Match {row[0][:8]}, Team {row[1][:8]} on {row[2]}: {row[3]} goals, {row[4]} shots, {row[6]:.1f}% pass accuracy")
    
    conn.close()

if __name__ == "__main__":
    
    # Configuration
    tables_dir = "/Users/thomasmcmillan/projects/nwsl_data/notebooks/match_html_files/tables"
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    
    logging.info("🚀 CREATING TEAM SUMMARY FROM CSV FILES")
    logging.info("=" * 60)
    
    # Step 1: Create table
    create_team_summary_table(db_path)
    
    # Step 2: Process CSV files
    processed, errors = process_csv_files(tables_dir, db_path)
    
    if processed > 0:
        # Step 3: Validate data
        validate_data(db_path)
        
        logging.info("=" * 60)
        logging.info("🎉 SUCCESS! Team summary created from CSV files")
        logging.info(f"📊 {processed} team records processed")
        logging.info("✨ Using real scraped data from HTML tables!")
    else:
        logging.error("❌ No data was processed successfully")