#!/usr/bin/env python3
"""
COMPREHENSIVE Team Statistics Extraction System
Extracts ALL available team statistics from multiple CSV file types:
- summary.csv: basic performance, goals, shots, cards
- misc.csv: fouls, corners, offsides, aerial duels  
- possession.csv: touches, carries, take-ons
- passing.csv: detailed passing statistics
- defense.csv: tackles, interceptions, blocks
"""

import sqlite3
import pandas as pd
import os
import hashlib
import logging
from pathlib import Path
from collections import defaultdict

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_comprehensive_team_stats_id(match_id: str, team_id: str) -> str:
    """Generate unique comprehensive team stats ID."""
    content = f"comprehensive_{match_id}_{team_id}"
    hex_hash = hashlib.md5(content.encode()).hexdigest()[:8]
    return f"team_comp_{hex_hash}"

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

def extract_team_totals_from_csv(file_path, expected_columns=None):
    """Extract team totals row from any CSV file type with robust error handling."""
    try:
        # Read CSV with multi-level headers
        df = pd.read_csv(file_path, header=[0, 1])
        
        if len(df) < 1:
            return None
        
        # Find the team totals row (contains "Players" in first column)
        team_totals_row = None
        for idx, row in df.iterrows():
            first_col = str(row.iloc[0]).strip()
            if 'Players' in first_col and first_col.split()[0].isdigit():
                team_totals_row = row
                break
        
        if team_totals_row is None:
            logging.warning(f"No team totals row found in {file_path}")
            return None
            
        return team_totals_row
        
    except Exception as e:
        logging.error(f"Error processing {file_path}: {e}")
        return None

def extract_comprehensive_team_stats_from_match(match_id, tables_dir="/Users/thomasmcmillan/projects/nwsl_data/notebooks/match_html_files/tables"):
    """Extract comprehensive team statistics from ALL available CSV file types."""
    
    match_dir = Path(tables_dir) / match_id
    if not match_dir.exists():
        logging.warning(f"No local data found for match {match_id} at {match_dir}")
        return []
    
    logging.info(f"Processing comprehensive team stats for match {match_id}")
    
    # Find all team IDs from available files
    team_files = defaultdict(dict)
    csv_files = list(match_dir.glob("*.csv"))
    
    for file_path in csv_files:
        filename = file_path.name
        if "_stats_" in filename and not filename.endswith("_all.csv") and not filename.startswith("sched_"):
            # Parse filename: {match_id}_stats_{team_id}_{type}.csv
            parts = filename.replace('.csv', '').split('_')
            if len(parts) >= 4:
                team_id = parts[2]
                stat_type = '_'.join(parts[3:])  # Handle multi-part types like "passing_types"
                # Skip invalid team IDs that are actually stat types
                if len(team_id) >= 8 and not stat_type in ['stats']:  # Valid team IDs are 8+ char hashes
                    team_files[team_id][stat_type] = file_path
    
    if not team_files:
        logging.warning(f"No team stat files found for match {match_id}")
        return []
    
    comprehensive_stats = []
    
    for team_id, files in team_files.items():
        logging.info(f"Processing comprehensive stats for team {team_id}")
        
        # Initialize comprehensive stats dictionary
        stats_data = {
            'team_stats_id': generate_comprehensive_team_stats_id(match_id, team_id),
            'match_id': match_id,
            'team_id': team_id,
        }
        
        # Extract from SUMMARY CSV (basic performance)
        if 'summary' in files:
            summary_row = extract_team_totals_from_csv(files['summary'])
            if summary_row is not None:
                try:
                    # Based on summary CSV structure: Player,#,Nation,Pos,Age,Min,Gls,Ast,PK,PKatt,Sh,SoT,CrdY,CrdR,Touches,Tkl,Int,Blocks,xG,npxG,xAG,SCA,GCA,Cmp,Att,Cmp%,PrgP,Carries,PrgC,Att,Succ
                    stats_data.update({
                        'goals': safe_int(summary_row.iloc[6]),  # Gls
                        'assists': safe_int(summary_row.iloc[7]),  # Ast
                        'shots': safe_int(summary_row.iloc[10]),  # Sh
                        'shots_on_target': safe_int(summary_row.iloc[11]),  # SoT
                        'yellow_cards': safe_int(summary_row.iloc[12]),  # CrdY
                        'red_cards': safe_int(summary_row.iloc[13]),  # CrdR
                        'touches': safe_int(summary_row.iloc[14]),  # Touches
                        'tackles': safe_int(summary_row.iloc[15]),  # Tkl
                        'interceptions': safe_int(summary_row.iloc[16]),  # Int
                        'blocks': safe_int(summary_row.iloc[17]),  # Blocks
                        'passes_completed': safe_int(summary_row.iloc[23]),  # Cmp
                        'passes_attempted': safe_int(summary_row.iloc[24]),  # Att
                        'pass_accuracy': safe_float(summary_row.iloc[25]),  # Cmp%
                        'progressive_passes': safe_int(summary_row.iloc[26]),  # PrgP
                        'carries': safe_int(summary_row.iloc[27]),  # Carries
                        'progressive_carries': safe_int(summary_row.iloc[28]),  # PrgC
                    })
                except Exception as e:
                    logging.error(f"Error extracting summary stats for team {team_id}: {e}")
        
        # Extract from MISC CSV (fouls, corners, offsides, aerial duels)
        if 'misc' in files:
            misc_row = extract_team_totals_from_csv(files['misc'])
            if misc_row is not None:
                try:
                    # Based on misc CSV structure: Player,#,Nation,Pos,Age,Min,CrdY,CrdR,2CrdY,Fls,Fld,Off,Crs,Int,TklW,PKwon,PKcon,OG,Recov,Won,Lost,Won%
                    stats_data.update({
                        'fouls': safe_int(misc_row.iloc[9]),  # Fls
                        'fouled': safe_int(misc_row.iloc[10]),  # Fld  
                        'offsides': safe_int(misc_row.iloc[11]),  # Off
                        'corners': safe_int(misc_row.iloc[12]),  # Crs
                        'tackles_won': safe_int(misc_row.iloc[14]),  # TklW
                        'aerials_won': safe_int(misc_row.iloc[19]),  # Won
                        'aerials_lost': safe_int(misc_row.iloc[20]),  # Lost
                    })
                except Exception as e:
                    logging.error(f"Error extracting misc stats for team {team_id}: {e}")
        
        # Extract from POSSESSION CSV (touches detail, carries, take-ons)
        if 'possession' in files:
            possession_row = extract_team_totals_from_csv(files['possession'])
            if possession_row is not None:
                try:
                    # Based on possession CSV structure: Player,#,Nation,Pos,Age,Min,Touches,Def Pen,Def 3rd,Mid 3rd,Att 3rd,Att Pen,Live,Att,Succ,Succ%,Tkld,Tkld%,Carries,TotDist,PrgDist,PrgC,1/3,CPA,Mis,Dis,Rec,PrgR
                    stats_data.update({
                        'touches_def_pen': safe_int(possession_row.iloc[7]),  # Def Pen
                        'touches_def_3rd': safe_int(possession_row.iloc[8]),  # Def 3rd
                        'touches_mid_3rd': safe_int(possession_row.iloc[9]),  # Mid 3rd
                        'touches_att_3rd': safe_int(possession_row.iloc[10]),  # Att 3rd
                        'touches_att_pen': safe_int(possession_row.iloc[11]),  # Att Pen
                        'take_ons_attempted': safe_int(possession_row.iloc[13]),  # Att (take-ons)
                        'take_ons_successful': safe_int(possession_row.iloc[14]),  # Succ
                        'carries_distance': safe_int(possession_row.iloc[19]),  # TotDist
                    })
                except Exception as e:
                    logging.error(f"Error extracting possession stats for team {team_id}: {e}")
        
        # Extract from PASSING CSV (detailed passing breakdown)
        if 'passing' in files:
            passing_row = extract_team_totals_from_csv(files['passing'])
            if passing_row is not None:
                try:
                    # Based on passing CSV structure: Player,#,Nation,Pos,Age,Min,Cmp,Att,Cmp%,TotDist,PrgDist,Cmp,Att,Cmp%,Cmp,Att,Cmp%,Cmp,Att,Cmp%,Ast,xAG,xA,KP,1/3,PPA,CrsPA,PrgP
                    stats_data.update({
                        'short_passes_completed': safe_int(passing_row.iloc[11]),  # Short Cmp
                        'short_passes_attempted': safe_int(passing_row.iloc[12]),  # Short Att
                        'medium_passes_completed': safe_int(passing_row.iloc[14]),  # Medium Cmp
                        'medium_passes_attempted': safe_int(passing_row.iloc[15]),  # Medium Att
                        'long_passes_completed': safe_int(passing_row.iloc[17]),  # Long Cmp
                        'long_passes_attempted': safe_int(passing_row.iloc[18]),  # Long Att
                        'key_passes': safe_int(passing_row.iloc[23]),  # KP
                        'passes_into_final_third': safe_int(passing_row.iloc[24]),  # 1/3
                    })
                except Exception as e:
                    logging.error(f"Error extracting passing stats for team {team_id}: {e}")
        
        # Extract from DEFENSE CSV (detailed defensive actions)
        if 'defense' in files:
            defense_row = extract_team_totals_from_csv(files['defense'])
            if defense_row is not None:
                try:
                    # Based on defense CSV structure: Player,#,Nation,Pos,Age,Min,Tkl,TklW,Def 3rd,Mid 3rd,Att 3rd,Tkl,Att,Tkl%,Lost,Blocks,Sh,Pass,Int,Tkl+Int,Clr,Err
                    stats_data.update({
                        'tackles_def_3rd': safe_int(defense_row.iloc[8]),  # Def 3rd
                        'tackles_mid_3rd': safe_int(defense_row.iloc[9]),  # Mid 3rd
                        'tackles_att_3rd': safe_int(defense_row.iloc[10]),  # Att 3rd
                        'blocks_shots': safe_int(defense_row.iloc[16]),  # Sh (blocks)
                        'blocks_passes': safe_int(defense_row.iloc[17]),  # Pass (blocks)
                        'clearances': safe_int(defense_row.iloc[20]),  # Clr
                    })
                except Exception as e:
                    logging.error(f"Error extracting defense stats for team {team_id}: {e}")
        
        # Calculate advanced metrics
        try:
            # Pass accuracy by distance
            if stats_data.get('short_passes_attempted', 0) > 0:
                stats_data['pass_accuracy_short'] = (stats_data.get('short_passes_completed', 0) / stats_data['short_passes_attempted']) * 100
            
            if stats_data.get('medium_passes_attempted', 0) > 0:
                stats_data['pass_accuracy_medium'] = (stats_data.get('medium_passes_completed', 0) / stats_data['medium_passes_attempted']) * 100
            
            if stats_data.get('long_passes_attempted', 0) > 0:
                stats_data['pass_accuracy_long'] = (stats_data.get('long_passes_completed', 0) / stats_data['long_passes_attempted']) * 100
            
            # Take-on success rate
            if stats_data.get('take_ons_attempted', 0) > 0:
                stats_data['take_on_success_rate'] = (stats_data.get('take_ons_successful', 0) / stats_data['take_ons_attempted']) * 100
            
            # Tackle success rate
            if stats_data.get('tackles', 0) > 0:
                stats_data['tackle_success_rate'] = (stats_data.get('tackles_won', 0) / stats_data['tackles']) * 100
            
            # Aerial duel win rate
            total_aerials = (stats_data.get('aerials_won', 0) or 0) + (stats_data.get('aerials_lost', 0) or 0)
            if total_aerials > 0:
                stats_data['aerial_win_rate'] = (stats_data.get('aerials_won', 0) / total_aerials) * 100
        
        except Exception as e:
            logging.error(f"Error calculating advanced metrics for team {team_id}: {e}")
        
        comprehensive_stats.append(stats_data)
    
    logging.info(f"Extracted comprehensive stats for {len(comprehensive_stats)} teams from match {match_id}")
    return comprehensive_stats

def save_comprehensive_team_stats_to_database(team_stats, db_path):
    """Save comprehensive team statistics to the database."""
    if not team_stats:
        return
        
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Define all columns for INSERT
    columns = [
        'team_stats_id', 'match_id', 'team_id',
        # Basic Performance
        'goals', 'assists', 'shots', 'shots_on_target', 'yellow_cards', 'red_cards',
        # Passing Statistics
        'passes_completed', 'passes_attempted', 'pass_accuracy',
        'short_passes_completed', 'short_passes_attempted',
        'medium_passes_completed', 'medium_passes_attempted',
        'long_passes_completed', 'long_passes_attempted',
        'progressive_passes', 'key_passes', 'passes_into_final_third',
        # Possession & Movement
        'touches', 'touches_def_pen', 'touches_def_3rd', 'touches_mid_3rd',
        'touches_att_3rd', 'touches_att_pen', 'carries', 'carries_distance',
        'progressive_carries', 'take_ons_attempted', 'take_ons_successful',
        # Defensive Actions
        'tackles', 'tackles_won', 'tackles_def_3rd', 'tackles_mid_3rd', 'tackles_att_3rd',
        'interceptions', 'blocks', 'blocks_shots', 'blocks_passes', 'clearances',
        # Miscellaneous
        'fouls', 'fouled', 'offsides', 'corners', 'aerials_won', 'aerials_lost',
        # Advanced Metrics
        'possession_pct', 'pass_accuracy_short', 'pass_accuracy_medium', 'pass_accuracy_long',
        'take_on_success_rate', 'tackle_success_rate', 'aerial_win_rate'
    ]
    
    # Insert comprehensive team stats
    for stats in team_stats:
        values = [stats.get(col) for col in columns]
        placeholders = ','.join(['?' for _ in columns])
        
        cursor.execute(f"""
            INSERT OR REPLACE INTO match_team_comprehensive ({','.join(columns)}) 
            VALUES ({placeholders})
        """, values)
    
    conn.commit()
    conn.close()
    
    logging.info(f"Saved {len(team_stats)} comprehensive team stats entries to database")

def main():
    """Test comprehensive team stats extraction."""
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    
    # Test with a match that has comprehensive data
    test_match_id = "dd365af8"  # Match we examined earlier
    
    # Extract comprehensive team stats
    team_stats = extract_comprehensive_team_stats_from_match(test_match_id)
    
    if team_stats:
        # Save to database
        save_comprehensive_team_stats_to_database(team_stats, db_path)
        
        print(f"Successfully extracted comprehensive stats for {len(team_stats)} teams")
        
        # Show sample comprehensive data
        for stats in team_stats:
            print(f"\n=== COMPREHENSIVE TEAM STATS: {stats['team_id']} ===")
            print(f"⚽ Basic: {stats.get('goals', 0)} goals, {stats.get('shots', 0)} shots ({stats.get('shots_on_target', 0)} on target)")
            print(f"📋 Passing: {stats.get('passes_completed', 0)}/{stats.get('passes_attempted', 0)} ({stats.get('pass_accuracy', 0):.1f}%)")
            print(f"   - Short: {stats.get('short_passes_completed', 0)}/{stats.get('short_passes_attempted', 0)} ({stats.get('pass_accuracy_short', 0):.1f}%)")
            print(f"   - Long: {stats.get('long_passes_completed', 0)}/{stats.get('long_passes_attempted', 0)} ({stats.get('pass_accuracy_long', 0):.1f}%)")
            print(f"🏃 Possession: {stats.get('touches', 0)} touches, {stats.get('carries', 0)} carries ({stats.get('carries_distance', 0)} yards)")
            print(f"🛡️ Defense: {stats.get('tackles', 0)} tackles ({stats.get('tackle_success_rate', 0):.1f}% success), {stats.get('interceptions', 0)} interceptions")
            print(f"⚠️ Discipline: {stats.get('fouls', 0)} fouls, {stats.get('yellow_cards', 0)} yellows, {stats.get('corners', 0)} corners")
            print(f"✈️ Aerials: {stats.get('aerials_won', 0)}-{stats.get('aerials_lost', 0)} ({stats.get('aerial_win_rate', 0):.1f}% win rate)")
    else:
        print("No comprehensive team stats extracted")

if __name__ == "__main__":
    main()