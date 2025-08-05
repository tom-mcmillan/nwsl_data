#!/usr/bin/env python3
"""
Aggregate Complete Team Statistics
Processes matches with 100% complete CSV data into match_team_comprehensive table.
Aggregates player-level stats into team-level summaries.
"""

import sqlite3
import pandas as pd
import os
import hashlib
from pathlib import Path
from collections import defaultdict
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Required stat categories for complete matches
REQUIRED_STATS = ['summary', 'passing', 'defense', 'misc', 'possession', 'passing_types']

def generate_team_stats_id(match_id: str, team_id: str) -> str:
    """Generate unique comprehensive team stats ID."""
    content = f"comprehensive_{match_id}_{team_id}"
    hex_hash = hashlib.md5(content.encode()).hexdigest()[:8]
    return f"team_comp_{hex_hash}"

def safe_numeric(value, default=0):
    """Safely convert value to numeric, return default if conversion fails."""
    if pd.isna(value) or value == '' or value is None:
        return default
    try:
        # Handle percentage strings like "56.5%"
        if isinstance(value, str) and value.endswith('%'):
            return float(value.rstrip('%'))
        return float(value)
    except (ValueError, TypeError):
        return default

def aggregate_team_stats_from_csvs(match_id: str, team_id: str, tables_dir: str) -> dict:
    """
    Aggregate team-level statistics from player-level CSV files.
    
    Returns:
        dict: Aggregated team statistics
    """
    
    tables_path = Path(tables_dir) / match_id
    team_stats = {}
    
    # Process each stat category
    for stat_type in REQUIRED_STATS:
        csv_file = tables_path / f"{match_id}_stats_{team_id}_{stat_type}.csv"
        
        if not csv_file.exists():
            logging.warning(f"Missing {stat_type} file: {csv_file}")
            continue
            
        try:
            # Read CSV with multi-level headers
            df = pd.read_csv(csv_file, header=[0, 1])
            
            # Skip header rows and get player data
            player_data = df.iloc[1:].copy()  # Skip the column name row
            
            # Process based on stat type
            if stat_type == 'summary':
                team_stats.update(aggregate_summary_stats(player_data))
            elif stat_type == 'passing':
                team_stats.update(aggregate_passing_stats(player_data))
            elif stat_type == 'defense':
                team_stats.update(aggregate_defense_stats(player_data))
            elif stat_type == 'misc':
                team_stats.update(aggregate_misc_stats(player_data))
            elif stat_type == 'possession':
                team_stats.update(aggregate_possession_stats(player_data))
            elif stat_type == 'passing_types':
                team_stats.update(aggregate_passing_types_stats(player_data))
                
        except Exception as e:
            logging.error(f"Error processing {csv_file}: {e}")
            continue
    
    return team_stats

def aggregate_summary_stats(df) -> dict:
    """Aggregate summary statistics (goals, assists, shots, cards)."""
    stats = {}
    
    # Basic performance metrics
    stats['goals'] = safe_numeric(df[('Performance', 'Gls')].sum())
    stats['assists'] = safe_numeric(df[('Performance', 'Ast')].sum())
    stats['shots'] = safe_numeric(df[('Performance', 'Sh')].sum())
    stats['shots_on_target'] = safe_numeric(df[('Performance', 'SoT')].sum())
    stats['yellow_cards'] = safe_numeric(df[('Performance', 'CrdY')].sum())
    stats['red_cards'] = safe_numeric(df[('Performance', 'CrdR')].sum())
    
    # Basic passing from summary
    stats['passes_completed'] = safe_numeric(df[('Passes', 'Cmp')].sum())
    stats['passes_attempted'] = safe_numeric(df[('Passes', 'Att')].sum())
    
    # Calculate pass accuracy
    if stats['passes_attempted'] > 0:
        stats['pass_accuracy'] = (stats['passes_completed'] / stats['passes_attempted']) * 100
    else:
        stats['pass_accuracy'] = 0.0
    
    # Progressive passes
    stats['progressive_passes'] = safe_numeric(df[('Passes', 'PrgP')].sum())
    
    # Other summary metrics
    stats['touches'] = safe_numeric(df[('Performance', 'Touches')].sum())
    
    return stats

def aggregate_passing_stats(df) -> dict:
    """Aggregate detailed passing statistics."""
    stats = {}
    
    # Total passing (should match summary)
    stats['passes_completed_detailed'] = safe_numeric(df[('Total', 'Cmp')].sum())
    stats['passes_attempted_detailed'] = safe_numeric(df[('Total', 'Att')].sum())
    
    # Short passes
    stats['short_passes_completed'] = safe_numeric(df[('Short', 'Cmp')].sum())
    stats['short_passes_attempted'] = safe_numeric(df[('Short', 'Att')].sum())
    
    # Medium passes  
    stats['medium_passes_completed'] = safe_numeric(df[('Medium', 'Cmp')].sum())
    stats['medium_passes_attempted'] = safe_numeric(df[('Medium', 'Att')].sum())
    
    # Long passes
    stats['long_passes_completed'] = safe_numeric(df[('Long', 'Cmp')].sum())
    stats['long_passes_attempted'] = safe_numeric(df[('Long', 'Att')].sum())
    
    # Key passes and final third
    stats['key_passes'] = safe_numeric(df[('Unnamed: 15_level_0', 'KP')].sum())
    stats['passes_into_final_third'] = safe_numeric(df[('Unnamed: 16_level_0', '1/3')].sum())
    
    # Calculate pass accuracy by distance
    for pass_type in ['short', 'medium', 'long']:
        completed = stats[f'{pass_type}_passes_completed']
        attempted = stats[f'{pass_type}_passes_attempted']
        if attempted > 0:
            stats[f'pass_accuracy_{pass_type}'] = (completed / attempted) * 100
        else:
            stats[f'pass_accuracy_{pass_type}'] = 0.0
    
    return stats

def aggregate_defense_stats(df) -> dict:
    """Aggregate defensive statistics."""
    stats = {}
    
    # Tackles
    stats['tackles'] = safe_numeric(df[('Tackles', 'Tkl')].sum())
    stats['tackles_won'] = safe_numeric(df[('Tackles', 'TklW')].sum())
    stats['tackles_def_3rd'] = safe_numeric(df[('Tackles', 'Def 3rd')].sum())
    stats['tackles_mid_3rd'] = safe_numeric(df[('Tackles', 'Mid 3rd')].sum())
    stats['tackles_att_3rd'] = safe_numeric(df[('Tackles', 'Att 3rd')].sum())
    
    # Interceptions and blocks
    stats['interceptions'] = safe_numeric(df[('Challenges', 'Int')].sum())
    stats['blocks'] = safe_numeric(df[('Blocks', 'Blocks')].sum())
    stats['blocks_shots'] = safe_numeric(df[('Blocks', 'Sh')].sum())
    stats['blocks_passes'] = safe_numeric(df[('Blocks', 'Pass')].sum())
    stats['clearances'] = safe_numeric(df[('Unnamed: 19_level_0', 'Clr')].sum())
    
    # Calculate tackle success rate
    if stats['tackles'] > 0:
        stats['tackle_success_rate'] = (stats['tackles_won'] / stats['tackles']) * 100
    else:
        stats['tackle_success_rate'] = 0.0
    
    return stats

def aggregate_misc_stats(df) -> dict:
    """Aggregate miscellaneous statistics (fouls, cards, aerials)."""
    stats = {}
    
    # Fouls
    stats['fouls'] = safe_numeric(df[('Performance', 'Fls')].sum())
    stats['fouled'] = safe_numeric(df[('Performance', 'Fld')].sum())
    stats['offsides'] = safe_numeric(df[('Performance', 'Off')].sum())
    
    # Set pieces
    stats['corners'] = safe_numeric(df[('Performance', 'Crs')].sum())
    
    # Aerial duels
    stats['aerials_won'] = safe_numeric(df[('Aerial Duels', 'Won')].sum())
    stats['aerials_lost'] = safe_numeric(df[('Aerial Duels', 'Lost')].sum())
    
    # Calculate aerial win rate
    total_aerials = stats['aerials_won'] + stats['aerials_lost']
    if total_aerials > 0:
        stats['aerial_win_rate'] = (stats['aerials_won'] / total_aerials) * 100
    else:
        stats['aerial_win_rate'] = 0.0
    
    return stats

def aggregate_possession_stats(df) -> dict:
    """Aggregate possession and movement statistics."""
    stats = {}
    
    # Touches by area
    stats['touches_def_pen'] = safe_numeric(df[('Touches', 'Def Pen')].sum())
    stats['touches_def_3rd'] = safe_numeric(df[('Touches', 'Def 3rd')].sum())
    stats['touches_mid_3rd'] = safe_numeric(df[('Touches', 'Mid 3rd')].sum())
    stats['touches_att_3rd'] = safe_numeric(df[('Touches', 'Att 3rd')].sum())
    stats['touches_att_pen'] = safe_numeric(df[('Touches', 'Att Pen')].sum())
    
    # Carries
    stats['carries'] = safe_numeric(df[('Carries', 'Carries')].sum())
    stats['carries_distance'] = safe_numeric(df[('Carries', 'TotDist')].sum())
    stats['progressive_carries'] = safe_numeric(df[('Carries', 'PrgC')].sum())
    
    # Take-ons
    stats['take_ons_attempted'] = safe_numeric(df[('Take-Ons', 'Att')].sum())
    stats['take_ons_successful'] = safe_numeric(df[('Take-Ons', 'Succ')].sum())
    
    # Calculate take-on success rate
    if stats['take_ons_attempted'] > 0:
        stats['take_on_success_rate'] = (stats['take_ons_successful'] / stats['take_ons_attempted']) * 100
    else:
        stats['take_on_success_rate'] = 0.0
    
    return stats

def aggregate_passing_types_stats(df) -> dict:
    """Aggregate passing types statistics."""
    # This could include pass types, crosses, corners, etc.
    # For now, we'll extract basic crossing data if available
    stats = {}
    
    # Try to get crossing data if columns exist
    try:
        if ('Crosses', 'Crs') in df.columns:
            stats['crosses'] = safe_numeric(df[('Crosses', 'Crs')].sum())
        if ('Corner Kicks', 'CK') in df.columns:
            stats['corner_kicks'] = safe_numeric(df[('Corner Kicks', 'CK')].sum())
    except:
        pass
    
    return stats

def insert_team_stats_to_db(match_id: str, team_id: str, team_stats: dict, db_path: str):
    """Insert aggregated team stats into match_team_comprehensive table."""
    
    conn = sqlite3.connect(db_path)
    
    try:
        # Generate team stats ID
        team_stats_id = generate_team_stats_id(match_id, team_id)
        
        # Prepare insert statement
        insert_sql = """
        INSERT OR REPLACE INTO match_team_comprehensive (
            team_stats_id, match_id, team_id,
            goals, assists, shots, shots_on_target, yellow_cards, red_cards,
            passes_completed, passes_attempted, pass_accuracy,
            short_passes_completed, short_passes_attempted,
            medium_passes_completed, medium_passes_attempted,
            long_passes_completed, long_passes_attempted,
            progressive_passes, key_passes, passes_into_final_third,
            touches, touches_def_pen, touches_def_3rd, touches_mid_3rd,
            touches_att_3rd, touches_att_pen,
            carries, carries_distance, progressive_carries,
            take_ons_attempted, take_ons_successful,
            tackles, tackles_won, tackles_def_3rd, tackles_mid_3rd, tackles_att_3rd,
            interceptions, blocks, blocks_shots, blocks_passes, clearances,
            fouls, fouled, offsides, corners,
            aerials_won, aerials_lost,
            pass_accuracy_short, pass_accuracy_medium, pass_accuracy_long,
            take_on_success_rate, tackle_success_rate, aerial_win_rate
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Extract values in correct order
        values = (
            team_stats_id, match_id, team_id,
            team_stats.get('goals', 0), team_stats.get('assists', 0),
            team_stats.get('shots', 0), team_stats.get('shots_on_target', 0),
            team_stats.get('yellow_cards', 0), team_stats.get('red_cards', 0),
            team_stats.get('passes_completed', 0), team_stats.get('passes_attempted', 0),
            team_stats.get('pass_accuracy', 0.0),
            team_stats.get('short_passes_completed', 0), team_stats.get('short_passes_attempted', 0),
            team_stats.get('medium_passes_completed', 0), team_stats.get('medium_passes_attempted', 0),
            team_stats.get('long_passes_completed', 0), team_stats.get('long_passes_attempted', 0),
            team_stats.get('progressive_passes', 0), team_stats.get('key_passes', 0),
            team_stats.get('passes_into_final_third', 0),
            team_stats.get('touches', 0), team_stats.get('touches_def_pen', 0),
            team_stats.get('touches_def_3rd', 0), team_stats.get('touches_mid_3rd', 0),
            team_stats.get('touches_att_3rd', 0), team_stats.get('touches_att_pen', 0),
            team_stats.get('carries', 0), team_stats.get('carries_distance', 0),
            team_stats.get('progressive_carries', 0),
            team_stats.get('take_ons_attempted', 0), team_stats.get('take_ons_successful', 0),
            team_stats.get('tackles', 0), team_stats.get('tackles_won', 0),
            team_stats.get('tackles_def_3rd', 0), team_stats.get('tackles_mid_3rd', 0),
            team_stats.get('tackles_att_3rd', 0),
            team_stats.get('interceptions', 0), team_stats.get('blocks', 0),
            team_stats.get('blocks_shots', 0), team_stats.get('blocks_passes', 0),
            team_stats.get('clearances', 0),
            team_stats.get('fouls', 0), team_stats.get('fouled', 0),
            team_stats.get('offsides', 0), team_stats.get('corners', 0),
            team_stats.get('aerials_won', 0), team_stats.get('aerials_lost', 0),
            team_stats.get('pass_accuracy_short', 0.0), team_stats.get('pass_accuracy_medium', 0.0),
            team_stats.get('pass_accuracy_long', 0.0),
            team_stats.get('take_on_success_rate', 0.0), team_stats.get('tackle_success_rate', 0.0),
            team_stats.get('aerial_win_rate', 0.0)
        )
        
        conn.execute(insert_sql, values)
        conn.commit()
        
        logging.info(f"✅ Inserted team stats: {match_id} - {team_id}")
        
    except Exception as e:
        logging.error(f"❌ Error inserting {match_id} - {team_id}: {e}")
        conn.rollback()
    
    finally:
        conn.close()

def process_complete_matches(complete_matches: list, tables_dir: str, db_path: str):
    """Process all complete matches and insert into database."""
    
    processed_count = 0
    error_count = 0
    
    logging.info(f"Processing {len(complete_matches)} complete matches...")
    
    for i, match_id in enumerate(complete_matches, 1):
        logging.info(f"[{i}/{len(complete_matches)}] Processing match {match_id}")
        
        # Find team IDs for this match
        match_dir = Path(tables_dir) / match_id
        team_ids = set()
        
        for stat_file in match_dir.glob(f"{match_id}_stats_*_summary.csv"):
            # Extract team_id from filename
            parts = stat_file.stem.split('_')
            if len(parts) >= 3:
                team_id = parts[2]
                team_ids.add(team_id)
        
        if len(team_ids) != 2:
            logging.warning(f"⚠️  Match {match_id} has {len(team_ids)} teams, expected 2")
            error_count += 1
            continue
        
        # Process each team
        for team_id in team_ids:
            try:
                # Aggregate team stats from CSVs
                team_stats = aggregate_team_stats_from_csvs(match_id, team_id, tables_dir)
                
                if team_stats:
                    # Insert into database
                    insert_team_stats_to_db(match_id, team_id, team_stats, db_path)
                    processed_count += 1
                else:
                    logging.warning(f"No stats aggregated for {match_id} - {team_id}")
                    error_count += 1
                    
            except Exception as e:
                logging.error(f"Error processing {match_id} - {team_id}: {e}")
                error_count += 1
    
    logging.info(f"✅ Processing complete!")
    logging.info(f"✅ Successfully processed: {processed_count} team records")
    logging.info(f"❌ Errors: {error_count}")
    
    return processed_count, error_count

if __name__ == "__main__":
    
    # Configuration
    tables_dir = "/Users/thomasmcmillan/projects/nwsl_data/notebooks/match_html_files/tables"
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    
    # Read complete matches from previous analysis
    complete_matches_file = "match_completeness_analysis.txt"
    
    if not os.path.exists(complete_matches_file):
        logging.error(f"Complete matches file not found: {complete_matches_file}")
        logging.error("Please run analyze_match_completeness.py first")
        exit(1)
    
    # Parse complete matches from file
    complete_matches = []
    with open(complete_matches_file, 'r') as f:
        in_complete_section = False
        for line in f:
            line = line.strip()
            if line == "COMPLETE MATCHES:":
                in_complete_section = True
                continue
            elif line.startswith("INCOMPLETE MATCHES"):
                break
            elif in_complete_section and line and not line.startswith('-'):
                complete_matches.append(line)
    
    logging.info(f"Found {len(complete_matches)} complete matches to process")
    
    # Process complete matches
    if complete_matches:
        processed, errors = process_complete_matches(complete_matches, tables_dir, db_path)
        
        logging.info(f"\n🎉 FINAL SUMMARY:")
        logging.info(f"📊 Complete matches available: {len(complete_matches)}")  
        logging.info(f"✅ Team records processed: {processed}")
        logging.info(f"❌ Processing errors: {errors}")
        logging.info(f"📈 Success rate: {(processed/(len(complete_matches)*2))*100:.1f}%")
    else:
        logging.error("No complete matches found to process")