#!/usr/bin/env python3
"""
Script to populate match_player_summary table with statistics from HTML files.

Usage: python populate_match_player_summary.py <match_id>
"""

import sqlite3
import sys
import os
from bs4 import BeautifulSoup
import re

def get_database_connection():
    """Get connection to the NWSL database."""
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    return sqlite3.connect(db_path)

def get_html_file_path(match_id):
    """Get the path to the HTML file for a given match_id."""
    html_dir = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files"
    html_file = os.path.join(html_dir, f"match_{match_id}.html")
    
    if not os.path.exists(html_file):
        raise FileNotFoundError(f"HTML file not found for match {match_id}: {html_file}")
    
    return html_file

def parse_html_file(html_file_path):
    """Parse the HTML file and return BeautifulSoup object."""
    with open(html_file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    return BeautifulSoup(content, 'html.parser')

def find_summary_tables(soup):
    """Find all summary tables in the HTML (stats_{team_id}_summary)."""
    # Find tables with id pattern stats_*_summary
    summary_tables = soup.find_all('table', id=re.compile(r'stats_[a-f0-9]+_summary'))
    return summary_tables

def extract_player_stats(table):
    """Extract player statistics from a summary table."""
    players_data = []
    
    # Find all player rows (tbody tr elements)
    tbody = table.find('tbody')
    if not tbody:
        return players_data
    
    rows = tbody.find_all('tr')
    
    for row in rows:
        # Skip team total rows (they don't have data-append-csv)
        player_cell = row.find('th', {'data-append-csv': True})
        if not player_cell:
            continue
        
        # Extract FBRef player ID
        fbref_player_id = player_cell.get('data-append-csv')
        
        # Extract player name
        player_link = player_cell.find('a')
        player_name = player_link.text.strip() if player_link else player_cell.text.strip()
        
        # Extract all statistical data
        stats = {}
        tds = row.find_all('td')
        
        # Map table columns to our database fields
        column_mapping = {
            'shirtnumber': 'shirt_number',
            'position': 'position',
            'age': 'age',
            'minutes': 'minutes_played',
            'goals': 'goals',
            'assists': 'assists',
            'pens_made': 'penalty_kicks_made',
            'pens_att': 'penalty_kicks_attempted',
            'shots': 'shots_total',
            'shots_on_target': 'shots_on_target',
            'cards_yellow': 'yellow_cards',
            'cards_red': 'red_cards',
            'touches': 'touches',
            'tackles': 'tackles',
            'interceptions': 'interceptions',
            'blocks': 'blocks',
            'xg': 'expected_goals',
            'npxg': 'non_penalty_expected_goals',
            'xg_assist': 'expected_assisted_goals',
            'sca': 'shot_creating_actions',
            'gca': 'goal_creating_actions',
            'passes_completed': 'passes_completed',
            'passes': 'passes_attempted',
            'passes_pct': 'pass_completion_percentage',
            'progressive_passes': 'progressive_passes',
            'carries': 'carries',
            'progressive_carries': 'progressive_carries',
            'take_ons': 'take_ons_attempted',
            'take_ons_won': 'take_ons_successful'
        }
        
        # Extract values from each cell
        for td in tds:
            data_stat = td.get('data-stat')
            if data_stat and data_stat in column_mapping:
                db_field = column_mapping[data_stat]
                value = td.text.strip()
                
                # Handle empty values and convert to appropriate types
                if value == '' or value == '0.0' and data_stat == 'age':
                    stats[db_field] = None
                elif data_stat in ['minutes_played', 'goals', 'assists', 'penalty_kicks_made', 
                                 'penalty_kicks_attempted', 'shots_total', 'shots_on_target',
                                 'yellow_cards', 'red_cards', 'touches', 'tackles', 
                                 'interceptions', 'blocks', 'shot_creating_actions',
                                 'goal_creating_actions', 'passes_completed', 'passes_attempted',
                                 'progressive_passes', 'carries', 'progressive_carries',
                                 'take_ons_attempted', 'take_ons_successful', 'shirt_number']:
                    stats[db_field] = int(value) if value else 0
                elif data_stat in ['expected_goals', 'non_penalty_expected_goals', 
                                 'expected_assisted_goals', 'pass_completion_percentage']:
                    stats[db_field] = float(value) if value else 0.0
                else:
                    stats[db_field] = value
        
        player_data = {
            'fbref_player_id': fbref_player_id,
            'player_name': player_name,
            'stats': stats
        }
        
        players_data.append(player_data)
    
    return players_data

def get_match_player_ids(conn, match_id):
    """Get match_player_id mappings for players in this match."""
    query = """
    SELECT mp.match_player_id, mp.player_name, mp.player_id, mp.season_id
    FROM match_player mp
    WHERE mp.match_id = ?
    """
    
    cursor = conn.execute(query, (match_id,))
    results = cursor.fetchall()
    
    # Create mapping from player_id (which should be FBRef ID) to match_player_id
    fbref_to_match_player = {}
    for match_player_id, player_name, player_id, season_id in results:
        if player_id:
            fbref_to_match_player[player_id] = {
                'match_player_id': match_player_id,
                'player_name': player_name,
                'player_id': player_id,
                'season_id': season_id
            }
    
    return fbref_to_match_player

def populate_match_player_summary(conn, match_id, players_data, fbref_mapping):
    """Update the match_player_summary table with extracted statistical data."""
    
    # Prepare update statement using correct column names from the table
    update_query = """
    UPDATE match_player_summary SET
        minutes_played = ?,
        goals = ?,
        assists = ?,
        penalty_kicks = ?,
        penalty_kicks_attempted = ?,
        shots = ?,
        shots_on_target = ?,
        yellow_cards = ?,
        red_cards = ?,
        touches = ?,
        tackles = ?,
        interceptions = ?,
        blocks = ?,
        xg = ?,
        npxg = ?,
        xag = ?,
        sca = ?,
        gca = ?,
        passes_completed = ?,
        passes_attempted = ?,
        pass_completion_pct = ?,
        progressive_passes = ?,
        carries = ?,
        progressive_carries = ?,
        take_ons_attempted = ?,
        take_ons_successful = ?,
        position = ?,
        age = ?,
        shirt_number = ?,
        season_id = ?
    WHERE match_player_summary_id = ?
    """
    
    updated_count = 0
    skipped_count = 0
    
    for player_data in players_data:
        fbref_id = player_data['fbref_player_id']
        
        if fbref_id not in fbref_mapping:
            print(f"Warning: No match_player record found for FBRef ID {fbref_id} ({player_data['player_name']})")
            skipped_count += 1
            continue
        
        match_player_id = fbref_mapping[fbref_id]['match_player_id']
        season_id = fbref_mapping[fbref_id]['season_id']
        
        # Generate match_player_summary_id from match_player_id
        match_player_summary_id = 'mps_' + match_player_id[3:]  # Replace 'mp_' with 'mps_'
        
        stats = player_data['stats']
        
        # Prepare values in the correct order (matching UPDATE statement)
        values = (
            stats.get('minutes_played', 0),
            stats.get('goals', 0),
            stats.get('assists', 0),
            stats.get('penalty_kicks_made', 0), 
            stats.get('penalty_kicks_attempted', 0),
            stats.get('shots_total', 0),
            stats.get('shots_on_target', 0),
            stats.get('yellow_cards', 0),
            stats.get('red_cards', 0),
            stats.get('touches', 0),
            stats.get('tackles', 0),
            stats.get('interceptions', 0),
            stats.get('blocks', 0),
            stats.get('expected_goals', 0.0),
            stats.get('non_penalty_expected_goals', 0.0),
            stats.get('expected_assisted_goals', 0.0),
            stats.get('shot_creating_actions', 0),
            stats.get('goal_creating_actions', 0),
            stats.get('passes_completed', 0),
            stats.get('passes_attempted', 0),
            stats.get('pass_completion_percentage', 0.0),
            stats.get('progressive_passes', 0),
            stats.get('carries', 0),
            stats.get('progressive_carries', 0),
            stats.get('take_ons_attempted', 0),
            stats.get('take_ons_successful', 0),
            stats.get('position', ''),
            stats.get('age', ''),
            stats.get('shirt_number', 0),
            season_id,  # season_id field
            match_player_summary_id  # WHERE clause parameter
        )
        
        try:
            cursor = conn.execute(update_query, values)
            if cursor.rowcount > 0:
                updated_count += 1
                print(f"Updated stats for {player_data['player_name']} (ID: {match_player_summary_id})")
            else:
                print(f"No record found to update for {player_data['player_name']} (ID: {match_player_summary_id})")
                skipped_count += 1
        except sqlite3.Error as e:
            print(f"Error updating stats for {player_data['player_name']}: {e}")
            skipped_count += 1
    
    return updated_count, skipped_count

def process_match(match_id):
    """Process a single match and populate its player summary statistics."""
    
    print(f"Processing match {match_id}...")
    
    try:
        # Get HTML file path
        html_file = get_html_file_path(match_id)
        print(f"Reading HTML file: {html_file}")
        
        # Parse HTML
        soup = parse_html_file(html_file)
        
        # Find summary tables
        summary_tables = find_summary_tables(soup)
        print(f"Found {len(summary_tables)} summary tables")
        
        if not summary_tables:
            print(f"No summary tables found for match {match_id}")
            return False
        
        # Get database connection
        conn = get_database_connection()
        
        # Get match_player mappings
        fbref_mapping = get_match_player_ids(conn, match_id)
        print(f"Found {len(fbref_mapping)} match_player records with FBRef IDs")
        
        total_inserted = 0
        total_skipped = 0
        
        # Process each summary table (one per team)
        for i, table in enumerate(summary_tables):
            table_id = table.get('id', f'table_{i}')
            print(f"\nProcessing table: {table_id}")
            
            # Extract player data
            players_data = extract_player_stats(table)
            print(f"Extracted data for {len(players_data)} players")
            
            if players_data:
                # Update database
                updated, skipped = populate_match_player_summary(
                    conn, match_id, players_data, fbref_mapping
                )
                total_inserted += updated
                total_skipped += skipped
        
        # Commit changes
        conn.commit()
        conn.close()
        
        print(f"\nMatch {match_id} processing complete:")
        print(f"  - Records updated: {total_inserted}")
        print(f"  - Records skipped: {total_skipped}")
        
        return True
        
    except Exception as e:
        print(f"Error processing match {match_id}: {e}")
        return False

def main():
    """Main function."""
    if len(sys.argv) != 2:
        print("Usage: python populate_match_player_summary.py <match_id>")
        sys.exit(1)
    
    match_id = sys.argv[1]
    
    success = process_match(match_id)
    
    if success:
        print(f"\nSuccessfully processed match {match_id}")
        sys.exit(0)
    else:
        print(f"\nFailed to process match {match_id}")
        sys.exit(1)

if __name__ == "__main__":
    main()