#!/usr/bin/env python3
"""
Script to populate match_player_summary table with 2018 statistics from HTML files.
Adapted for 2018's reduced field set (24 fields vs 37 in modern seasons).

Usage: python populate_match_player_summary_2018.py <match_id>
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
    """Extract player statistics from a summary table - 2018 version."""
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
        
        # Map 2018 table columns to our database fields
        column_mapping_2018 = {
            'shirtnumber': 'shirt_number',
            'position': 'position', 
            'age': 'age',
            'minutes': 'minutes_played',
            'goals': 'goals',
            'assists': 'assists',
            'pens_made': 'penalty_kicks',
            'pens_att': 'penalty_kicks_attempted',
            'shots': 'shots',
            'shots_on_target': 'shots_on_target',
            'cards_yellow': 'yellow_cards',
            'cards_red': 'red_cards',
            'fouls': 'fouls_committed',
            'fouled': 'fouls_drawn',
            'offsides': 'offsides',
            'crosses': 'crosses',
            'tackles_won': 'tackles',
            'interceptions': 'interceptions',
            'own_goals': 'own_goals',
            'pens_won': 'penalties_won',
            'pens_conceded': 'penalties_conceded'
        }
        
        # Extract values from each cell
        for td in tds:
            data_stat = td.get('data-stat')
            if data_stat and data_stat in column_mapping_2018:
                db_field = column_mapping_2018[data_stat]
                value = td.text.strip()
                
                # Handle empty values and convert to appropriate types
                if value == '':
                    stats[db_field] = None
                elif data_stat in ['minutes_played', 'goals', 'assists', 'penalty_kicks', 
                                 'penalty_kicks_attempted', 'shots', 'shots_on_target',
                                 'yellow_cards', 'red_cards', 'fouls_committed', 'fouls_drawn',
                                 'offsides', 'crosses', 'tackles', 'interceptions', 'own_goals',
                                 'penalties_won', 'penalties_conceded', 'shirt_number']:
                    stats[db_field] = int(value) if value else 0
                elif data_stat in ['age']:
                    # Handle age format like "24-307" - take just the year part
                    if value and '-' in value:
                        stats[db_field] = int(value.split('-')[0])
                    else:
                        stats[db_field] = int(value) if value else None
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
    SELECT mp.match_player_id, mp.player_name, mp.player_id, mp.season_id,
           CASE WHEN mps.match_player_id IS NOT NULL THEN 1 ELSE 0 END as has_summary
    FROM match_player mp
    LEFT JOIN match_player_summary mps ON mp.match_player_id = mps.match_player_id
    WHERE mp.match_id = ?
    ORDER BY has_summary DESC
    """
    
    cursor = conn.execute(query, (match_id,))
    results = cursor.fetchall()
    
    # Create mapping from player_id (which should be FBRef ID) to match_player_id
    fbref_to_match_player = {}
    for match_player_id, player_name, player_id, season_id, has_summary in results:
        if player_id:
            if player_id not in fbref_to_match_player or has_summary:
                fbref_to_match_player[player_id] = {
                    'match_player_id': match_player_id,
                    'player_name': player_name,
                    'player_id': player_id,
                    'season_id': season_id
                }
    
    return fbref_to_match_player

def populate_match_player_summary_2018(conn, match_id, players_data, fbref_mapping):
    """Update the match_player_summary table with 2018 statistical data."""
    
    # Prepare update statement for 2018 fields only
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
        tackles = ?,
        interceptions = ?,
        position = ?,
        age = ?
    WHERE match_player_summary_id = ?
    """
    
    records_updated = 0
    records_skipped = 0
    
    for player_data in players_data:
        fbref_id = player_data['fbref_player_id']
        player_name = player_data['player_name']
        stats = player_data['stats']
        
        if fbref_id in fbref_mapping:
            match_player_info = fbref_mapping[fbref_id]
            
            # Find corresponding match_player_summary record
            query = """
            SELECT match_player_summary_id 
            FROM match_player_summary mps
            JOIN match_player mp ON mps.match_player_id = mp.match_player_id
            WHERE mp.match_id = ? AND mp.player_id = ?
            """
            
            cursor = conn.execute(query, (match_id, fbref_id))
            result = cursor.fetchone()
            
            if result:
                match_player_summary_id = result[0]
                
                # Prepare values for update - only 2018 available fields
                values = [
                    stats.get('minutes_played', 0),
                    stats.get('goals', 0),
                    stats.get('assists', 0), 
                    stats.get('penalty_kicks', 0),
                    stats.get('penalty_kicks_attempted', 0),
                    stats.get('shots', 0),
                    stats.get('shots_on_target', 0),
                    stats.get('yellow_cards', 0),
                    stats.get('red_cards', 0),
                    stats.get('tackles', 0),
                    stats.get('interceptions', 0),
                    stats.get('position', None),
                    stats.get('age', None),
                    match_player_summary_id
                ]
                
                conn.execute(update_query, values)
                records_updated += 1
                print(f"Updated stats for {player_name} (ID: {match_player_summary_id})")
            else:
                records_skipped += 1
                print(f"No match_player_summary record found for {player_name} (FBRef: {fbref_id})")
        else:
            records_skipped += 1
            print(f"Player {player_name} (FBRef: {fbref_id}) not found in match_player mapping")
    
    conn.commit()
    return records_updated, records_skipped

def main():
    if len(sys.argv) != 2:
        print("Usage: python populate_match_player_summary_2018.py <match_id>")
        sys.exit(1)
    
    match_id = sys.argv[1]
    
    print(f"Processing match {match_id}...")
    
    try:
        # Get HTML file path
        html_file_path = get_html_file_path(match_id)
        print(f"Reading HTML file: {html_file_path}")
        
        # Parse HTML
        soup = parse_html_file(html_file_path)
        
        # Find summary tables
        summary_tables = find_summary_tables(soup)
        print(f"Found {len(summary_tables)} summary tables")
        
        if not summary_tables:
            print(f"No summary tables found for match {match_id}")
            return
        
        # Get database connection
        conn = get_database_connection()
        
        # Get match player mappings
        fbref_mapping = get_match_player_ids(conn, match_id)
        print(f"Found {len(fbref_mapping)} match_player records with FBRef IDs")
        
        total_records_updated = 0
        total_records_skipped = 0
        
        # Process each summary table
        for i, table in enumerate(summary_tables):
            table_id = table.get('id', f'table_{i}')
            print(f"\nProcessing table: {table_id}")
            
            # Extract player data
            players_data = extract_player_stats(table)
            print(f"Extracted data for {len(players_data)} players")
            
            # Update database
            updated, skipped = populate_match_player_summary_2018(conn, match_id, players_data, fbref_mapping)
            total_records_updated += updated
            total_records_skipped += skipped
        
        conn.close()
        
        print(f"\nMatch {match_id} processing complete:")
        print(f"  - Records updated: {total_records_updated}")
        print(f"  - Records skipped: {total_records_skipped}")
        print(f"\nSuccessfully processed match {match_id}")
        
    except Exception as e:
        print(f"Error processing match {match_id}: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()