#!/usr/bin/env python3
"""
Script to create match_player records from HTML files for orphaned matches.

This script extracts player rosters from FBRef HTML files and creates
match_player records for matches that exist in the match table but have
no associated player data.
"""

import sqlite3
import sys
import uuid
from pathlib import Path

from bs4 import BeautifulSoup


def create_match_player_records(match_id):
    """Extract players from HTML file and create match_player records."""

    # Database connection
    db_path = "data/processed/nwsldata.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # HTML file path
    html_file = f"/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files/match_{match_id}.html"

    print(f"Processing match {match_id}...")

    if not Path(html_file).exists():
        print(f"Error: HTML file not found for match {match_id}: {html_file}")
        conn.close()
        return False

    print(f"Reading HTML file: {html_file}")

    # Read and parse HTML
    with open(html_file, encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), "html.parser")

    # Find summary tables (these contain player data)
    summary_tables = soup.find_all("table", {"class": "stats_table"})
    summary_tables = [table for table in summary_tables if "summary" in table.get("id", "")]

    print(f"Found {len(summary_tables)} summary tables")

    if not summary_tables:
        print(f"No summary tables found in {html_file}")
        conn.close()
        return False

    match_player_records = []
    players_added = 0
    players_skipped = 0

    for table in summary_tables:
        table_id = table.get("id", "")
        print(f"\nProcessing table: {table_id}")

        # Extract team ID from table ID (e.g., stats_bf961da0_summary -> bf961da0)
        team_id = table_id.replace("stats_", "").replace("_summary", "")

        # Find all player rows
        tbody = table.find("tbody")
        if not tbody:
            continue

        rows = tbody.find_all("tr")

        for row in rows:
            # Skip non-player rows
            if "spacer" in row.get("class", []):
                continue

            # Get player data
            player_cell = row.find("th", {"data-stat": "player"})
            if not player_cell:
                continue

            player_link = player_cell.find("a")
            if not player_link:
                continue

            # Extract FBRef player ID from href
            href = player_link.get("href", "")
            if "/players/" not in href:
                continue

            fbref_player_id = href.split("/players/")[1].split("/")[0]
            player_name = player_link.text.strip()

            # Check if player exists in our player table
            cursor.execute("SELECT player_id FROM player WHERE player_id = ?", (fbref_player_id,))
            if not cursor.fetchone():
                print(f"Warning: Player {player_name} (ID: {fbref_player_id}) not found in player table - skipping")
                players_skipped += 1
                continue

            # Get minutes played
            minutes_cell = row.find("td", {"data-stat": "minutes"})
            minutes_played = (
                int(minutes_cell.text.strip()) if minutes_cell and minutes_cell.text.strip().isdigit() else 0
            )

            # Create match_player record
            match_player_id = f"mp_{uuid.uuid4().hex[:8]}"

            match_player_records.append(
                {
                    "match_player_id": match_player_id,
                    "match_id": match_id,
                    "player_id": fbref_player_id,
                    "team_id": team_id,
                    "minutes_played": minutes_played,
                    "player_name": player_name,
                }
            )

            players_added += 1

    print(f"\nExtracted data for {players_added} players")

    # Insert match_player records
    if match_player_records:
        for record in match_player_records:
            try:
                cursor.execute(
                    """
                    INSERT INTO match_player (match_player_id, match_id, player_id, player_name, team_id, minutes_played)
                    VALUES (?, ?, ?, ?, ?, ?)
                """,
                    (
                        record["match_player_id"],
                        record["match_id"],
                        record["player_id"],
                        record["player_name"],
                        record["team_id"],
                        record["minutes_played"],
                    ),
                )
                print(f"Added match_player record for {record['player_name']} (ID: {record['match_player_id']})")
            except sqlite3.Error as e:
                print(f"Error inserting record for {record['player_name']}: {e}")
                players_skipped += 1
                players_added -= 1

    # Commit changes
    conn.commit()
    conn.close()

    print(f"\nMatch {match_id} processing complete:")
    print(f"  - Records added: {players_added}")
    print(f"  - Records skipped: {players_skipped}")

    return True


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python create_match_player_records.py <match_id>")
        sys.exit(1)

    match_id = sys.argv[1]
    success = create_match_player_records(match_id)

    if success:
        print(f"Successfully processed match {match_id}")
    else:
        print(f"Failed to process match {match_id}")
