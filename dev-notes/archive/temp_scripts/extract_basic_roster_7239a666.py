#!/usr/bin/env python3
"""
Extract basic roster data for match 7239a666 into simplified match_player table
Only extracting: match_id, player_id, player_name, team_id, shirt_number, minutes_played
"""

import os
import sqlite3
import uuid

import pandas as pd
from bs4 import BeautifulSoup


def extract_basic_roster_7239a666():
    """Extract basic roster info for match 7239a666"""

    match_id = "7239a666"
    html_path = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files/match_7239a666.html"
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"

    print(f"🎯 Extracting basic roster for match: {match_id}")
    print(f"📄 HTML file: match_{match_id}.html")

    # Check if HTML file exists
    if not os.path.exists(html_path):
        print(f"❌ HTML file not found: {html_path}")
        return False

    # Read HTML
    with open(html_path, encoding="utf-8") as f:
        html_content = f.read()

    soup = BeautifulSoup(html_content, "html.parser")

    # Find all tables to identify teams
    tables = soup.find_all("table")
    table_ids = [table.get("id") for table in tables if table.get("id")]

    # Find summary tables for both teams
    summary_tables = [tid for tid in table_ids if "stats_" in tid and "_summary" in tid]

    if not summary_tables:
        print("❌ No summary tables found")
        return False

    print(f"✅ Found {len(summary_tables)} summary tables: {summary_tables}")

    all_players = []

    # Process each team's summary table
    for table_id in summary_tables:
        team_id = table_id.split("_")[1]  # Extract team_id from stats_{team_id}_summary
        print(f"\n🔍 Processing team: {team_id}")

        table = soup.find("table", id=table_id)
        if not table:
            continue

        try:
            # Convert to DataFrame
            df = pd.read_html(str(table))[0]

            # Handle MultiIndex columns
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ["_".join(col).strip() if col[1] else col[0] for col in df.columns.values]

            print(f"  Table has {len(df)} rows")

            # Process each player row
            valid_players = 0
            for _idx, row in df.iterrows():
                player_name = str(row.iloc[0]).strip()

                # Filter out invalid rows and team totals
                if (
                    pd.isna(player_name)
                    or player_name == ""
                    or player_name.lower() in ["player", "nan"]
                    or "Players" in player_name  # Filter out "16 Players", etc.
                    or "Player" == player_name
                ):
                    print(f"    ⏭️  Skipping: {player_name}")
                    continue

                # Extract basic roster info only
                basic_player = {
                    "match_id": match_id,
                    "player_name": player_name,
                    "team_id": team_id,
                    "shirt_number": safe_extract_int(row, ["#", "Unnamed: 1_level_0"]),
                    "minutes_played": safe_extract_int(row, ["Min", "Unnamed: 5_level_0"]),
                }

                all_players.append(basic_player)
                valid_players += 1
                print(
                    f"    ✅ {player_name} (#{basic_player.get('shirt_number', '?')}, {basic_player.get('minutes_played', '?')} min)"
                )

            print(f"  Extracted {valid_players} valid players from team {team_id}")

        except Exception as e:
            print(f"  ❌ Error processing team {team_id}: {e}")

    print("\n📊 TOTAL EXTRACTION SUMMARY:")
    print(f"Valid players extracted: {len(all_players)}")

    # Insert into simplified match_player table
    if all_players:
        success = insert_basic_roster(all_players, db_path)
        return success
    else:
        print("❌ No players extracted")
        return False


def safe_extract_int(row, possible_columns):
    """Safely extract integer value from row"""
    for col in possible_columns:
        if col in row.index:
            value = row[col]
            if pd.notna(value) and str(value).strip() != "":
                try:
                    return int(float(str(value).replace(",", "")))
                except:
                    continue
    return None


def insert_basic_roster(players_data, db_path):
    """Insert basic roster data into simplified match_player table"""

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Check if match already has data
        match_id = players_data[0]["match_id"]
        cursor.execute("SELECT COUNT(*) FROM match_player WHERE match_id = ?", (match_id,))
        existing_count = cursor.fetchone()[0]

        if existing_count > 0:
            print(f"⚠️  Match {match_id} already has {existing_count} player records")
            response = input("Delete existing and re-insert? (y/n): ")
            if response.lower() != "y":
                conn.close()
                return False
            cursor.execute("DELETE FROM match_player WHERE match_id = ?", (match_id,))

        inserted_count = 0

        for player in players_data:
            # Generate proper match_player_id: mp_ + 8 hex chars
            match_player_id = f"mp_{uuid.uuid4().hex[:8]}"

            # Try to resolve existing player_id by name
            cursor.execute("SELECT player_id FROM player WHERE player_name = ?", (player["player_name"],))
            result = cursor.fetchone()
            existing_player_id = result[0] if result else None

            if existing_player_id:
                print(f"    🔗 Found player_id for {player['player_name']}: {existing_player_id}")

            # Insert basic roster data
            insert_sql = """
                INSERT INTO match_player (
                    match_player_id, match_id, player_id, player_name, team_id, 
                    shirt_number, minutes_played
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """

            values = (
                match_player_id,
                player["match_id"],
                existing_player_id,
                player["player_name"],
                player["team_id"],
                player["shirt_number"],
                player["minutes_played"],
            )

            cursor.execute(insert_sql, values)
            inserted_count += 1

        conn.commit()
        conn.close()

        print(f"✅ Successfully inserted {inserted_count} players into match_player table")
        return True

    except Exception as e:
        print(f"❌ Database insertion error: {e}")
        return False


def verify_insertion(match_id, db_path):
    """Verify the basic roster was inserted correctly"""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT match_player_id, player_id, player_name, team_id, shirt_number, minutes_played
        FROM match_player 
        WHERE match_id = ? 
        ORDER BY team_id, shirt_number
    """,
        (match_id,),
    )

    results = cursor.fetchall()

    print("\n🔍 INSERTION VERIFICATION:")
    print(f"Total records inserted: {len(results)}")

    # Group by team
    team_counts = {}
    linked_players = 0

    for row in results:
        mp_id, p_id, name, t_id, shirt, minutes = row
        if t_id not in team_counts:
            team_counts[t_id] = 0
        team_counts[t_id] += 1

        if p_id:
            linked_players += 1

    print(f"Players by team: {team_counts}")
    print(f"Players with existing player_id links: {linked_players}/{len(results)}")

    # Show sample data
    print("\nSample roster data:")
    for row in results[:8]:
        mp_id, p_id, name, t_id, shirt, minutes = row
        print(f"  {mp_id} | {name} | #{shirt} | {minutes}min | Team:{t_id[:8]}...")

    conn.close()


if __name__ == "__main__":
    success = extract_basic_roster_7239a666()

    if success:
        verify_insertion("7239a666", "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db")
        print("\n🎉 SUCCESS: Basic roster extracted for match 7239a666!")
    else:
        print("\n💥 FAILED: Could not extract roster for match 7239a666")
