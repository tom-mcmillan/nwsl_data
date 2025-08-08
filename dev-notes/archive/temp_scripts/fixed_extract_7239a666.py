#!/usr/bin/env python3
"""
FIXED extraction for match 7239a666 with correct shirt_number and minutes_played mapping
"""

import sqlite3
import uuid

import pandas as pd
from bs4 import BeautifulSoup


def fixed_extract_7239a666():
    """Re-extract match 7239a666 with FIXED field mapping"""

    match_id = "7239a666"
    html_path = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files/match_7239a666.html"
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"

    print(f"🔧 FIXED extraction for match: {match_id}")

    # Read HTML
    with open(html_path, encoding="utf-8") as f:
        html_content = f.read()

    soup = BeautifulSoup(html_content, "html.parser")

    # Find summary tables
    summary_tables = ["stats_6f666306_summary", "stats_df9a10a1_summary"]

    all_players = []

    for table_id in summary_tables:
        team_id = table_id.split("_")[1]
        print(f"\n🔍 Processing team: {team_id}")

        table = soup.find("table", id=table_id)
        if not table:
            continue

        # Convert to DataFrame
        df = pd.read_html(str(table))[0]

        # Handle MultiIndex columns with FIXED mapping
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ["_".join(col).strip() if col[1] else col[0] for col in df.columns.values]

        print(f"  Processing {len(df)} rows")

        valid_players = 0
        for _idx, row in df.iterrows():
            player_name = str(row.iloc[0]).strip()

            # Filter out invalid rows and team totals
            if (
                pd.isna(player_name)
                or player_name == ""
                or player_name.lower() in ["player", "nan"]
                or "Players" in player_name
            ):
                continue

            # Extract with FIXED column mapping
            basic_player = {
                "match_id": match_id,
                "player_name": player_name,
                "team_id": team_id,
                "shirt_number": safe_extract_int(row, ["Unnamed: 1_level_0_#"]),  # FIXED
                "minutes_played": safe_extract_int(row, ["Unnamed: 5_level_0_Min"]),  # FIXED
            }

            all_players.append(basic_player)
            valid_players += 1
            print(f"    ✅ {player_name} (#{basic_player['shirt_number']}, {basic_player['minutes_played']} min)")

        print(f"  Extracted {valid_players} players with FIXED fields")

    print("\n📊 FIXED EXTRACTION SUMMARY:")
    print(f"Total players: {len(all_players)}")

    # Show sample with actual values
    if all_players:
        sample = all_players[0]
        print(f"Sample player: {sample['player_name']}")
        print(f"  Shirt number: {sample['shirt_number']}")
        print(f"  Minutes: {sample['minutes_played']}")

    # Re-insert with fixed data
    if all_players:
        success = reinsert_fixed_roster(all_players, db_path)
        return success

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


def reinsert_fixed_roster(players_data, db_path):
    """Re-insert roster data with fixed field values"""

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        match_id = players_data[0]["match_id"]

        # Delete existing data for this match
        cursor.execute("DELETE FROM match_player WHERE match_id = ?", (match_id,))
        print(f"🗑️  Deleted existing records for match {match_id}")

        inserted_count = 0

        for player in players_data:
            # Generate proper match_player_id
            match_player_id = f"mp_{uuid.uuid4().hex[:8]}"

            # Resolve existing player_id
            cursor.execute("SELECT player_id FROM player WHERE player_name = ?", (player["player_name"],))
            result = cursor.fetchone()
            existing_player_id = result[0] if result else None

            # Insert with FIXED fields
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
                player["shirt_number"],  # Now has actual values
                player["minutes_played"],  # Now has actual values
            )

            cursor.execute(insert_sql, values)
            inserted_count += 1

        conn.commit()
        conn.close()

        print(f"✅ Re-inserted {inserted_count} players with FIXED fields")
        return True

    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def verify_fixed_data(match_id, db_path):
    """Verify the fixed data has actual shirt numbers and minutes"""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT player_name, shirt_number, minutes_played, team_id
        FROM match_player 
        WHERE match_id = ? 
        ORDER BY team_id, shirt_number
        LIMIT 10
    """,
        (match_id,),
    )

    results = cursor.fetchall()

    print("\n🔍 VERIFICATION OF FIXED DATA:")
    print("Sample players with shirt numbers and minutes:")

    for name, shirt, minutes, team in results:
        print(f"  {name} | #{shirt} | {minutes} min | Team: {team[:8]}...")

    # Count non-null values
    cursor.execute(
        """
        SELECT 
            COUNT(*) as total,
            COUNT(shirt_number) as with_shirt_number,
            COUNT(minutes_played) as with_minutes
        FROM match_player WHERE match_id = ?
    """,
        (match_id,),
    )

    total, with_shirt, with_minutes = cursor.fetchone()

    print("\nField completeness:")
    print(f"  Total players: {total}")
    print(f"  With shirt numbers: {with_shirt}/{total} ({with_shirt/total*100:.1f}%)")
    print(f"  With minutes played: {with_minutes}/{total} ({with_minutes/total*100:.1f}%)")

    conn.close()


if __name__ == "__main__":
    success = fixed_extract_7239a666()

    if success:
        verify_fixed_data("7239a666", "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db")
        print("\n🎉 SUCCESS: Match 7239a666 re-extracted with FIXED shirt numbers and minutes!")
    else:
        print("\n💥 FAILED: Could not fix extraction")
