#!/usr/bin/env python3
"""
Restore match 008e301f data properly with correct ID format and existing player linkages
"""

import sqlite3
import uuid

import pandas as pd
from bs4 import BeautifulSoup


def restore_008e301f_properly():
    """Restore match 008e301f with proper ID structure and no fake players"""

    match_id = "008e301f"
    html_path = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files/match_008e301f.html"
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"

    print(f"🔧 RESTORING match {match_id} with proper data integrity")

    # Read HTML
    with open(html_path, encoding="utf-8") as f:
        html_content = f.read()

    soup = BeautifulSoup(html_content, "html.parser")

    # Get team IDs and summary tables
    team_ids = ["257fad2b", "8e306dc6"]  # From previous analysis

    all_players = []

    for team_id in team_ids:
        print(f"\n🔍 Processing team: {team_id}")

        # Get summary table only (start conservative)
        summary_table = soup.find("table", id=f"stats_{team_id}_summary")

        if summary_table:
            df = pd.read_html(str(summary_table))[0]

            # Handle MultiIndex columns
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ["_".join(col).strip() if col[1] else col[0] for col in df.columns.values]

            print(f"  Found summary table with {len(df)} rows")

            # Process each player row
            for _idx, row in df.iterrows():
                player_name = str(row.iloc[0]).strip()

                # CRITICAL: Filter out team totals and invalid rows
                if (
                    pd.isna(player_name)
                    or player_name == ""
                    or player_name.lower() in ["player", "nan"]
                    or "Players" in player_name  # This filters out "15 Players", "14 Players" etc.
                    or "Player" == player_name
                ):
                    print(f"    ⏭️  Skipping invalid/team total row: {player_name}")
                    continue

                print(f"    ✅ Valid player: {player_name}")

                # Extract basic stats
                player_data = {
                    "match_id": match_id,
                    "player_name": player_name,
                    "team_id": team_id,
                    # Basic info with safe extraction
                    "shirt_number": safe_extract_int(row, ["#", "Unnamed: 1_level_0"]),
                    "position": safe_extract_str(row, ["Pos", "Unnamed: 3_level_0"]),
                    "minutes_played": safe_extract_int(row, ["Min", "Unnamed: 5_level_0"]),
                    # Core performance stats
                    "summary_perf_gls": safe_extract_int(row, ["Performance_Gls", "Gls"]),
                    "summary_perf_ast": safe_extract_int(row, ["Performance_Ast", "Ast"]),
                    "summary_perf_sh": safe_extract_int(row, ["Performance_Sh", "Sh"]),
                    "summary_perf_sot": safe_extract_int(row, ["Performance_SoT", "SoT"]),
                    "summary_exp_xg": safe_extract_float(row, ["Expected_xG", "xG"]),
                }

                all_players.append(player_data)

    print("\n📊 VALIDATION:")
    print(f"Total valid players extracted: {len(all_players)}")

    # Show all player names for verification
    player_names = [p["player_name"] for p in all_players]
    print(f"Player names: {player_names}")

    # Check for any suspicious entries
    suspicious = [name for name in player_names if "Player" in name or any(char.isdigit() for char in name.split()[0])]
    if suspicious:
        print(f"⚠️  SUSPICIOUS ENTRIES FOUND: {suspicious}")
        return False

    # Insert with proper ID structure
    print("\n💾 Inserting with proper ID structure...")
    insert_with_proper_ids(all_players, db_path)

    return True


def safe_extract_int(row, possible_columns):
    """Safely extract integer value"""
    for col in possible_columns:
        if col in row.index:
            value = row[col]
            if pd.notna(value) and str(value).strip() != "":
                try:
                    return int(float(str(value).replace(",", "")))
                except:
                    continue
    return None


def safe_extract_float(row, possible_columns):
    """Safely extract float value"""
    for col in possible_columns:
        if col in row.index:
            value = row[col]
            if pd.notna(value) and str(value).strip() != "":
                try:
                    return float(str(value).replace(",", ""))
                except:
                    continue
    return None


def safe_extract_str(row, possible_columns):
    """Safely extract string value"""
    for col in possible_columns:
        if col in row.index:
            value = row[col]
            if pd.notna(value) and str(value).strip() != "":
                return str(value).strip()
    return None


def insert_with_proper_ids(players_data, db_path):
    """Insert players with proper mp_ ID format and preserve existing player_ids"""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    inserted_count = 0

    for player in players_data:
        try:
            # Generate proper match_player_id in correct format: mp_ + 8 hex chars
            match_player_id = f"mp_{uuid.uuid4().hex[:8]}"

            # Try to find existing player_id by name
            cursor.execute("SELECT player_id FROM player WHERE player_name = ?", (player["player_name"],))
            result = cursor.fetchone()
            existing_player_id = result[0] if result else None

            if existing_player_id:
                print(f"    🔗 Found existing player_id for {player['player_name']}: {existing_player_id}")
            else:
                print(f"    🆕 No existing player_id for {player['player_name']}")

            # Get season_id
            cursor.execute("SELECT season_id FROM match WHERE match_id = ?", (player["match_id"],))
            result = cursor.fetchone()
            season_id = result[0] if result else None

            # Insert with conservative field set
            insert_sql = """
                INSERT INTO match_player (
                    match_player_id, match_id, player_id, player_name, team_id, 
                    shirt_number, position, minutes_played, season_id,
                    summary_perf_gls, summary_perf_ast, summary_perf_sh, summary_perf_sot, summary_exp_xg
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            values = (
                match_player_id,
                player["match_id"],
                existing_player_id,  # Use existing player_id if found
                player["player_name"],
                player["team_id"],
                player["shirt_number"],
                player["position"],
                player["minutes_played"],
                season_id,
                player["summary_perf_gls"],
                player["summary_perf_ast"],
                player["summary_perf_sh"],
                player["summary_perf_sot"],
                player["summary_exp_xg"],
            )

            cursor.execute(insert_sql, values)
            inserted_count += 1

            print(f"    ✅ Inserted: {player['player_name']} (ID: {match_player_id})")

        except Exception as e:
            print(f"    ❌ Error inserting {player['player_name']}: {e}")

    conn.commit()
    conn.close()

    print(f"\n✅ Successfully restored {inserted_count} valid players")


def verify_restoration(match_id, db_path):
    """Verify the restoration was successful"""

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check the restored data
    cursor.execute(
        """
        SELECT match_player_id, player_id, player_name, summary_perf_gls, summary_exp_xg
        FROM match_player 
        WHERE match_id = ? 
        ORDER BY player_name
    """,
        (match_id,),
    )

    results = cursor.fetchall()

    print("\n🔍 RESTORATION VERIFICATION:")
    print(f"Total records: {len(results)}")

    # Check ID formats
    proper_mp_ids = [r[0] for r in results if r[0].startswith("mp_") and len(r[0]) == 11]
    print(f"Proper mp_ IDs: {len(proper_mp_ids)}/{len(results)}")

    # Check for linked player_ids
    linked_players = [r for r in results if r[1] is not None]
    print(f"Players with existing player_id links: {len(linked_players)}")

    # Check for suspicious names
    suspicious_names = [r[2] for r in results if "Player" in r[2] and r[2] != "Player"]
    if suspicious_names:
        print(f"⚠️  SUSPICIOUS NAMES STILL PRESENT: {suspicious_names}")
    else:
        print("✅ No suspicious player names found")

    # Show sample of restored data
    print("\nSample restored data:")
    for row in results[:5]:
        mp_id, p_id, name, goals, xg = row
        print(f"  {mp_id} | {p_id} | {name} | Goals: {goals} | xG: {xg}")

    conn.close()


if __name__ == "__main__":
    success = restore_008e301f_properly()

    if success:
        verify_restoration("008e301f", "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db")
        print("\n🎉 RESTORATION COMPLETE")
    else:
        print("\n💥 RESTORATION FAILED - suspicious data detected")
