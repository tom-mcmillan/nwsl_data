#!/usr/bin/env python3
"""
Fixed extractor for match 008e301f that properly handles MultiIndex columns
"""

import sqlite3
import uuid

import pandas as pd
from bs4 import BeautifulSoup


def extract_and_insert_008e301f():
    """Extract and insert player data for match 008e301f with proper MultiIndex handling"""

    match_id = "008e301f"
    html_path = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files/match_008e301f.html"
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"

    print(f"🎯 Processing match {match_id} with FIXED MultiIndex handling")

    # Read HTML
    with open(html_path, encoding="utf-8") as f:
        html_content = f.read()

    soup = BeautifulSoup(html_content, "html.parser")

    # Find summary tables
    summary_table_ids = ["stats_257fad2b_summary", "stats_8e306dc6_summary"]

    all_players = []

    for table_id in summary_table_ids:
        print(f"\n🔍 Processing table: {table_id}")
        table = soup.find("table", id=table_id)

        if table:
            # Extract team_id from table_id
            team_id = table_id.split("_")[1]

            # Convert to DataFrame
            df = pd.read_html(str(table))[0]

            # Handle MultiIndex columns properly
            if isinstance(df.columns, pd.MultiIndex):
                print("✅ Found MultiIndex columns - flattening...")
                # Flatten MultiIndex: ('Performance', 'Gls') -> 'Performance_Gls'
                df.columns = ["_".join(col).strip() if col[1] else col[0] for col in df.columns.values]

            print(f"Columns after flattening: {list(df.columns)[:10]}...")  # Show first 10

            # Process each player row
            players_extracted = 0
            for _idx, row in df.iterrows():
                player_name = str(row.iloc[0]).strip()  # First column is player name

                # Skip invalid rows
                if pd.isna(player_name) or player_name == "" or player_name.lower() in ["player", "nan"]:
                    continue

                # Extract stats with proper column names
                player_data = {
                    "match_id": match_id,
                    "player_name": player_name,
                    "team_id": team_id,
                    # Basic info - try different possible column names
                    "shirt_number": safe_extract(row, ["#", "Unnamed: 1_level_0"]),
                    "position": safe_extract(row, ["Pos", "Unnamed: 3_level_0"]),
                    "minutes_played": safe_extract(row, ["Min", "Unnamed: 5_level_0"]),
                    # Performance stats with MultiIndex names
                    "summary_perf_gls": safe_extract(row, ["Performance_Gls", "Gls"]),
                    "summary_perf_ast": safe_extract(row, ["Performance_Ast", "Ast"]),
                    "summary_perf_pk": safe_extract(row, ["Performance_PK", "PK"]),
                    "summary_perf_sh": safe_extract(row, ["Performance_Sh", "Sh"]),
                    "summary_perf_sot": safe_extract(row, ["Performance_SoT", "SoT"]),
                    "summary_perf_crdy": safe_extract(row, ["Performance_CrdY", "CrdY"]),
                    "summary_perf_crdr": safe_extract(row, ["Performance_CrdR", "CrdR"]),
                    "summary_perf_touches": safe_extract(row, ["Performance_Touches", "Touches"]),
                    "summary_perf_tkl": safe_extract(row, ["Performance_Tkl", "Tkl"]),
                    "summary_perf_int": safe_extract(row, ["Performance_Int", "Int"]),
                    "summary_perf_blocks": safe_extract(row, ["Performance_Blocks", "Blocks"]),
                    # Expected stats
                    "summary_exp_xg": safe_extract(row, ["Expected_xG", "xG"]),
                    "summary_exp_npxg": safe_extract(row, ["Expected_npxG", "npxG"]),
                    "summary_exp_xag": safe_extract(row, ["Expected_xAG", "xAG"]),
                    # SCA/GCA
                    "summary_sca_sca": safe_extract(row, ["SCA_SCA", "SCA"]),
                    "summary_sca_gca": safe_extract(row, ["SCA_GCA", "GCA"]),
                    # Passing
                    "summary_pass_cmp": safe_extract(row, ["Passes_Cmp", "Cmp"]),
                    "summary_pass_att": safe_extract(row, ["Passes_Att", "Att"]),
                    "summary_pass_cmp_pct": safe_extract(row, ["Passes_Cmp%", "Cmp%"]),
                    "summary_pass_prgp": safe_extract(row, ["Passes_PrgP", "PrgP"]),
                    # Carries & Take-ons
                    "summary_carry_carries": safe_extract(row, ["Carries_Carries", "Carries"]),
                    "summary_carry_prgc": safe_extract(row, ["Carries_PrgC", "PrgC"]),
                    "summary_take_att": safe_extract(row, ["Take-Ons_Att", "Take-Ons Att"]),
                    "summary_take_succ": safe_extract(row, ["Take-Ons_Succ", "Take-Ons Succ"]),
                }

                all_players.append(player_data)
                players_extracted += 1

                # Show sample data for first player
                if players_extracted == 1:
                    print(f"Sample player: {player_name}")
                    populated = {k: v for k, v in player_data.items() if v is not None}
                    print(f"  Goals: {player_data.get('summary_perf_gls')}")
                    print(f"  xG: {player_data.get('summary_exp_xg')}")
                    print(f"  Minutes: {player_data.get('minutes_played')}")
                    print(f"  Total populated fields: {len(populated)}")

            print(f"✅ Extracted {players_extracted} players from {table_id}")

    print(f"\n💾 Inserting {len(all_players)} players into database...")

    # Clear existing data
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM match_player WHERE match_id = ?", (match_id,))

    # Insert new data
    inserted_count = 0
    for player in all_players:
        success = insert_player_with_stats(cursor, player)
        if success:
            inserted_count += 1

    conn.commit()
    conn.close()

    print(f"✅ Successfully inserted {inserted_count} players with actual stats!")

    # Verify the data
    verify_insertion(match_id, db_path)


def safe_extract(row, possible_columns):
    """Safely extract value from row using multiple possible column names"""
    for col in possible_columns:
        if col in row.index:
            value = row[col]
            if pd.notna(value) and str(value).strip() != "":
                try:
                    # Try to convert to appropriate type
                    if "." in str(value):
                        return float(value)
                    elif str(value).isdigit():
                        return int(value)
                    else:
                        return str(value).strip()
                except:
                    return str(value).strip()
    return None


def insert_player_with_stats(cursor, player):
    """Insert player with comprehensive stats"""
    try:
        # Generate unique ID
        match_player_id = str(uuid.uuid4())[:8]

        # Resolve season_id
        cursor.execute("SELECT season_id FROM match WHERE match_id = ?", (player["match_id"],))
        result = cursor.fetchone()
        season_id = result[0] if result else None

        # Insert with all available fields
        insert_sql = """
            INSERT INTO match_player (
                match_player_id, match_id, player_id, player_name, team_id, team_name,
                shirt_number, position, minutes_played, season_id,
                
                -- Performance stats
                summary_perf_gls, summary_perf_ast, summary_perf_pk, summary_perf_sh,
                summary_perf_sot, summary_perf_crdy, summary_perf_crdr, summary_perf_touches,
                summary_perf_tkl, summary_perf_int, summary_perf_blocks,
                
                -- Expected stats
                summary_exp_xg, summary_exp_npxg, summary_exp_xag,
                
                -- SCA/GCA
                summary_sca_sca, summary_sca_gca,
                
                -- Passing
                summary_pass_cmp, summary_pass_att, summary_pass_cmp_pct, summary_pass_prgp,
                
                -- Carries & Take-ons
                summary_carry_carries, summary_carry_prgc, summary_take_att, summary_take_succ
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        values = (
            match_player_id,
            player["match_id"],
            None,  # player_id
            player["player_name"],
            player["team_id"],
            None,  # team_name
            player["shirt_number"],
            player["position"],
            player["minutes_played"],
            season_id,
            # Performance
            player["summary_perf_gls"],
            player["summary_perf_ast"],
            player["summary_perf_pk"],
            player["summary_perf_sh"],
            player["summary_perf_sot"],
            player["summary_perf_crdy"],
            player["summary_perf_crdr"],
            player["summary_perf_touches"],
            player["summary_perf_tkl"],
            player["summary_perf_int"],
            player["summary_perf_blocks"],
            # Expected
            player["summary_exp_xg"],
            player["summary_exp_npxg"],
            player["summary_exp_xag"],
            # SCA/GCA
            player["summary_sca_sca"],
            player["summary_sca_gca"],
            # Passing
            player["summary_pass_cmp"],
            player["summary_pass_att"],
            player["summary_pass_cmp_pct"],
            player["summary_pass_prgp"],
            # Carries & Take-ons
            player["summary_carry_carries"],
            player["summary_carry_prgc"],
            player["summary_take_att"],
            player["summary_take_succ"],
        )

        cursor.execute(insert_sql, values)
        return True

    except Exception as e:
        print(f"❌ Error inserting {player['player_name']}: {e}")
        return False


def verify_insertion(match_id, db_path):
    """Verify the data was inserted correctly"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT player_name, summary_perf_gls, summary_perf_ast, summary_exp_xg, minutes_played 
        FROM match_player 
        WHERE match_id = ? AND (summary_perf_gls > 0 OR summary_exp_xg > 0)
        ORDER BY summary_perf_gls DESC, summary_exp_xg DESC
    """,
        (match_id,),
    )

    results = cursor.fetchall()

    print("\n🎉 VERIFICATION - Players with goals or xG > 0:")
    for player_name, goals, assists, xg, minutes in results:
        print(f"  {player_name}: {goals} goals, {assists} assists, {xg} xG, {minutes} mins")

    if not results:
        print("⚠️  No players found with goals or xG > 0 - checking all data...")
        cursor.execute("SELECT COUNT(*), AVG(minutes_played) FROM match_player WHERE match_id = ?", (match_id,))
        count, avg_mins = cursor.fetchone()
        print(f"  Total players: {count}, Average minutes: {avg_mins}")

    conn.close()


if __name__ == "__main__":
    extract_and_insert_008e301f()
