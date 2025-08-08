#!/usr/bin/env python3
"""
Add 2013 Team Summary Data
Process 2013 matches using only the basic fields available in that era.
"""

import hashlib
import logging
import sqlite3
from pathlib import Path

import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def generate_match_team_id(match_id: str, team_id: str) -> str:
    """Generate unique match team ID."""
    content = f"team_summary_{match_id}_{team_id}"
    hex_hash = hashlib.md5(content.encode()).hexdigest()[:8]
    return f"mt_{hex_hash}"


def safe_int(value, default=0):
    """Safely convert value to int."""
    if pd.isna(value) or value == "" or value is None:
        return default
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def extract_2013_keeper_stats(keeper_csv_path: str) -> dict:
    """Extract goalkeeper totals from 2013 keeper CSV file."""
    try:
        # Read the CSV file
        df = pd.read_csv(keeper_csv_path, header=[0, 1])

        # Sum up all goalkeeper stats (in case multiple keepers played)
        keeper_stats = {}
        keeper_stats["shots_on_target_against"] = 0
        keeper_stats["saves"] = 0

        # Process each keeper row (data starts at index 0 after headers)
        for idx in range(len(df)):
            if pd.notna(df.iloc[idx, 0]) and str(df.iloc[idx, 0]).strip():
                # Columns: Player,Nation,Age,Min,SoTA,GA,Saves,Save%
                keeper_stats["shots_on_target_against"] += safe_int(df.iloc[idx, 4])  # SoTA
                keeper_stats["saves"] += safe_int(df.iloc[idx, 6])  # Saves

        # Calculate save percentage
        if keeper_stats["shots_on_target_against"] > 0:
            keeper_stats["save_percentage"] = round(
                (keeper_stats["saves"] / keeper_stats["shots_on_target_against"]) * 100, 1
            )
        else:
            keeper_stats["save_percentage"] = 0.0

        return keeper_stats

    except Exception as e:
        logging.error(f"Error reading keeper file {keeper_csv_path}: {e}")
        return {"shots_on_target_against": 0, "saves": 0, "save_percentage": 0.0}


def extract_2013_team_stats(csv_file_path: str) -> dict:
    """Extract team totals from 2013 summary CSV file."""
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file_path, header=[0, 1])

        # Find the totals row - look for "Players" in first column
        totals_row = None
        for idx in range(len(df) - 1, -1, -1):  # Start from the end
            first_col_val = str(df.iloc[idx, 0]).strip()
            if "Players" in first_col_val:
                totals_row = df.iloc[idx]
                break

        if totals_row is None:
            logging.warning(f"No totals row found in {csv_file_path}")
            return {}

        # Extract 2013 statistics
        # Columns: Player,#,Nation,Pos,Age,Min,Gls,Ast,PK,PKatt,Sh,SoT,CrdY,CrdR,Fls,Fld,Off,Crs,...
        team_stats = {}

        try:
            team_stats["goals"] = safe_int(totals_row.iloc[6])  # Gls
            team_stats["assists"] = safe_int(totals_row.iloc[7])  # Ast
            team_stats["penalty_goals"] = safe_int(totals_row.iloc[8])  # PK
            team_stats["penalty_attempts"] = safe_int(totals_row.iloc[9])  # PKatt
            team_stats["shots"] = safe_int(totals_row.iloc[10])  # Sh
            team_stats["shots_on_target"] = safe_int(totals_row.iloc[11])  # SoT
            team_stats["yellow_cards"] = safe_int(totals_row.iloc[12])  # CrdY
            team_stats["red_cards"] = safe_int(totals_row.iloc[13])  # CrdR
            team_stats["fouls"] = safe_int(totals_row.iloc[14])  # Fls
            team_stats["fouled"] = safe_int(totals_row.iloc[15])  # Fld
            team_stats["offsides"] = safe_int(totals_row.iloc[16])  # Off
            team_stats["corners"] = safe_int(totals_row.iloc[17])  # Crs

        except Exception as e:
            logging.error(f"Error extracting stats from {csv_file_path}: {e}")
            return {}

        return team_stats

    except Exception as e:
        logging.error(f"Error reading {csv_file_path}: {e}")
        return {}


def get_2013_matches(db_path: str) -> list:
    """Get all 2013 matches from database."""
    conn = sqlite3.connect(db_path)

    query = """
    SELECT match_id, match_date 
    FROM match 
    WHERE match_date LIKE '2013%' 
    ORDER BY match_date
    """

    matches = conn.execute(query).fetchall()
    conn.close()

    return matches


def process_2013_matches(tables_dir: str, db_path: str):
    """Process 2013 matches and add to match_team_summary table."""

    # Get 2013 matches
    matches_2013 = get_2013_matches(db_path)
    logging.info(f"Found {len(matches_2013)} matches from 2013")

    tables_path = Path(tables_dir)
    processed_count = 0
    error_count = 0

    conn = sqlite3.connect(db_path)

    for match_id, match_date in matches_2013:
        match_dir = tables_path / match_id

        if not match_dir.exists():
            logging.warning(f"No CSV directory for match {match_id}")
            continue

        # Find summary CSV files for this match
        summary_files = list(match_dir.glob(f"{match_id}_stats_*_summary.csv"))

        if not summary_files:
            logging.warning(f"No summary files for match {match_id}")
            continue

        logging.info(f"Processing 2013 match {match_id} on {match_date} ({len(summary_files)} teams)")

        for summary_file in summary_files:
            # Extract team_id from filename
            parts = summary_file.stem.split("_")
            if len(parts) >= 3:
                team_id = parts[2]
            else:
                logging.warning(f"Could not extract team_id from {summary_file}")
                continue

            try:
                # Extract team statistics
                team_stats = extract_2013_team_stats(summary_file)

                if not team_stats:
                    error_count += 1
                    continue

                # Extract goalkeeper statistics
                keeper_file = match_dir / f"{match_id}_keeper_stats_{team_id}.csv"
                if keeper_file.exists():
                    keeper_stats = extract_2013_keeper_stats(keeper_file)
                    team_stats.update(keeper_stats)
                else:
                    # Default values if no keeper file
                    team_stats["shots_on_target_against"] = 0
                    team_stats["saves"] = 0
                    team_stats["save_percentage"] = 0.0

                # Generate match team ID
                match_team_id = generate_match_team_id(match_id, team_id)

                # Get team name
                team_name_query = "SELECT team_name FROM team WHERE team_id = ?"
                team_name_result = conn.execute(team_name_query, (team_id,)).fetchone()
                team_name = team_name_result[0] if team_name_result else None

                # Insert into database
                insert_sql = """
                INSERT OR REPLACE INTO match_team_summary (
                    match_team_id, match_id, team_id, team_name, match_date,
                    goals, assists, penalty_goals, penalty_attempts,
                    shots, shots_on_target, yellow_cards, red_cards,
                    fouls, fouled, offsides, corners,
                    shots_on_target_against, saves, save_percentage
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """

                values = (
                    match_team_id,
                    match_id,
                    team_id,
                    team_name,
                    match_date,
                    team_stats.get("goals", 0),
                    team_stats.get("assists", 0),
                    team_stats.get("penalty_goals", 0),
                    team_stats.get("penalty_attempts", 0),
                    team_stats.get("shots", 0),
                    team_stats.get("shots_on_target", 0),
                    team_stats.get("yellow_cards", 0),
                    team_stats.get("red_cards", 0),
                    team_stats.get("fouls", 0),
                    team_stats.get("fouled", 0),
                    team_stats.get("offsides", 0),
                    team_stats.get("corners", 0),
                    team_stats.get("shots_on_target_against", 0),
                    team_stats.get("saves", 0),
                    team_stats.get("save_percentage", 0.0),
                )

                conn.execute(insert_sql, values)
                processed_count += 1

            except Exception as e:
                logging.error(f"Error processing {summary_file}: {e}")
                error_count += 1

    conn.commit()
    conn.close()

    logging.info(f"✅ Processed {processed_count} team records from 2013")
    logging.info(f"❌ Errors: {error_count}")

    return processed_count, error_count


def validate_2013_data(db_path: str):
    """Validate the 2013 data."""
    conn = sqlite3.connect(db_path)

    # Get 2013 statistics
    summary_sql = """
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT match_id) as unique_matches,
        COUNT(DISTINCT team_id) as unique_teams,
        SUM(goals) as total_goals,
        SUM(shots) as total_shots,
        SUM(corners) as total_corners,
        ROUND(AVG(goals), 2) as avg_goals_per_team,
        ROUND(AVG(shots), 1) as avg_shots_per_team
    FROM match_team_summary 
    WHERE match_date LIKE '2013%';
    """

    summary = conn.execute(summary_sql).fetchone()

    logging.info("📊 2013 SEASON STATISTICS:")
    logging.info(f"   Team records: {summary[0]}")
    logging.info(f"   Matches: {summary[1]}")
    logging.info(f"   Teams: {summary[2]}")
    logging.info(f"   Total goals: {summary[3]}")
    logging.info(f"   Total shots: {summary[4]}")
    logging.info(f"   Total corners: {summary[5]}")
    logging.info(f"   Avg goals per team: {summary[6]}")
    logging.info(f"   Avg shots per team: {summary[7]}")

    # Show sample data
    sample_sql = """
    SELECT match_id, team_id, match_date, goals, shots, shots_on_target, saves, shots_on_target_against, save_percentage
    FROM match_team_summary 
    WHERE match_date LIKE '2013%' AND goals > 0
    ORDER BY goals DESC
    LIMIT 5;
    """

    sample_data = conn.execute(sample_sql).fetchall()

    logging.info("📋 TOP 2013 PERFORMANCES:")
    for row in sample_data:
        logging.info(
            f"   Match {row[0][:8]} on {row[2]}: {row[3]} goals, {row[4]} shots ({row[5]} on target), {row[6]} saves/{row[7]} shots against ({row[8]}%)"
        )

    conn.close()


if __name__ == "__main__":
    # Configuration
    tables_dir = "/Users/thomasmcmillan/projects/nwsl_data/notebooks/match_html_files/tables"
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"

    logging.info("🚀 ADDING 2013 TEAM SUMMARY DATA")
    logging.info("=" * 50)

    # Process 2013 matches
    processed, errors = process_2013_matches(tables_dir, db_path)

    if processed > 0:
        # Validate data
        validate_2013_data(db_path)

        logging.info("=" * 50)
        logging.info("🎉 SUCCESS! Added 2013 team summary data")
        logging.info(f"📊 {processed} team records added for 2013 season")
    else:
        logging.error("❌ No 2013 data was processed successfully")
