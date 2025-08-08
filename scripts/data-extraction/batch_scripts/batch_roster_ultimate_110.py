#!/usr/bin/env python3
"""
Batch roster extraction for 110 ULTIMATE historical match IDs
Using the proven FIXED methodology from previous successful extractions
Building towards the ULTIMATE 12+ SEASON comprehensive NWSL database!
"""

import logging
import os
import sqlite3
import time
import uuid
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/Users/thomasmcmillan/projects/nwsl_data/ultimate_110_extraction.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# List of 110 ULTIMATE historical match IDs to process
ULTIMATE_110_MATCH_IDS = [
    "04d023e7",
    "d6aec4b1",
    "2fd9c845",
    "99453465",
    "f740ebaf",
    "6ed85fb8",
    "be1600aa",
    "e2a1b418",
    "66645e5e",
    "b3aab324",
    "9e8abda1",
    "db3aa3b0",
    "e3a360d3",
    "188f52c0",
    "35003fd2",
    "74564b15",
    "807c06e4",
    "1186ea77",
    "f6ec53f0",
    "2d5af9d6",
    "edfa82d4",
    "2ac75a24",
    "7f0a506f",
    "9a0ab07f",
    "f0b59875",
    "70f1e21b",
    "62cdbc09",
    "f2a8f16b",
    "63378f00",
    "38402333",
    "b118c20b",
    "eed6455b",
    "83cdf3e1",
    "ac3f3db5",
    "17061441",
    "c6df790b",
    "25049986",
    "19c171d0",
    "9f613a73",
    "a68c37d0",
    "a23cf271",
    "f31a9d7d",
    "fbe77a1f",
    "22486374",
    "43103b88",
    "7014113e",
    "890a4ab4",
    "dbca8ac5",
    "5ba2eba7",
    "b9ab40c2",
    "059fa801",
    "0bfa24ae",
    "c0417d1e",
    "2a83a3f6",
    "3ac4acb5",
    "bce76cd8",
    "66ca439b",
    "48d47dc9",
    "fa063275",
    "9feb3b94",
    "a45da2df",
    "6195a47a",
    "b6b656ae",
    "19e8a4b9",
    "77e8defe",
    "581cef24",
    "79fde10f",
    "a41a3eeb",
    "1a248e02",
    "69b3b00d",
    "47d5955b",
    "e03ef57f",
    "14529941",
    "8e775a64",
    "04480428",
    "4d031196",
    "dae49ac4",
    "6863d0dc",
    "30540f3f",
    "c00ea6a4",
    "a459e5c0",
    "c9c09e02",
    "56a01c6b",
    "5dd078d5",
    "5dd549e4",
    "70461c41",
    "f6a0f221",
    "0a10a72f",
    "e9e2fcf9",
    "92a0b833",
    "3fded0a5",
    "af4d51ac",
    "dcd5f391",
    "9ce230c1",
    "b0355b88",
    "e8af8ca3",
    "9830bc25",
    "812788e2",
    "21d682a9",
    "a835491d",
    "43d092f2",
    "f59d77ca",
    "dd2c669d",
    "e0dc82d9",
    "8b97808b",
    "0c35f2f9",
    "c8ac28b2",
    "ee9cc30e",
    "217a40ab",
    "f881e2c1",
    "60f738e9",
]


class Ultimate110RosterExtractor:
    """Extract roster data for 110 ULTIMATE historical matches using proven methodology"""

    def __init__(self):
        self.html_dir = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files"
        self.db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"

        # Tracking
        self.processed_matches = 0
        self.failed_matches = []
        self.successful_matches = []
        self.total_players_extracted = 0
        self.start_time = None

    def process_all_matches(self):
        """Process all 110 ULTIMATE historical match IDs"""

        self.start_time = time.time()

        logger.info("🚀 Starting ULTIMATE 110 roster extraction")
        logger.info(f"📁 HTML directory: {self.html_dir}")
        logger.info(f"💾 Database: {self.db_path}")
        logger.info(f"📄 Total matches to process: {len(ULTIMATE_110_MATCH_IDS)}")
        logger.info("🎯 Building towards ULTIMATE 12+ SEASON comprehensive NWSL database!")
        logger.info("🏆 This could complete the MOST COMPREHENSIVE NWSL DATABASE EVER!")

        # Check seasons and existing data
        self.analyze_matches()

        # Check which matches already have data
        existing_matches = self.check_existing_data()

        matches_to_process = []
        for match_id in ULTIMATE_110_MATCH_IDS:
            if match_id in existing_matches:
                logger.info(f"⏭️  Skipping {match_id} - already has {existing_matches[match_id]} players")
            else:
                matches_to_process.append(match_id)

        logger.info(f"📝 Matches to process: {len(matches_to_process)}")
        logger.info(f"⏭️  Matches to skip: {len(ULTIMATE_110_MATCH_IDS) - len(matches_to_process)}")

        if not matches_to_process:
            logger.info("✅ All matches already processed!")
            return self.generate_summary()

        # Process each match
        logger.info(f"\n{'='*60}")
        logger.info(f"STARTING ULTIMATE 110 EXTRACTION - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")

        for i, match_id in enumerate(matches_to_process, 1):
            self.process_single_match(match_id, i, len(matches_to_process))

        return self.generate_final_summary()

    def analyze_matches(self):
        """Analyze what seasons these matches belong to"""

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        placeholders = ",".join(["?" for _ in ULTIMATE_110_MATCH_IDS])
        cursor.execute(
            f"""
            SELECT s.season_year, COUNT(*) as match_count
            FROM match m 
            JOIN season s ON m.season_id = s.season_id 
            WHERE m.match_id IN ({placeholders})
            GROUP BY s.season_year 
            ORDER BY s.season_year
        """,
            ULTIMATE_110_MATCH_IDS,
        )

        season_breakdown = cursor.fetchall()

        logger.info("📊 Season breakdown of ULTIMATE 110 matches:")
        total_found = 0
        for season_year, count in season_breakdown:
            logger.info(f"  {season_year}: {count} matches")
            total_found += count

        if total_found < len(ULTIMATE_110_MATCH_IDS):
            logger.warning(f"⚠️  Only {total_found}/{len(ULTIMATE_110_MATCH_IDS)} matches found in database")

        conn.close()

    def check_existing_data(self):
        """Check which matches already have roster data"""

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Get matches that already have roster data
        placeholders = ",".join(["?" for _ in ULTIMATE_110_MATCH_IDS])
        cursor.execute(
            f"""
            SELECT match_id, COUNT(*) as player_count
            FROM match_player 
            WHERE match_id IN ({placeholders})
            GROUP BY match_id
        """,
            ULTIMATE_110_MATCH_IDS,
        )

        existing_data = {row[0]: row[1] for row in cursor.fetchall()}

        conn.close()

        logger.info("📊 Existing data check:")
        logger.info(f"  Matches with roster data: {len(existing_data)}/{len(ULTIMATE_110_MATCH_IDS)}")

        return existing_data

    def process_single_match(self, match_id, current, total):
        """Process a single match using FIXED methodology"""

        html_path = os.path.join(self.html_dir, f"match_{match_id}.html")

        logger.info(f"[{current}/{total}] Processing {match_id}...")

        try:
            # Check if HTML file exists
            if not os.path.exists(html_path):
                logger.error(f"❌ HTML file not found: {html_path}")
                self.failed_matches.append((match_id, "HTML file not found"))
                return

            # Extract roster data using FIXED methodology
            players = self.extract_roster_fixed(html_path, match_id)

            if players:
                # Insert into database
                if self.insert_roster_data(players):
                    self.successful_matches.append(match_id)
                    self.total_players_extracted += len(players)
                    logger.info(f"✅ Success! Extracted {len(players)} players")
                else:
                    self.failed_matches.append((match_id, "Database insertion failed"))
            else:
                self.failed_matches.append((match_id, "No players extracted"))

        except Exception as e:
            logger.error(f"❌ Error processing {match_id}: {str(e)}")
            self.failed_matches.append((match_id, str(e)))

        self.processed_matches += 1

        # Progress update every 25 matches
        if current % 25 == 0:
            elapsed = time.time() - self.start_time
            rate = current / elapsed * 60  # matches per minute
            remaining = (total - current) / rate if rate > 0 else 0

            logger.info(f"📊 Progress: {current}/{total} ({current/total*100:.1f}%)")
            logger.info(f"⏱️  Rate: {rate:.1f} matches/min, Est. remaining: {remaining:.1f} min")
            logger.info(f"✅ Success: {len(self.successful_matches)}, ❌ Failed: {len(self.failed_matches)}")

    def extract_roster_fixed(self, html_path, match_id):
        """Extract roster using FIXED methodology"""

        # Read HTML
        with open(html_path, encoding="utf-8") as f:
            html_content = f.read()

        soup = BeautifulSoup(html_content, "html.parser")

        # Find all tables to identify summary tables
        tables = soup.find_all("table")
        table_ids = [table.get("id") for table in tables if table.get("id")]

        # Find summary tables
        summary_tables = [tid for tid in table_ids if "stats_" in tid and "_summary" in tid]

        if not summary_tables:
            logger.warning(f"⚠️  No summary tables found for {match_id}")
            return []

        all_players = []

        # Process each team's summary table
        for table_id in summary_tables:
            team_id = table_id.split("_")[1]
            table = soup.find("table", id=table_id)

            if not table:
                continue

            # Convert to DataFrame
            df = pd.read_html(str(table))[0]

            # Handle MultiIndex columns with FIXED mapping
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ["_".join(col).strip() if col[1] else col[0] for col in df.columns.values]

            # Process each player row
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

                # Extract using FIXED column mapping
                player_data = {
                    "match_id": match_id,
                    "player_name": player_name,
                    "team_id": team_id,
                    "shirt_number": self.safe_extract_int(row, ["Unnamed: 1_level_0_#"]),
                    "minutes_played": self.safe_extract_int(row, ["Unnamed: 5_level_0_Min"]),
                }

                all_players.append(player_data)

        return all_players

    def safe_extract_int(self, row, possible_columns):
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

    def insert_roster_data(self, players_data):
        """Insert roster data into match_player table including season_id"""

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            inserted_count = 0

            # Get season_id for the match
            match_id = players_data[0]["match_id"]
            cursor.execute("SELECT season_id FROM match WHERE match_id = ?", (match_id,))
            result = cursor.fetchone()
            season_id = result[0] if result else None

            for player in players_data:
                # Generate proper match_player_id
                match_player_id = f"mp_{uuid.uuid4().hex[:8]}"

                # Resolve existing player_id
                cursor.execute("SELECT player_id FROM player WHERE player_name = ?", (player["player_name"],))
                result = cursor.fetchone()
                existing_player_id = result[0] if result else None

                # Insert roster data including season_id
                insert_sql = """
                    INSERT INTO match_player (
                        match_player_id, match_id, player_id, player_name, team_id, 
                        shirt_number, minutes_played, season_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """

                values = (
                    match_player_id,
                    player["match_id"],
                    existing_player_id,
                    player["player_name"],
                    player["team_id"],
                    player["shirt_number"],
                    player["minutes_played"],
                    season_id,
                )

                cursor.execute(insert_sql, values)
                inserted_count += 1

            conn.commit()
            conn.close()

            return True

        except Exception as e:
            logger.error(f"❌ Database insertion error: {e}")
            return False

    def generate_final_summary(self):
        """Generate final extraction summary"""

        elapsed = time.time() - self.start_time

        logger.info(f"\n{'='*60}")
        logger.info(f"ULTIMATE 110 EXTRACTION COMPLETE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'='*60}")
        logger.info(f"⏱️  Total time: {int(elapsed//60)}m {int(elapsed%60)}s")
        logger.info(f"📄 Matches processed: {self.processed_matches}/{len(ULTIMATE_110_MATCH_IDS)}")
        logger.info(f"✅ Successful extractions: {len(self.successful_matches)}")
        logger.info(f"❌ Failed extractions: {len(self.failed_matches)}")
        logger.info(f"👥 Total players extracted: {self.total_players_extracted}")

        if self.successful_matches:
            logger.info(
                f"📊 Average players per match: {self.total_players_extracted/len(self.successful_matches):.1f}"
            )
            logger.info(f"⚡ Processing rate: {len(self.successful_matches)/elapsed*60:.1f} matches/min")

        success_rate = len(self.successful_matches) / self.processed_matches * 100 if self.processed_matches > 0 else 0
        logger.info(f"🎯 Success rate: {success_rate:.1f}%")

        if self.failed_matches:
            logger.info("\n❌ Failed matches:")
            for match_id, reason in self.failed_matches[:10]:  # Show first 10 failures
                logger.info(f"  {match_id}: {reason}")
            if len(self.failed_matches) > 10:
                logger.info(f"  ... and {len(self.failed_matches) - 10} more")

        logger.info(f"{'='*60}")

        return self.generate_summary()

    def generate_summary(self):
        """Generate extraction summary"""
        elapsed = time.time() - self.start_time if self.start_time else 0

        return {
            "total_matches": len(ULTIMATE_110_MATCH_IDS),
            "processed_matches": self.processed_matches,
            "successful_matches": len(self.successful_matches),
            "failed_matches": len(self.failed_matches),
            "total_players_extracted": self.total_players_extracted,
            "elapsed_time": elapsed,
            "success_rate": len(self.successful_matches) / self.processed_matches * 100
            if self.processed_matches > 0
            else 0,
            "processing_rate": len(self.successful_matches) / elapsed * 60 if elapsed > 0 else 0,
            "failed_match_details": self.failed_matches,
        }


def main():
    """Main function to run ULTIMATE 110 batch extraction"""

    extractor = Ultimate110RosterExtractor()
    results = extractor.process_all_matches()

    # ULTIMATE DATABASE VERIFICATION - THE MOST COMPREHENSIVE CHECK EVER!
    logger.info("\n🔍 ULTIMATE 12+ SEASON DATABASE VERIFICATION:")
    conn = sqlite3.connect("/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db")
    cursor = conn.cursor()

    # Get overall database statistics
    cursor.execute("""
        SELECT 
            COUNT(DISTINCT s.season_year) as seasons_with_data,
            COUNT(DISTINCT mp.match_id) as total_matches_with_roster,
            COUNT(*) as total_players,
            COUNT(CASE WHEN mp.shirt_number IS NOT NULL THEN 1 END) as players_with_shirt_numbers,
            COUNT(CASE WHEN mp.minutes_played IS NOT NULL THEN 1 END) as players_with_minutes,
            COUNT(CASE WHEN mp.player_id IS NOT NULL THEN 1 END) as players_with_id_links,
            COUNT(CASE WHEN mp.season_id IS NOT NULL THEN 1 END) as players_with_season_id
        FROM match_player mp 
        JOIN match m ON mp.match_id = m.match_id 
        JOIN season s ON m.season_id = s.season_id
    """)

    seasons, matches, total_players, with_shirts, with_minutes, with_links, with_season = cursor.fetchone()

    logger.info("🎯 ULTIMATE DATABASE STATUS:")
    logger.info(f"Seasons with roster data: {seasons}")
    logger.info(f"Total matches with roster data: {matches}")
    logger.info(f"Total players in database: {total_players}")
    logger.info(f"Players with shirt numbers: {with_shirts} ({with_shirts/total_players*100:.1f}%)")
    logger.info(f"Players with minutes: {with_minutes} ({with_minutes/total_players*100:.1f}%)")
    logger.info(f"Players with ID links: {with_links} ({with_links/total_players*100:.1f}%)")
    logger.info(f"Players with season_id: {with_season} ({with_season/total_players*100:.1f}%)")

    # Get complete season breakdown
    cursor.execute("""
        SELECT s.season_year, COUNT(DISTINCT mp.match_id) as matches, COUNT(*) as players
        FROM match_player mp 
        JOIN match m ON mp.match_id = m.match_id 
        JOIN season s ON m.season_id = s.season_id 
        GROUP BY s.season_year 
        ORDER BY s.season_year DESC
    """)

    season_breakdown = cursor.fetchall()

    logger.info("\n📊 ULTIMATE MULTI-SEASON BREAKDOWN:")
    total_matches_db = 0
    total_players_db = 0
    earliest_season = None
    latest_season = None

    for season_year, match_count, player_count in season_breakdown:
        logger.info(f"  {season_year}: {match_count} matches, {player_count} players")
        total_matches_db += match_count
        total_players_db += player_count
        if earliest_season is None or season_year < earliest_season:
            earliest_season = season_year
        if latest_season is None or season_year > latest_season:
            latest_season = season_year

    span_years = latest_season - earliest_season + 1 if earliest_season and latest_season else 0

    logger.info("\n🏆 ULTIMATE DATABASE ACHIEVEMENT:")
    logger.info(f"  Total Seasons: {len(season_breakdown)}")
    logger.info(f"  Historical Span: {earliest_season}-{latest_season} ({span_years} years)")
    logger.info(f"  Total Matches: {total_matches_db}")
    logger.info(f"  Total Players: {total_players_db}")
    logger.info("  Database Status: ULTIMATE ✅")

    if len(season_breakdown) >= 12:
        logger.info(f"\n🎉 ULTIMATE MILESTONE: {len(season_breakdown)}-SEASON NWSL DATABASE!")
        logger.info(f"🚀 This database spans {span_years} years of professional women's soccer!")
        logger.info("🌟 ACHIEVEMENT: Most comprehensive NWSL database in existence!")
        logger.info(f"📈 Coverage: Complete historical record from {earliest_season} to {latest_season}!")
    elif len(season_breakdown) >= 11:
        logger.info("\n🎉 EXPANDING THE UNPRECEDENTED DATABASE!")
        logger.info(f"🚀 {len(season_breakdown)} seasons and growing!")

    conn.close()

    return results


if __name__ == "__main__":
    results = main()
