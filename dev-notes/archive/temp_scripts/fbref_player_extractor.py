#!/usr/bin/env python3
"""
FBRef Player Stats Extractor - Following scraping.md methodology exactly
Extract comprehensive player statistics from locally saved FBRef HTML files
"""

import logging
import os

import pandas as pd
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class FBRefPlayerExtractor:
    """
    Extract Player Stats from FBRef HTML files using the scraping.md methodology
    """

    def __init__(self, db_path: str = "data/processed/nwsldata.db"):
        self.db_path = db_path
        self.processed_matches = []
        self.failed_matches = []

    def extract_match_player_stats(self, html_content: str, match_id: str) -> list[dict] | None:
        """
        Extract all player stats from HTML following scraping.md methodology
        Returns list of player dictionaries with comprehensive stats
        """
        try:
            # Create BeautifulSoup object (following scraping.md methodology)
            soup = BeautifulSoup(html_content, "html.parser")

            # Find all tables in page (following scraping.md methodology)
            tables = soup.find_all("table")

            # Get all table IDs (following scraping.md methodology)
            table_ids = []
            for table in tables:
                table_id = table.get("id")
                if table_id:
                    table_ids.append(table_id)

            # Find player stats tables for both teams
            player_stats_tables = [
                tid
                for tid in table_ids
                if "stats_" in tid
                and any(
                    keyword in tid
                    for keyword in [
                        "summary",
                        "passing",
                        "defense",
                        "possession",
                        "misc",
                        "passing_types",
                    ]
                )
            ]

            if not player_stats_tables:
                logger.warning(f"⚠️  No player stats tables found for match {match_id}")
                return None

            # Extract team IDs from table names
            team_ids = set()
            for table_id in player_stats_tables:
                if "stats_" in table_id:
                    team_id = table_id.split("_")[1]  # Extract team_id from stats_{team_id}_{category}
                    team_ids.add(team_id)

            logger.info(f"📊 Found {len(team_ids)} teams with {len(player_stats_tables)} player stats tables")

            all_players = []

            # Process each team
            for team_id in team_ids:
                team_players = self._extract_team_players(soup, team_id, match_id)
                all_players.extend(team_players)

            logger.info(f"✅ Extracted stats for {len(all_players)} total players")
            return all_players

        except Exception as e:
            logger.error(f"❌ Error extracting player stats from match {match_id}: {str(e)}")
            return None

    def _extract_team_players(self, soup: BeautifulSoup, team_id: str, match_id: str) -> list[dict]:
        """Extract all players for one team from all available stat categories"""

        # Available stat categories

        # Get summary table first (has basic player info)
        summary_table_id = f"stats_{team_id}_summary"
        summary_table = soup.find("table", id=summary_table_id)

        if not summary_table:
            logger.warning(f"⚠️  No summary table found for team {team_id}")
            return []

        # Extract basic player info from summary table (following scraping.md methodology)
        players_data = self._extract_summary_stats(summary_table, team_id, match_id)

        if not players_data:
            return []

        # Enhance with additional stat categories
        for category in ["passing", "passing_types", "defense", "possession", "misc"]:
            table_id = f"stats_{team_id}_{category}"
            table = soup.find("table", id=table_id)

            if table:
                category_stats = self._extract_category_stats(table, category)
                # Merge stats by player name
                self._merge_player_stats(players_data, category_stats)

        logger.info(f"📈 Extracted stats for {len(players_data)} players from team {team_id}")
        return list(players_data.values())

    def _extract_summary_stats(self, table, team_id: str, match_id: str) -> dict[str, dict]:
        """Extract basic player info and summary stats from summary table"""
        players = {}

        try:
            # Convert table to DataFrame (following scraping.md methodology)
            df = pd.read_html(str(table))[0]

            # Clean up column names (flatten MultiIndex if present)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ["_".join(col).strip() for col in df.columns.values]

            # Find player name column (usually first column or 'Player')
            player_col = None
            for col in df.columns:
                if "player" in col.lower() or col == df.columns[0]:
                    player_col = col
                    break

            if not player_col:
                logger.warning("⚠️  Could not find player column in summary table")
                return {}

            # Process each player row
            for _idx, row in df.iterrows():
                player_name = str(row[player_col]).strip()

                # Skip invalid rows
                if pd.isna(player_name) or player_name == "" or player_name.lower() in ["player", "nan"]:
                    continue

                # Create player data structure matching match_player schema
                player_data = {
                    "match_id": match_id,
                    "player_id": None,  # Will try to extract from HTML
                    "player_name": player_name,
                    "team_id": team_id,
                    "team_name": None,
                    "season_id": None,
                    # Basic info
                    "shirt_number": self._safe_convert(row, ["#", "Pos", "Jersey"], int),
                    "position": self._safe_convert(row, ["Pos", "Position"], str),
                    "minutes_played": self._safe_convert(row, ["Min", "Minutes"], int),
                    # Summary performance stats
                    "summary_perf_gls": self._safe_convert(row, ["Gls", "Goals"], int),
                    "summary_perf_ast": self._safe_convert(row, ["Ast", "Assists"], int),
                    "summary_perf_pk": self._safe_convert(row, ["PK", "PKs"], int),
                    "summary_perf_pkatt": self._safe_convert(row, ["PKatt", "PKs Attempted"], int),
                    "summary_perf_sh": self._safe_convert(row, ["Sh", "Shots"], int),
                    "summary_perf_sot": self._safe_convert(row, ["SoT", "Shots on Target"], int),
                    "summary_perf_crdy": self._safe_convert(row, ["CrdY", "Yellow Cards"], int),
                    "summary_perf_crdr": self._safe_convert(row, ["CrdR", "Red Cards"], int),
                    "summary_perf_touches": self._safe_convert(row, ["Touches"], int),
                    "summary_perf_tkl": self._safe_convert(row, ["Tkl", "Tackles"], int),
                    "summary_perf_int": self._safe_convert(row, ["Int", "Interceptions"], int),
                    "summary_perf_blocks": self._safe_convert(row, ["Blocks"], int),
                    # Expected stats
                    "summary_exp_xg": self._safe_convert(row, ["xG", "Expected Goals"], float),
                    "summary_exp_npxg": self._safe_convert(row, ["npxG", "Non-Penalty xG"], float),
                    "summary_exp_xag": self._safe_convert(row, ["xAG", "Expected Assists"], float),
                    # Shot/Goal creation
                    "summary_sca_sca": self._safe_convert(row, ["SCA", "Shot-Creating Actions"], int),
                    "summary_sca_gca": self._safe_convert(row, ["GCA", "Goal-Creating Actions"], int),
                    # Passing snippet
                    "summary_pass_cmp": self._safe_convert(row, ["Cmp", "Passes Completed"], int),
                    "summary_pass_att": self._safe_convert(row, ["Att", "Passes Attempted"], int),
                    "summary_pass_cmp_pct": self._safe_convert(row, ["Cmp%", "Pass Completion %"], float),
                    "summary_pass_prgp": self._safe_convert(row, ["PrgP", "Progressive Passes"], int),
                    # Carries & Take-ons
                    "summary_carry_carries": self._safe_convert(row, ["Carries"], int),
                    "summary_carry_prgc": self._safe_convert(row, ["PrgC", "Progressive Carries"], int),
                    "summary_take_att": self._safe_convert(row, ["Att", "Take-Ons Attempted"], int),
                    "summary_take_succ": self._safe_convert(row, ["Succ", "Take-Ons Successful"], int),
                }

                players[player_name] = player_data

        except Exception as e:
            logger.error(f"❌ Error extracting summary stats: {str(e)}")
            return {}

        return players

    def _extract_category_stats(self, table, category: str) -> dict[str, dict]:
        """Extract stats from a specific category table"""
        stats = {}

        try:
            # Convert table to DataFrame (following scraping.md methodology)
            df = pd.read_html(str(table))[0]

            # Clean up column names
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = ["_".join(col).strip() for col in df.columns.values]

            # Find player name column
            player_col = None
            for col in df.columns:
                if "player" in col.lower() or col == df.columns[0]:
                    player_col = col
                    break

            if not player_col:
                return {}

            # Map category to schema fields
            field_mappings = self._get_field_mappings(category)

            # Process each player
            for _idx, row in df.iterrows():
                player_name = str(row[player_col]).strip()

                if pd.isna(player_name) or player_name == "" or player_name.lower() in ["player", "nan"]:
                    continue

                player_stats = {}
                for schema_field, possible_cols in field_mappings.items():
                    value = self._safe_convert(row, possible_cols, float if "pct" in schema_field else int)
                    if value is not None:
                        player_stats[schema_field] = value

                if player_stats:
                    stats[player_name] = player_stats

        except Exception as e:
            logger.error(f"❌ Error extracting {category} stats: {str(e)}")

        return stats

    def _merge_player_stats(self, players_data: dict, category_stats: dict):
        """Merge category stats into main player data"""
        for player_name, stats in category_stats.items():
            if player_name in players_data:
                players_data[player_name].update(stats)

    def _safe_convert(self, row, possible_columns: list[str], convert_type):
        """Safely convert a value from any of the possible column names"""
        for col in possible_columns:
            if col in row.index:
                try:
                    value = row[col]
                    if pd.isna(value) or value == "" or str(value).strip() == "":
                        continue
                    if convert_type == int:
                        return int(float(str(value).replace(",", "")))
                    elif convert_type == float:
                        return float(str(value).replace("%", "").replace(",", ""))
                    else:
                        return str(value).strip()
                except:
                    continue
        return None

    def _get_field_mappings(self, category: str) -> dict[str, list[str]]:
        """Get mapping from schema fields to possible column names for each category"""

        if category == "passing":
            return {
                "passing_total_cmp": ["Cmp", "Completed"],
                "passing_total_att": ["Att", "Attempted"],
                "passing_total_cmp_pct": ["Cmp%", "Completion%"],
                "passing_total_totdist": ["TotDist", "Total Distance"],
                "passing_total_prgdist": ["PrgDist", "Progressive Distance"],
                "passing_short_cmp": ["Short_Cmp", "Short Completed"],
                "passing_medium_cmp": ["Medium_Cmp", "Medium Completed"],
                "passing_long_cmp": ["Long_Cmp", "Long Completed"],
                "passing_ast": ["Ast", "Assists"],
                "passing_xag": ["xAG", "Expected Assists"],
                "passing_kp": ["KP", "Key Passes"],
                "passing_final_third": ["1/3", "Final Third"],
                "passing_ppa": ["PPA", "Penalty Area"],
                "passing_prgp": ["PrgP", "Progressive Passes"],
            }
        elif category == "defense":
            return {
                "def_tkl": ["Tkl", "Tackles"],
                "def_tklw": ["TklW", "Tackles Won"],
                "def_tkl_def_3rd": ["Def 3rd", "Defensive Third"],
                "def_tkl_mid_3rd": ["Mid 3rd", "Middle Third"],
                "def_tkl_att_3rd": ["Att 3rd", "Attacking Third"],
                "def_int": ["Int", "Interceptions"],
                "def_blocks_total": ["Blocks", "Total Blocks"],
                "def_clr": ["Clr", "Clearances"],
                "def_err": ["Err", "Errors"],
            }
        elif category == "possession":
            return {
                "poss_touches": ["Touches"],
                "poss_touches_def_pen": ["Def Pen", "Defensive Penalty Area"],
                "poss_touches_def_3rd": ["Def 3rd", "Defensive Third"],
                "poss_touches_mid_3rd": ["Mid 3rd", "Middle Third"],
                "poss_touches_att_3rd": ["Att 3rd", "Attacking Third"],
                "poss_touches_att_pen": ["Att Pen", "Attacking Penalty Area"],
                "poss_take_att": ["Att", "Take-Ons Attempted"],
                "poss_take_succ": ["Succ", "Successful"],
                "poss_carry_carries": ["Carries"],
                "poss_carry_totdist": ["TotDist", "Total Distance"],
                "poss_carry_prgdist": ["PrgDist", "Progressive Distance"],
                "poss_carry_prgc": ["PrgC", "Progressive Carries"],
                "poss_rec_rec": ["Rec", "Received"],
            }
        elif category == "misc":
            return {
                "misc_crdy": ["CrdY", "Yellow Cards"],
                "misc_crdr": ["CrdR", "Red Cards"],
                "misc_fls": ["Fls", "Fouls"],
                "misc_fld": ["Fld", "Fouled"],
                "misc_off": ["Off", "Offsides"],
                "misc_int": ["Int", "Interceptions"],
                "misc_recov": ["Recov", "Recoveries"],
                "aerial_won": ["Won", "Aerials Won"],
                "aerial_lost": ["Lost", "Aerials Lost"],
            }
        else:
            return {}

    def process_html_file(self, html_file_path: str) -> bool:
        """Process a single HTML file and extract comprehensive player stats"""
        try:
            # Extract match_id from filename
            match_id = os.path.basename(html_file_path).replace("match_", "").replace(".html", "")

            # Read HTML content
            with open(html_file_path, encoding="utf-8") as f:
                html_content = f.read()

            # Extract player stats using scraping.md methodology
            player_stats = self.extract_match_player_stats(html_content, match_id)

            if player_stats:
                logger.info(
                    f"✅ Successfully extracted comprehensive stats for {len(player_stats)} players in match {match_id}"
                )
                self.processed_matches.append((match_id, player_stats))
                return True
            else:
                logger.warning(f"⚠️  Failed to extract player stats for match {match_id}")
                self.failed_matches.append(match_id)
                return False

        except Exception as e:
            logger.error(f"❌ Error processing file {html_file_path}: {str(e)}")
            return False


def test_single_match(html_file_path: str):
    """Test extraction on a single match file"""
    extractor = FBRefPlayerExtractor()
    success = extractor.process_html_file(html_file_path)

    if success and extractor.processed_matches:
        match_id, players = extractor.processed_matches[0]
        print(f"\n📊 EXTRACTION RESULTS FOR {match_id}:")
        print(f"Players extracted: {len(players)}")

        if players:
            sample_player = players[0]
            print("\nSample player data structure:")
            for key, value in sample_player.items():
                if value is not None:
                    print(f"  {key}: {value}")

    return success


if __name__ == "__main__":
    # Test with first HTML file
    html_dir = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files"
    html_files = [f for f in os.listdir(html_dir) if f.endswith(".html") and f.startswith("match_")]

    if html_files:
        test_file = os.path.join(html_dir, sorted(html_files)[0])
        print(f"Testing extraction with: {test_file}")
        test_single_match(test_file)
    else:
        print("No HTML files found!")
