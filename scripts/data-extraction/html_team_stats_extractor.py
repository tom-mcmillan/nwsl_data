#!/usr/bin/env python3
"""
HTML Team Stats Extractor - Following Henrik Schjøth's methodology exactly
Extract Team Stats from locally saved FBRef HTML files using BeautifulSoup
"""

import logging
import os
import re

from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class HTMLTeamStatsExtractor:
    """
    Extract Team Stats from saved FBRef HTML files using Henrik's BeautifulSoup methodology
    """

    def __init__(self, db_path: str = "data/processed/nwsldata.db"):
        self.db_path = db_path
        self.processed_matches = []
        self.failed_matches = []

    def extract_team_stats_from_html(self, html_content: str, match_id: str) -> dict[str, dict[str, any]] | None:
        """
        Extract Team Stats using BeautifulSoup - EXACTLY as Henrik describes
        Returns dict with home_team and away_team stats
        """
        try:
            # Create BeautifulSoup object for navigating HTML
            soup = BeautifulSoup(html_content, "html.parser")

            # Find the Team Stats section - using Henrik's approach
            team_stats_div = soup.find("div", id="team_stats")

            if not team_stats_div:
                logger.warning(f"⚠️  No team_stats div found for match {match_id}")
                return None

            # Look for team names in the team_stats_extra section first (more reliable)
            team_stats_extra = soup.find("div", id="team_stats_extra")
            if team_stats_extra:
                team_headers = team_stats_extra.find_all("div", class_="th")
                if len(team_headers) >= 2:
                    # First and third th elements are team names (middle one is "&nbsp;")
                    home_team_name = self._clean_team_name(team_headers[0].get_text().strip())
                    away_team_name = (
                        self._clean_team_name(team_headers[2].get_text().strip())
                        if len(team_headers) > 2
                        else self._clean_team_name(team_headers[1].get_text().strip())
                    )
                else:
                    logger.warning(f"⚠️  Could not find team headers in team_stats_extra for match {match_id}")
                    return None
            else:
                # Fallback: look for team names in main team_stats section
                team_headers = team_stats_div.find_all("div", class_="th")
                if len(team_headers) >= 2:
                    home_team_name = self._clean_team_name(team_headers[0].get_text().strip())
                    away_team_name = (
                        self._clean_team_name(team_headers[2].get_text().strip())
                        if len(team_headers) > 2
                        else self._clean_team_name(team_headers[1].get_text().strip())
                    )
                else:
                    logger.warning(f"⚠️  Could not find team headers for match {match_id}")
                    return None

            logger.info(f"📊 Extracting stats for {home_team_name} vs {away_team_name}")

            # Initialize stats dictionaries
            home_stats = {"team_name": home_team_name}
            away_stats = {"team_name": away_team_name}

            # Extract stats from each category - following FBRef structure
            stats_table = team_stats_div.find("table")
            if not stats_table:
                logger.warning(f"⚠️  No stats table found for match {match_id}")
                return None

            # Process each stat category
            rows = stats_table.find_all("tr")

            for row in rows:
                # Look for category headers (Possession, Passing Accuracy, etc.)
                category_header = row.find("th", colspan="2")
                if category_header:
                    category = category_header.get_text().strip()

                    # Get the next row with actual data
                    next_row = row.find_next_sibling("tr")
                    if next_row:
                        stats = self._extract_category_stats(next_row, category)
                        if stats and len(stats) == 2:
                            # Map category to database column name
                            column_name = self._map_category_to_column(category)
                            if column_name:
                                home_stats[column_name] = stats[0]
                                away_stats[column_name] = stats[1]

            # Extract detailed stats from the bottom section
            detailed_stats = self._extract_detailed_stats(team_stats_div)
            if detailed_stats:
                home_stats.update(detailed_stats.get("home", {}))
                away_stats.update(detailed_stats.get("away", {}))

            return {"home_team": home_stats, "away_team": away_stats}

        except Exception as e:
            logger.error(f"❌ Error extracting stats from match {match_id}: {str(e)}")
            return None

    def _clean_team_name(self, team_name: str) -> str:
        """Clean team name from HTML artifacts"""
        # Remove common HTML artifacts and extra whitespace
        cleaned = re.sub(r"<[^>]+>", "", team_name)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned

    def _extract_category_stats(self, row, category: str) -> list[str] | None:
        """Extract stats from a category row (Possession, Passing Accuracy, etc.)"""
        try:
            cells = row.find_all("td")
            if len(cells) < 2:
                return None

            stats = []
            for cell in cells[:2]:  # Home and away
                # Get the text content, handling various formats
                text = cell.get_text().strip()

                if category == "Possession":
                    # Extract percentage (e.g., "55%" -> 55)
                    pct_match = re.search(r"(\d+)%", text)
                    if pct_match:
                        stats.append(int(pct_match.group(1)))
                    else:
                        stats.append(None)

                elif category == "Passing Accuracy":
                    # Extract percentage from "368 of 490 — 75%"
                    pct_match = re.search(r"(\d+)%", text)
                    if pct_match:
                        stats.append(int(pct_match.group(1)))
                    else:
                        stats.append(None)

                elif category == "Shots on Target":
                    # Extract percentage from "4 of 10 — 40%"
                    pct_match = re.search(r"(\d+)%", text)
                    if pct_match:
                        stats.append(int(pct_match.group(1)))
                    else:
                        stats.append(None)

                elif category == "Saves":
                    # Extract percentage from "2 of 4 — 50%"
                    pct_match = re.search(r"(\d+)%", text)
                    if pct_match:
                        stats.append(int(pct_match.group(1)))
                    else:
                        stats.append(None)

                elif category == "Cards":
                    # Skip cards - too unreliable to extract from HTML
                    stats.append(None)
                else:
                    stats.append(text)

            return stats if len(stats) == 2 else None

        except Exception as e:
            logger.error(f"❌ Error extracting category stats for {category}: {str(e)}")
            return None

    def _extract_detailed_stats(self, team_stats_div) -> dict[str, dict[str, int]] | None:
        """Extract detailed stats from team_stats_extra section (Fouls, Corners, etc.)"""
        try:
            # Look for the team_stats_extra section which contains detailed stats
            team_stats_extra = team_stats_div.find_next_sibling("div", id="team_stats_extra")
            if not team_stats_extra:
                # Try to find it within the same container
                team_stats_extra = team_stats_div.find("div", id="team_stats_extra")
                if not team_stats_extra:
                    # Last resort - look for it anywhere in the soup
                    soup = BeautifulSoup(str(team_stats_div.parent), "html.parser")
                    team_stats_extra = soup.find("div", id="team_stats_extra")

            if not team_stats_extra:
                logger.warning("⚠️  Could not find team_stats_extra section")
                return None

            home_stats = {}
            away_stats = {}

            # Parse multiple stat sections within team_stats_extra
            # Each section has team headers followed by stat triplets
            stat_sections = team_stats_extra.find_all("div", recursive=False)

            for section in stat_sections:
                # Get all divs in this section
                section_divs = section.find_all("div")

                # Skip the first 3 divs (team headers: "Dash", "&nbsp;", "Spirit")
                stat_divs = section_divs[3:]

                # Process in groups of 3: home_value, stat_name, away_value
                for i in range(0, len(stat_divs), 3):
                    if i + 2 < len(stat_divs):
                        try:
                            home_value = stat_divs[i].get_text().strip()
                            stat_name = stat_divs[i + 1].get_text().strip()
                            away_value = stat_divs[i + 2].get_text().strip()

                            # Convert stat name to our database column format
                            stat_key = self._normalize_stat_name(stat_name)

                            if stat_key and home_value.isdigit() and away_value.isdigit():
                                home_stats[stat_key] = int(home_value)
                                away_stats[stat_key] = int(away_value)

                        except (ValueError, IndexError) as e:
                            logger.debug(f"Skipping stat triplet at index {i}: {e}")
                            continue

            return {"home": home_stats, "away": away_stats} if home_stats or away_stats else None

        except Exception as e:
            logger.error(f"❌ Error extracting detailed stats: {str(e)}")
            return None

    def _normalize_stat_name(self, stat_name: str) -> str | None:
        """Convert FBRef stat names to our database column names"""
        stat_mapping = {
            "Fouls": "fouls",
            "Corners": "corners",
            "Crosses": "crosses",
            "Touches": "touches",
            "Tackles": "tackles",
            "Interceptions": "interceptions",
            "Aerials Won": "aerials_won",
            "Clearances": "clearances",
            "Offsides": "offsides",
            "Goal Kicks": "goal_kicks",
            "Throw Ins": "throw_ins",
            "Long Balls": "long_balls",
        }
        return stat_mapping.get(stat_name)

    def _map_category_to_column(self, category: str) -> str | None:
        """Map Team Stats category names to database column names"""
        category_mapping = {
            "Possession": "possession_pct",
            "Passing Accuracy": "passing_acc_pct",
            "Shots on Target": "SoT_pct",
            "Saves": "saves_pct",
            "Cards": "yellow_cards",
        }
        return category_mapping.get(category)

    def process_html_file(self, html_file_path: str) -> bool:
        """Process a single HTML file and extract team stats"""
        try:
            # Extract match_id from filename
            match_id = os.path.basename(html_file_path).replace("match_", "").replace(".html", "")

            # Read HTML content
            with open(html_file_path, encoding="utf-8") as f:
                html_content = f.read()

            # Extract team stats
            team_stats = self.extract_team_stats_from_html(html_content, match_id)

            if team_stats:
                logger.info(f"✅ Successfully extracted stats for match {match_id}")
                self.processed_matches.append((match_id, team_stats))
                return True
            else:
                logger.warning(f"⚠️  Failed to extract stats for match {match_id}")
                self.failed_matches.append(match_id)
                return False

        except Exception as e:
            logger.error(f"❌ Error processing file {html_file_path}: {str(e)}")
            return False

    def process_html_directory(self, html_dir: str) -> dict[str, any]:
        """Process all HTML files in a directory"""
        logger.info(f"🚀 Processing HTML files in {html_dir}")

        html_files = [f for f in os.listdir(html_dir) if f.endswith(".html") and f.startswith("match_")]
        logger.info(f"📄 Found {len(html_files)} HTML match files")

        results = {"processed": 0, "failed": 0, "extracted_stats": []}

        for html_file in sorted(html_files):
            html_path = os.path.join(html_dir, html_file)
            if self.process_html_file(html_path):
                results["processed"] += 1
            else:
                results["failed"] += 1

        results["extracted_stats"] = self.processed_matches

        logger.info(f"📊 Processing complete: {results['processed']} success, {results['failed']} failed")
        return results


# Usage functions
def extract_stats_from_saved_html(html_dir: str) -> dict[str, any]:
    """
    Main function to extract Team Stats from saved HTML files
    """
    extractor = HTMLTeamStatsExtractor()
    return extractor.process_html_directory(html_dir)


def test_single_match(html_file_path: str):
    """Test extraction on a single match file"""
    extractor = HTMLTeamStatsExtractor()
    return extractor.process_html_file(html_file_path)


logger.info("🏆 HTML Team Stats Extractor ready - Following Henrik Schjøth's methodology!")
logger.info("📖 Usage: extract_stats_from_saved_html('/path/to/html/files/')")
