#!/usr/bin/env python3
"""
FBRef Player Scraper - Following Henrik Schjøth's methodology EXACTLY
BeautifulSoup-first + Selenium fallback with proper headers and rate limiting
"""

import logging
import random
import sqlite3
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

# Create a persistent session for connection reuse
session = requests.Session()

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class FBRefScraper:
    """
    FBRef scraper implementing Henrik Schjøth's proven methodology:
    1. BeautifulSoup-first strategy (10x faster)
    2. Proper User-Agent headers (critical!)
    3. Sacred 6-second rate limiting
    4. Selenium fallback for dynamic content
    5. Robust error recovery
    """

    def __init__(self, db_path: str = "data/processed/nwsldata.db"):
        self.db_path = db_path

        # EXACT headers from Henrik's documentation + additional deception headers
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

        self.nation_mapping = self._load_nation_mapping()
        self.request_count = 0
        self.success_count = 0
        self.failed_players = []

        logger.info("🚀 FBRef Scraper initialized with Henrik's methodology")
        logger.info(f"📊 Nation mapping loaded: {len(self.nation_mapping)} countries")

    def _load_nation_mapping(self) -> dict[str, str]:
        """Load comprehensive nation name to ID mapping"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT nation_id, nation_name FROM nation")

        mapping = {}
        for nation_id, nation_name in cursor.fetchall():
            # Primary mapping
            mapping[nation_name.lower()] = nation_id

            # Comprehensive aliases for ALL major countries
            aliases = {
                "united states": ["usa", "us", "america", "american", "united states of america"],
                "new zealand": ["nzl", "nz", "new zealander"],
                "brazil": ["brazilian", "bra", "brasil"],
                "spain": ["spanish", "esp", "españa"],
                "canada": ["canadian", "can"],
                "australia": ["australian", "aus", "aussie"],
                "guatemala": ["guatemalan", "gtm"],
                "cameroon": ["cameroonian", "cmr"],
                "republic of ireland": ["ireland", "irish", "irl", "eire"],
                "united kingdom": ["uk", "england", "english", "britain", "british", "gbr"],
                "germany": ["german", "deutschland", "deu", "ger"],
                "france": ["french", "fra", "française"],
                "netherlands": ["dutch", "nld", "holland"],
                "sweden": ["swedish", "swe"],
                "norway": ["norwegian", "nor"],
                "denmark": ["danish", "dnk"],
                "colombia": ["colombian", "col"],
                "mexico": ["mexican", "mex"],
                "jamaica": ["jamaican", "jam"],
                "haiti": ["haitian", "hti"],
                "costa rica": ["costa rican", "crc"],
                "japan": ["japanese", "jpn"],
                "south korea": ["korean", "kor", "korea"],
            }

            nation_lower = nation_name.lower()
            for full_name, variations in aliases.items():
                if nation_lower == full_name or any(alias in nation_lower for alias in variations):
                    for alias in variations:
                        mapping[alias] = nation_id

        conn.close()
        return mapping

    def sacred_rate_limit(self, aggressive: bool = False):
        """
        The Sacred Rate Limiting Rule
        As established by Henrik Schjøth's proven methodology
        With aggressive mode for heavy rate limiting
        """
        if aggressive:
            # When getting 429s, increase delay significantly
            delay = random.uniform(15.0, 25.0)  # 15-25 seconds
            logger.info(f"⏱️  Aggressive rate limit (429 protection): waiting {delay:.1f} seconds...")
        else:
            # Standard Henrik methodology
            delay = random.uniform(6.0, 8.0)  # 6-8 seconds
            logger.info(f"⏱️  Sacred rate limit: waiting {delay:.1f} seconds...")

        time.sleep(delay)

    def scrape_player_beautifulsoup(self, player_id: str, player_name: str) -> str | None:
        """
        BeautifulSoup-first strategy - EXACTLY as Henrik describes
        This is the preferred method (10x faster than Selenium)
        """
        try:
            # Construct FBRef URL
            player_name_url = player_name.replace(" ", "-")
            url = f"https://fbref.com/en/players/{player_id}/{player_name_url}"

            logger.info(f"🌐 BeautifulSoup request: {url}")

            # Make request with proper headers (CRITICAL!)
            data = requests.get(url, headers=self.headers)

            # Check for success response (status code 200)
            if data.status_code == 200:
                logger.info("✅ Successfully retrieved page")

                # Create BeautifulSoup object for navigating HTML
                soup = BeautifulSoup(data.text, "html.parser")

                # Extract biographical information from the page
                bio_text = self._extract_bio_from_soup(soup)

                if bio_text:
                    return bio_text
                else:
                    logger.warning("⚠️  No biographical data found in HTML")
                    return None

            else:
                logger.error(f"❌ Failed to retrieve page. Status code: {data.status_code}")
                return None

        except Exception as e:
            logger.error(f"❌ BeautifulSoup scraping failed: {str(e)}")
            return None

    def _extract_bio_from_soup(self, soup: BeautifulSoup) -> str | None:
        """Extract biographical data from BeautifulSoup parsed page"""
        bio_lines = []

        try:
            # Look for meta info section - common FBRef pattern
            meta_div = soup.find("div", {"id": "meta"}) or soup.find("div", class_="meta")

            if meta_div:
                # Extract all text and look for biographical patterns
                text_content = meta_div.get_text(separator="\n", strip=True)

                # Look for specific biographical markers
                lines = text_content.split("\n")
                for line in lines:
                    line = line.strip()

                    # Look for date patterns (DOB)
                    if any(keyword in line.lower() for keyword in ["born:", "birth:", "age:"]):
                        bio_lines.append(f"DOB: {line}")

                    # Look for nationality
                    elif any(keyword in line.lower() for keyword in ["nationality:", "citizenship:", "country:"]):
                        bio_lines.append(f"Nationality: {line}")

                    # Look for physical attributes
                    elif "cm" in line.lower() or "height" in line.lower():
                        bio_lines.append(f"Height: {line}")

                    elif any(keyword in line.lower() for keyword in ["foot:", "footed:"]):
                        bio_lines.append(f"Preferred Foot: {line}")

            # Also look in player info boxes
            info_boxes = soup.find_all("div", class_="info_box")
            for box in info_boxes:
                text = box.get_text(separator="\n", strip=True)
                # Similar pattern matching as above
                for line in text.split("\n"):
                    line = line.strip()
                    if any(pattern in line.lower() for pattern in ["born", "height", "nationality", "foot"]):
                        bio_lines.append(line)

            if bio_lines:
                return "\n".join(bio_lines)
            else:
                return None

        except Exception as e:
            logger.error(f"❌ Error extracting bio data: {str(e)}")
            return None

    def scrape_player_selenium(self, player_id: str, player_name: str) -> str | None:
        """
        Selenium fallback - for when BeautifulSoup fails
        Only use when necessary (slower but handles dynamic content)
        """
        logger.info(f"🔄 Falling back to Selenium for {player_name}")

        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.support.ui import WebDriverWait

            # Setup for Selenium - EXACTLY as Henrik describes
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")

            # Note: You'll need to set the correct path to chromedriver
            # service = ChromeService(executable_path=r"path/to/chromedriver")
            driver = webdriver.Chrome(options=chrome_options)

            # Construct URL
            player_name_url = player_name.replace(" ", "-")
            url = f"https://fbref.com/en/players/{player_id}/{player_name_url}"

            logger.info(f"🌐 Selenium request: {url}")

            # Load page
            driver.get(url)

            try:
                # Wait for page to load
                wait = WebDriverWait(driver, 10)  # Max 10 sec

                # Wait for main content to load
                wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

                # Extract page source and parse with BeautifulSoup
                page_source = driver.page_source
                soup = BeautifulSoup(page_source, "html.parser")

                bio_text = self._extract_bio_from_soup(soup)

                return bio_text

            except Exception as e:
                logger.error(f"❌ Selenium extraction failed: {str(e)}")
                return None

            finally:
                driver.quit()

        except ImportError:
            logger.error("❌ Selenium not available - install with: pip install selenium")
            return None
        except Exception as e:
            logger.error(f"❌ Selenium setup failed: {str(e)}")
            return None

    def scrape_single_player(self, player_id: str, player_name: str) -> bool:
        """
        Master scraping method - BeautifulSoup first, Selenium fallback
        EXACTLY following Henrik's hybrid methodology
        """
        self.request_count += 1

        logger.info(f"🔍 [{self.request_count}] Scraping {player_name} ({player_id})")

        # Try BeautifulSoup first (Henrik's preferred method)
        bio_text = self.scrape_player_beautifulsoup(player_id, player_name)

        if not bio_text:
            # If BeautifulSoup fails, use Selenium fallback
            logger.info(f"🔄 BeautifulSoup failed for {player_name}, trying Selenium...")
            bio_text = self.scrape_player_selenium(player_id, player_name)

        if bio_text:
            # Parse the extracted text
            bio_data = self._parse_biographical_data(bio_text)

            # Log what we found
            found_items = []
            if bio_data["dob"]:
                found_items.append(f"DOB: {bio_data['dob']}")
            if bio_data["nation_id"]:
                found_items.append(f"Nation: {bio_data['nation_id']}")
            if bio_data["footed"]:
                found_items.append(f"Foot: {bio_data['footed']}")
            if bio_data["height_cm"]:
                found_items.append(f"Height: {bio_data['height_cm']}cm")

            if found_items:
                logger.info(f"  📊 Found: {', '.join(found_items)}")

                # Update database
                if self._update_player_database(player_id, bio_data):
                    self.success_count += 1
                    logger.info("  ✅ Updated database successfully")
                    return True
                else:
                    logger.error("  ❌ Database update failed")
            else:
                logger.warning("  ⚠️  No biographical data found")
        else:
            logger.error(f"  ❌ Both BeautifulSoup and Selenium failed for {player_name}")
            self.failed_players.append((player_id, player_name))

        return False

    def _parse_biographical_data(self, bio_text: str) -> dict[str, any]:
        """Parse biographical text into structured data"""
        bio_data = {"dob": None, "nation_id": None, "footed": None, "height_cm": None}

        if not bio_text:
            return bio_data

        lines = [line.strip() for line in bio_text.split("\n") if line.strip()]

        for line in lines:
            line_lower = line.lower()

            # DOB Parsing
            if any(pattern in line_lower for pattern in ["dob:", "born:", "birth:"]):
                dob_text = self._extract_value_after_colon(line)
                bio_data["dob"] = self._parse_date_advanced(dob_text)

            # Nationality Parsing
            elif any(pattern in line_lower for pattern in ["nationality:", "citizenship:", "country:"]):
                nat_text = self._extract_value_after_colon(line)
                bio_data["nation_id"] = self._map_nationality_to_id(nat_text)

            # Preferred Foot Parsing
            elif "foot" in line_lower:
                foot_text = self._extract_value_after_colon(line).lower()
                if "left" in foot_text:
                    bio_data["footed"] = "Left"
                elif "right" in foot_text:
                    bio_data["footed"] = "Right"
                elif "both" in foot_text:
                    bio_data["footed"] = "Both"

            # Height Parsing
            elif "height" in line_lower and "cm" in line_lower:
                height_text = self._extract_value_after_colon(line)
                bio_data["height_cm"] = self._parse_height_advanced(height_text)

        return bio_data

    def _extract_value_after_colon(self, line: str) -> str:
        """Extract value after colon, handling various formats"""
        if ":" in line:
            return line.split(":", 1)[1].strip()
        return line.strip()

    def _parse_date_advanced(self, date_str: str) -> str | None:
        """Advanced date parsing handling all FBRef date formats"""
        if not date_str:
            return None

        # Clean the date string
        date_clean = date_str.replace(",", "").strip()

        # Multiple date format patterns
        date_formats = [
            "%B %d %Y",  # January 15 1995
            "%b %d %Y",  # Jan 15 1995
            "%Y-%m-%d",  # 1995-01-15
            "%m/%d/%Y",  # 01/15/1995
            "%d/%m/%Y",  # 15/01/1995
            "%d %B %Y",  # 15 January 1995
            "%d %b %Y",  # 15 Jan 1995
            "%Y",  # Just year: 1995
        ]

        for fmt in date_formats:
            try:
                if fmt == "%Y" and len(date_clean) == 4 and date_clean.isdigit():
                    # For year-only, assume January 1st
                    dt = datetime(int(date_clean), 1, 1)
                else:
                    dt = datetime.strptime(date_clean, fmt)

                # Validate reasonable birth year (1950-2010)
                if 1950 <= dt.year <= 2010:
                    return dt.strftime("%Y-%m-%d")

            except ValueError:
                continue

        logger.warning(f"⚠️  Could not parse date: '{date_str}'")
        return None

    def _parse_height_advanced(self, height_str: str) -> int | None:
        """Advanced height parsing for cm formats"""
        if not height_str:
            return None

        height_clean = height_str.lower().strip()

        try:
            # CM format: "175 cm", "175cm", "175 cm (5-9)"
            if "cm" in height_clean:
                cm_match = height_clean.split("cm")[0].strip()
                # Extract first number found
                cm_num = "".join(filter(lambda x: x.isdigit() or x == ".", cm_match.split()[0]))
                if cm_num:
                    height_val = int(float(cm_num))
                    # Validate reasonable height (140-220 cm)
                    if 140 <= height_val <= 220:
                        return height_val

        except (ValueError, IndexError, AttributeError):
            pass

        logger.warning(f"⚠️  Could not parse height: '{height_str}'")
        return None

    def _map_nationality_to_id(self, nationality_text: str) -> str | None:
        """Advanced nationality mapping with fuzzy matching"""
        if not nationality_text:
            return None

        nat_clean = nationality_text.lower().strip()

        # Direct match
        if nat_clean in self.nation_mapping:
            return self.nation_mapping[nat_clean]

        # Multi-nationality handling (e.g., "United States, England")
        if "," in nat_clean:
            primary_nat = nat_clean.split(",")[0].strip()
            if primary_nat in self.nation_mapping:
                return self.nation_mapping[primary_nat]

        # Fuzzy matching with scoring
        best_match = None
        best_score = 0

        for nation_name, nation_id in self.nation_mapping.items():
            # Calculate match score
            score = 0
            if nat_clean in nation_name:
                score = len(nat_clean) / len(nation_name)
            elif nation_name in nat_clean:
                score = len(nation_name) / len(nat_clean) * 0.8

            if score > best_score and score > 0.5:  # Minimum confidence threshold
                best_score = score
                best_match = nation_id

        if best_match:
            logger.info(f"🎯 Fuzzy matched '{nationality_text}' → {best_match} (score: {best_score:.2f})")
            return best_match

        logger.warning(f"⚠️  Unknown nationality: '{nationality_text}'")
        return None

    def _update_player_database(self, player_id: str, bio_data: dict[str, any]) -> bool:
        """Update player with transactional safety"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Build dynamic update query
            updates = []
            params = []

            for field, value in bio_data.items():
                if value is not None:
                    updates.append(f"{field} = ?")
                    params.append(value)

            if not updates:
                conn.close()
                return False

            params.append(player_id)
            query = f"UPDATE player SET {', '.join(updates)} WHERE player_id = ?"

            cursor.execute(query, params)
            rows_affected = cursor.rowcount
            conn.commit()
            conn.close()

            return rows_affected > 0

        except Exception as e:
            logger.error(f"❌ Database update failed for {player_id}: {str(e)}")
            return False

    def scrape_player_batch(self, player_batch: list[tuple[str, str]]) -> dict[str, any]:
        """
        Scrape a batch of players with proper rate limiting
        Following Henrik's methodology EXACTLY
        """
        logger.info(f"🚀 Starting batch scrape - {len(player_batch)} players")
        logger.info("📋 Following Henrik Schjøth's proven methodology")

        results = {"success": [], "failed": [], "total_processed": 0}

        for i, (player_id, player_name) in enumerate(player_batch, 1):
            logger.info(f"\n[{i}/{len(player_batch)}] Processing {player_name}")

            # Apply sacred rate limiting BEFORE each request (except first)
            if i > 1:
                # Use aggressive rate limiting if we've had failures
                aggressive_mode = len(results["failed"]) > 0
                self.sacred_rate_limit(aggressive=aggressive_mode)

            # Scrape the player
            success = self.scrape_single_player(player_id, player_name)

            if success:
                results["success"].append((player_id, player_name))
            else:
                results["failed"].append((player_id, player_name))

            results["total_processed"] += 1

        # Print final summary
        logger.info("\n📊 Batch Summary:")
        logger.info(f"✅ Success: {len(results['success'])}")
        logger.info(f"❌ Failed: {len(results['failed'])}")
        logger.info(f"📈 Success Rate: {len(results['success'])/len(player_batch)*100:.1f}%")

        return results


# Initialize the scraper
scraper = FBRefScraper()


def scrape_players(player_list: list[tuple[str, str]]) -> dict[str, any]:
    """
    Main function to scrape players using Henrik's methodology
    Usage: scrape_players([('player_id', 'Player Name'), ...])
    """
    return scraper.scrape_player_batch(player_list)


logger.info("🏆 FBRef Scraper ready - Following Henrik Schjøth's methodology EXACTLY!")
logger.info("📖 Usage: scrape_players([('player_id', 'Player Name'), ...])")
