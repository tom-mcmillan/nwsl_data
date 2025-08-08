#!/usr/bin/env python3
"""
Live Player Data Scraper - integrates with WebFetch for real scraping
Usage: Process player batches with proper error handling and database updates
"""

import sqlite3
from datetime import datetime


class LivePlayerScraper:
    def __init__(self, db_path: str = "data/processed/nwsldata.db"):
        self.db_path = db_path
        self.nation_mapping = self._load_nation_mapping()
        self.batch_results = []

    def _load_nation_mapping(self) -> dict[str, str]:
        """Load nation name to nation_id mapping"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT nation_id, nation_name FROM nation")

        mapping = {}
        for nation_id, nation_name in cursor.fetchall():
            mapping[nation_name.lower()] = nation_id

            # Add common aliases
            aliases = {
                "united states": ["usa", "us", "america", "american"],
                "new zealand": ["nzl", "nz"],
                "brazil": ["brazilian", "bra"],
                "spain": ["spanish", "esp"],
                "canada": ["canadian", "can"],
                "australia": ["australian", "aus"],
                "guatemala": ["guatemalan", "gtm"],
                "cameroon": ["cameroonian", "cmr"],
                "republic of ireland": ["ireland", "irish", "irl"],
            }

            nation_lower = nation_name.lower()
            for full_name, variations in aliases.items():
                if nation_lower == full_name:
                    for alias in variations:
                        mapping[alias] = nation_id

        conn.close()
        return mapping

    def _map_nationality(self, nationality_text: str) -> str | None:
        """Map nationality text to database nation_id"""
        if not nationality_text:
            return None

        nat_clean = nationality_text.lower().strip()

        # Direct match
        if nat_clean in self.nation_mapping:
            return self.nation_mapping[nat_clean]

        # Fuzzy matching
        for nation_name, nation_id in self.nation_mapping.items():
            if nat_clean in nation_name or nation_name in nat_clean:
                return nation_id

        print(f"⚠️  Unknown nationality: '{nationality_text}'")
        return None

    def parse_player_response(self, response_text: str) -> dict[str, any]:
        """Parse WebFetch response into structured data"""
        data = {"dob": None, "nation_id": None, "footed": None, "height_cm": None}

        if not response_text:
            return data

        lines = response_text.split("\n")

        for line in lines:
            line = line.strip()

            # Parse DOB
            if line.startswith("DOB:"):
                dob_text = line.replace("DOB:", "").strip()
                data["dob"] = self._parse_date(dob_text)

            # Parse Nationality
            elif line.startswith("Nationality:"):
                nat_text = line.replace("Nationality:", "").strip()
                data["nation_id"] = self._map_nationality(nat_text)

            # Parse Preferred Foot
            elif "Preferred Foot:" in line:
                foot_text = line.split("Preferred Foot:")[1].strip()
                if foot_text.lower() in ["left", "right", "both"]:
                    data["footed"] = foot_text.title()

            # Parse Height (look for cm)
            elif "Height:" in line and "cm" in line:
                height_text = line.split("Height:")[1].strip()
                data["height_cm"] = self._parse_height_cm(height_text)

        return data

    def _parse_date(self, date_str: str) -> str | None:
        """Parse various date formats to YYYY-MM-DD"""
        if not date_str:
            return None

        # Remove common prefixes/suffixes
        date_clean = date_str.replace(",", "").strip()

        date_formats = [
            "%B %d %Y",  # January 15 1995
            "%b %d %Y",  # Jan 15 1995
            "%Y-%m-%d",  # 1995-01-15
            "%m/%d/%Y",  # 01/15/1995
            "%d %B %Y",  # 15 January 1995
        ]

        for fmt in date_formats:
            try:
                dt = datetime.strptime(date_clean, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        print(f"⚠️  Could not parse date: '{date_str}'")
        return None

    def _parse_height_cm(self, height_str: str) -> int | None:
        """Extract height in cm from text"""
        if not height_str or "cm" not in height_str:
            return None

        try:
            # Extract number before 'cm'
            cm_part = height_str.split("cm")[0].strip()
            # Handle cases like "175 cm" or "175cm"
            height_num = "".join(filter(lambda x: x.isdigit() or x == ".", cm_part))

            if height_num:
                return int(float(height_num))

        except (ValueError, IndexError):
            pass

        print(f"⚠️  Could not parse height: '{height_str}'")
        return None

    def update_player_in_db(self, player_id: str, bio_data: dict[str, any]) -> bool:
        """Update player record with new biographical data"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Build update query dynamically
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
            print(f"❌ Database update failed: {str(e)}")
            return False

    def print_update_summary(self, player_name: str, bio_data: dict[str, any]):
        """Print what was updated for a player"""
        updates = []
        if bio_data.get("dob"):
            updates.append(f"DOB: {bio_data['dob']}")
        if bio_data.get("nation_id"):
            updates.append(f"Nation: {bio_data['nation_id']}")
        if bio_data.get("footed"):
            updates.append(f"Foot: {bio_data['footed']}")
        if bio_data.get("height_cm"):
            updates.append(f"Height: {bio_data['height_cm']}cm")

        if updates:
            print(f"  ✅ Updated: {', '.join(updates)}")
        else:
            print("  ⚠️  No data to update")


# Helper function to process scraped data
def process_scraped_player(player_id: str, player_name: str, scraped_response: str, scraper: LivePlayerScraper) -> bool:
    """Process a single scraped player response"""

    print(f"\n🔍 Processing {player_name} ({player_id})")

    # Parse the scraped response
    bio_data = scraper.parse_player_response(scraped_response)

    # Update database if we have useful data
    if any(bio_data.values()):
        success = scraper.update_player_in_db(player_id, bio_data)
        if success:
            scraper.print_update_summary(player_name, bio_data)
            return True
        else:
            print(f"  ❌ Database update failed for {player_name}")
    else:
        print(f"  ⚠️  No usable biographical data found for {player_name}")

    return False


# Usage functions
def get_db_stats(db_path: str = "data/processed/nwsldata.db") -> dict[str, int]:
    """Get current database completion statistics"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM player")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM player WHERE dob IS NOT NULL")
    with_dob = cursor.fetchone()[0]

    conn.close()

    return {
        "total_players": total,
        "players_with_dob": with_dob,
        "percentage": round((with_dob / total) * 100, 1) if total > 0 else 0,
    }


def print_progress_update(before_stats: dict[str, int], after_stats: dict[str, int]):
    """Print progress summary"""
    improvement = after_stats["players_with_dob"] - before_stats["players_with_dob"]

    print("\n📊 Progress Update:")
    print(
        f"   Before: {before_stats['players_with_dob']}/{before_stats['total_players']} ({before_stats['percentage']}%)"
    )
    print(f"   After:  {after_stats['players_with_dob']}/{after_stats['total_players']} ({after_stats['percentage']}%)")
    print(f"   Added:  +{improvement} players with DOB")


# Initialize the scraper instance
scraper = LivePlayerScraper()

print("🚀 Live Player Scraper ready!")
print("📊 Usage: process_scraped_player(player_id, player_name, scraped_response, scraper)")
print("📈 Stats: get_db_stats()")
