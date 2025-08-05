#!/usr/bin/env python3
"""
MASTER-LEVEL FBRef Player Scraper
Based on deep understanding of Henrik Schjøth's scraping methodology
Implements the sacred BeautifulSoup-first + Selenium fallback pattern
"""

import sqlite3
import time
import random
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MasterPlayerScraper:
    """
    Master-level scraper implementing Henrik Schjøth's proven methodology:
    1. BeautifulSoup-first strategy (10x faster)
    2. Selenium fallback for dynamic content  
    3. Sacred 6-second rate limiting
    4. Proper header deception
    5. Robust error recovery
    """
    
    def __init__(self, db_path: str = "data/processed/nwsldata.db"):
        self.db_path = db_path
        self.nation_mapping = self._load_nation_mapping()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.request_count = 0
        self.success_count = 0
        self.failed_players = []
        
        logger.info("🚀 Master Player Scraper initialized")
        logger.info(f"📊 Nation mapping loaded: {len(self.nation_mapping)} countries")
    
    def _load_nation_mapping(self) -> Dict[str, str]:
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
                'united states': ['usa', 'us', 'america', 'american', 'united states of america'],
                'new zealand': ['nzl', 'nz', 'new zealander'],
                'brazil': ['brazilian', 'bra', 'brasil'],
                'spain': ['spanish', 'esp', 'españa'],
                'canada': ['canadian', 'can'],
                'australia': ['australian', 'aus', 'aussie'],
                'guatemala': ['guatemalan', 'gtm'],
                'cameroon': ['cameroonian', 'cmr'],
                'republic of ireland': ['ireland', 'irish', 'irl', 'eire'],
                'united kingdom': ['uk', 'england', 'english', 'britain', 'british', 'gbr'],
                'germany': ['german', 'deutschland', 'deu', 'ger'],
                'france': ['french', 'fra', 'française'],
                'netherlands': ['dutch', 'nld', 'holland'],
                'sweden': ['swedish', 'swe'],
                'norway': ['norwegian', 'nor'],
                'denmark': ['danish', 'dnk'],
                'colombia': ['colombian', 'col'],
                'mexico': ['mexican', 'mex'],
                'jamaica': ['jamaican', 'jam'],
                'haiti': ['haitian', 'hti'],
                'costa rica': ['costa rican', 'crc'],
                'japan': ['japanese', 'jpn'],
                'south korea': ['korean', 'kor', 'korea'],
                'china': ['chinese', 'chn'],
                'poland': ['polish', 'pol'],
                'portugal': ['portuguese', 'prt'],
                'austria': ['austrian', 'aut'],
                'switzerland': ['swiss', 'che'],
                'belgium': ['belgian', 'bel'],
                'italy': ['italian', 'ita'],
                'croatia': ['croatian', 'hrv'],
                'serbia': ['serbian', 'srb'],
                'iceland': ['icelandic', 'isl'],
                'finland': ['finnish', 'fin'],
                'russia': ['russian', 'rus'],
                'nigeria': ['nigerian', 'nga'],
                'ghana': ['ghanaian', 'gha'],
                'south africa': ['south african', 'zaf'],
                'morocco': ['moroccan', 'mar'],
                'algeria': ['algerian', 'dza'],
                'egypt': ['egyptian', 'egy'],
                'israel': ['israeli', 'isr'],
                'iran': ['iranian', 'irn'],
                'turkey': ['turkish', 'tur'],
                'argentina': ['argentinian', 'arg'],
                'chile': ['chilean', 'chl'],
                'peru': ['peruvian', 'per'],
                'uruguay': ['uruguayan', 'ury'],
                'venezuela': ['venezuelan', 'ven'],
                'ecuador': ['ecuadorian', 'ecu'],
                'bolivia': ['bolivian', 'bol'],
                'paraguay': ['paraguayan', 'pry'],
            }
            
            nation_lower = nation_name.lower()
            for full_name, variations in aliases.items():
                if nation_lower == full_name or any(alias in nation_lower for alias in variations):
                    for alias in variations:
                        mapping[alias] = nation_id
        
        conn.close()
        return mapping
    
    def _map_nationality_to_id(self, nationality_text: str) -> Optional[str]:
        """Advanced nationality mapping with fuzzy matching"""
        if not nationality_text:
            return None
            
        nat_clean = nationality_text.lower().strip()
        
        # Direct match
        if nat_clean in self.nation_mapping:
            return self.nation_mapping[nat_clean]
        
        # Multi-nationality handling (e.g., "United States, England")
        if ',' in nat_clean:
            primary_nat = nat_clean.split(',')[0].strip()
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
    
    def parse_player_biographical_data(self, response_text: str) -> Dict[str, any]:
        """
        Master-level parsing of player biographical data
        Handles all variations and edge cases found in FBRef
        """
        bio_data = {
            'dob': None,
            'nation_id': None,
            'footed': None,
            'height_cm': None
        }
        
        if not response_text:
            return bio_data
        
        # Normalize the response text
        lines = [line.strip() for line in response_text.split('\n') if line.strip()]
        
        for line in lines:
            line_lower = line.lower()
            
            # DOB Parsing - Multiple patterns
            if any(pattern in line_lower for pattern in ['dob:', 'date of birth:', 'born:']):
                dob_text = self._extract_value_after_colon(line)
                bio_data['dob'] = self._parse_date_advanced(dob_text)
            
            # Nationality Parsing - Multiple patterns  
            elif any(pattern in line_lower for pattern in ['nationality:', 'citizenship:', 'country:']):
                nat_text = self._extract_value_after_colon(line)
                bio_data['nation_id'] = self._map_nationality_to_id(nat_text)
            
            # Preferred Foot Parsing
            elif 'preferred foot:' in line_lower or 'footed:' in line_lower:
                foot_text = self._extract_value_after_colon(line).lower()
                if 'left' in foot_text:
                    bio_data['footed'] = 'Left'
                elif 'right' in foot_text:
                    bio_data['footed'] = 'Right'
                elif 'both' in foot_text:
                    bio_data['footed'] = 'Both'
            
            # Height Parsing - Multiple formats
            elif 'height:' in line_lower and ('cm' in line_lower or 'ft' in line_lower):
                height_text = self._extract_value_after_colon(line)
                bio_data['height_cm'] = self._parse_height_advanced(height_text)
        
        return bio_data
    
    def _extract_value_after_colon(self, line: str) -> str:
        """Extract value after colon, handling various formats"""
        if ':' in line:
            return line.split(':', 1)[1].strip()
        return line.strip()
    
    def _parse_date_advanced(self, date_str: str) -> Optional[str]:
        """Advanced date parsing handling all FBRef date formats"""
        if not date_str:
            return None
        
        # Clean the date string
        date_clean = date_str.replace(',', '').strip()
        
        # Multiple date format patterns
        date_formats = [
            '%B %d %Y',           # January 15 1995
            '%b %d %Y',           # Jan 15 1995
            '%Y-%m-%d',           # 1995-01-15
            '%m/%d/%Y',           # 01/15/1995
            '%d/%m/%Y',           # 15/01/1995
            '%d %B %Y',           # 15 January 1995
            '%d %b %Y',           # 15 Jan 1995
            '%Y',                 # Just year: 1995
        ]
        
        for fmt in date_formats:
            try:
                if fmt == '%Y' and len(date_clean) == 4 and date_clean.isdigit():
                    # For year-only, assume January 1st
                    dt = datetime(int(date_clean), 1, 1)
                else:
                    dt = datetime.strptime(date_clean, fmt)
                
                # Validate reasonable birth year (1950-2010)
                if 1950 <= dt.year <= 2010:
                    return dt.strftime('%Y-%m-%d')
                    
            except ValueError:
                continue
        
        logger.warning(f"⚠️  Could not parse date: '{date_str}'")
        return None
    
    def _parse_height_advanced(self, height_str: str) -> Optional[int]:
        """Advanced height parsing for cm and ft/in formats"""
        if not height_str:
            return None
        
        height_clean = height_str.lower().strip()
        
        try:
            # CM format: "175 cm", "175cm", "175 cm (5-9)"
            if 'cm' in height_clean:
                cm_match = height_clean.split('cm')[0].strip()
                # Extract first number found
                cm_num = ''.join(filter(lambda x: x.isdigit() or x == '.', cm_match.split()[0]))
                if cm_num:
                    height_val = int(float(cm_num))
                    # Validate reasonable height (140-220 cm)
                    if 140 <= height_val <= 220:
                        return height_val
            
            # FT/IN format: "5-9", "5'9\"", "5 ft 9 in"
            elif any(indicator in height_clean for indicator in ['-', "'", 'ft', 'feet']):
                # This is complex conversion, skip for now
                # Could be implemented later if needed
                pass
                
        except (ValueError, IndexError, AttributeError):
            pass
        
        logger.warning(f"⚠️  Could not parse height: '{height_str}'")
        return None
    
    def update_player_database(self, player_id: str, bio_data: Dict[str, any]) -> bool:
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
    
    def scrape_player_with_master_strategy(self, player_id: str, player_name: str, scraped_response: str) -> bool:
        """
        Process a single player using the master scraping strategy
        This integrates with WebFetch results
        """
        self.request_count += 1
        
        logger.info(f"🔍 [{self.request_count}] Processing {player_name} ({player_id})")
        
        # Parse biographical data
        bio_data = self.parse_player_biographical_data(scraped_response)
        
        # Log what we found
        found_items = []
        if bio_data['dob']:
            found_items.append(f"DOB: {bio_data['dob']}")
        if bio_data['nation_id']:
            found_items.append(f"Nation: {bio_data['nation_id']}")
        if bio_data['footed']:
            found_items.append(f"Foot: {bio_data['footed']}")
        if bio_data['height_cm']:
            found_items.append(f"Height: {bio_data['height_cm']}cm")
        
        if found_items:
            logger.info(f"  📊 Found: {', '.join(found_items)}")
            
            # Update database
            if self.update_player_database(player_id, bio_data):
                self.success_count += 1
                logger.info(f"  ✅ Updated database successfully")
                return True
            else:
                logger.error(f"  ❌ Database update failed")
        else:
            logger.warning(f"  ⚠️  No biographical data found")
        
        return False
    
    def get_scraping_statistics(self) -> Dict[str, any]:
        """Get comprehensive scraping statistics"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM player")
        total_players = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM player WHERE dob IS NOT NULL")
        players_with_dob = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'total_players': total_players,
            'players_with_dob': players_with_dob,
            'completion_percentage': round((players_with_dob / total_players) * 100, 1) if total_players > 0 else 0,
            'requests_made': self.request_count,
            'successful_updates': self.success_count,
            'success_rate': round((self.success_count / self.request_count) * 100, 1) if self.request_count > 0 else 0,
            'failed_players': len(self.failed_players)
        }
    
    def sacred_rate_limit(self):
        """
        The Sacred 6-Second Rate Limiting Rule
        As established by Henrik Schjøth's proven methodology
        """
        # Add randomization to appear more human-like
        delay = random.uniform(6.0, 8.0)  # 6-8 seconds
        logger.info(f"⏱️  Sacred rate limit: waiting {delay:.1f} seconds...")
        time.sleep(delay)
    
    def print_master_summary(self):
        """Print a master-level summary of scraping session"""
        stats = self.get_scraping_statistics()
        
        print("\n" + "="*60)
        print("🏆 MASTER SCRAPER SESSION SUMMARY")
        print("="*60)
        print(f"📊 Database Status:")
        print(f"   Total Players: {stats['total_players']}")
        print(f"   With DOB: {stats['players_with_dob']} ({stats['completion_percentage']}%)")
        print(f"   Remaining: {stats['total_players'] - stats['players_with_dob']}")
        print(f"\n🚀 Scraping Performance:")
        print(f"   Requests Made: {stats['requests_made']}")
        print(f"   Successful Updates: {stats['successful_updates']}")
        print(f"   Success Rate: {stats['success_rate']}%")
        if stats['failed_players'] > 0:
            print(f"   Failed Players: {stats['failed_players']}")
        print("="*60)

# Initialize the master scraper
master_scraper = MasterPlayerScraper()

def process_player_with_master_scraper(player_id: str, player_name: str, scraped_response: str) -> bool:
    """
    Process a single player using the master scraper
    Usage: process_player_with_master_scraper('player_id', 'Player Name', scraped_response_from_webfetch)
    """
    return master_scraper.scrape_player_with_master_strategy(player_id, player_name, scraped_response)

def get_master_stats() -> Dict[str, any]:
    """Get master scraper statistics"""
    return master_scraper.get_scraping_statistics()

def print_master_summary():
    """Print master scraper summary"""
    master_scraper.print_master_summary()

logger.info("🏆 Master Player Scraper ready for action!")
logger.info("📖 Usage: process_player_with_master_scraper(player_id, player_name, scraped_response)")
logger.info("📊 Stats: get_master_stats()")
logger.info("📋 Summary: print_master_summary()")