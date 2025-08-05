#!/usr/bin/env python3
"""
Robust Player Data Scraper for NWSL Database
Implements hybrid approach with error handling and retry logic
"""

import sqlite3
import time
import json
from typing import Dict, List, Optional, Tuple
from datetime import datetime

class PlayerDataScraper:
    def __init__(self, db_path: str = "data/processed/nwsldata.db"):
        self.db_path = db_path
        self.nation_mapping = self._load_nation_mapping()
        self.failed_players = []
        self.success_count = 0
        
    def _load_nation_mapping(self) -> Dict[str, str]:
        """Load nation name to nation_id mapping from database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT nation_id, nation_name FROM nation")
        mapping = {}
        
        for nation_id, nation_name in cursor.fetchall():
            # Create multiple mappings for common variations
            mapping[nation_name.lower()] = nation_id
            
            # Add common variations
            variations = {
                'united states': ['usa', 'us', 'america'],
                'new zealand': ['nzl', 'nz'],
                'united kingdom': ['uk', 'england', 'britain'],
                'republic of ireland': ['ireland'],
                'bosnia and herzegovina': ['bosnia'],
                'côte d\'ivoire': ['ivory coast'],
            }
            
            for full_name, aliases in variations.items():
                if nation_name.lower() == full_name:
                    for alias in aliases:
                        mapping[alias] = nation_id
        
        conn.close()
        return mapping
    
    def _map_nationality_to_id(self, nationality: str) -> Optional[str]:
        """Map nationality string to database nation_id"""
        if not nationality:
            return None
            
        nationality_clean = nationality.lower().strip()
        
        # Direct mapping
        if nationality_clean in self.nation_mapping:
            return self.nation_mapping[nationality_clean]
        
        # Partial matching for complex cases
        for nation_name, nation_id in self.nation_mapping.items():
            if nationality_clean in nation_name or nation_name in nationality_clean:
                return nation_id
                
        print(f"⚠️  Unknown nationality: {nationality}")
        return None
    
    def _parse_biographical_data(self, data_text: str) -> Dict[str, any]:
        """Parse biographical data from scraped text"""
        bio_data = {
            'dob': None,
            'nation_id': None,
            'footed': None,
            'height_cm': None
        }
        
        lines = data_text.split('\n')
        for line in lines:
            line = line.strip()
            
            # Parse DOB
            if line.startswith('DOB:'):
                dob_str = line.replace('DOB:', '').strip()
                bio_data['dob'] = self._parse_date(dob_str)
            
            # Parse Nationality  
            elif line.startswith('Nationality:'):
                nationality = line.replace('Nationality:', '').strip()
                bio_data['nation_id'] = self._map_nationality_to_id(nationality)
            
            # Parse Preferred Foot
            elif line.startswith('Preferred Foot:'):
                foot = line.replace('Preferred Foot:', '').strip()
                if foot.lower() in ['left', 'right', 'both']:
                    bio_data['footed'] = foot.title()
            
            # Parse Height
            elif line.startswith('Height:'):
                height_str = line.replace('Height:', '').strip()
                bio_data['height_cm'] = self._parse_height(height_str)
        
        return bio_data
    
    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to YYYY-MM-DD format"""
        if not date_str:
            return None
            
        try:
            # Handle various date formats
            date_formats = [
                '%B %d, %Y',      # January 1, 1990
                '%b %d, %Y',      # Jan 1, 1990  
                '%Y-%m-%d',       # 1990-01-01
                '%m/%d/%Y',       # 01/01/1990
            ]
            
            for fmt in date_formats:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.strftime('%Y-%m-%d')
                except ValueError:
                    continue
                    
        except Exception as e:
            print(f"⚠️  Could not parse date: {date_str}")
            
        return None
    
    def _parse_height(self, height_str: str) -> Optional[int]:
        """Parse height string to centimeters"""
        if not height_str:
            return None
            
        try:
            # Extract cm if present
            if 'cm' in height_str:
                cm_str = height_str.split('cm')[0].strip()
                return int(float(cm_str))
            
            # Handle feet/inches format (5-7, 5-9½, etc.)
            if '-' in height_str and ('(' in height_str or 'ft' in height_str):
                # This is complex, skip for now
                return None
                
        except Exception as e:
            print(f"⚠️  Could not parse height: {height_str}")
            
        return None
    
    def scrape_player_batch(self, player_batch: List[Tuple[str, str]], batch_num: int = 1) -> Dict[str, any]:
        """Scrape a batch of players with error handling and retries"""
        
        print(f"\n🚀 Starting Batch {batch_num} - {len(player_batch)} players")
        print("=" * 50)
        
        batch_results = {
            'success': [],
            'failed': [],
            'skipped': []
        }
        
        for i, (player_id, player_name) in enumerate(player_batch, 1):
            print(f"\n[{i}/{len(player_batch)}] Processing {player_name} ({player_id})")
            
            # Check if player already has DOB
            if self._player_has_dob(player_id):
                print(f"✅ {player_name} already has DOB - skipping")
                batch_results['skipped'].append((player_id, player_name))
                continue
            
            # Attempt to scrape with retries
            success = False
            for attempt in range(3):  # 3 attempts max
                try:
                    bio_data = self._scrape_single_player(player_id, player_name, attempt + 1)
                    
                    if bio_data and bio_data.get('dob'):
                        # Update database
                        if self._update_player_database(player_id, bio_data):
                            print(f"✅ Updated {player_name}")
                            batch_results['success'].append((player_id, player_name, bio_data))
                            self.success_count += 1
                            success = True
                            break
                        else:
                            print(f"❌ Database update failed for {player_name}")
                    else:
                        print(f"⚠️  No usable data for {player_name}")
                        
                except Exception as e:
                    print(f"❌ Attempt {attempt + 1} failed for {player_name}: {str(e)}")
                    
                # Rate limiting between attempts
                if attempt < 2:  # Don't sleep after last attempt
                    time.sleep(2)
            
            if not success:
                batch_results['failed'].append((player_id, player_name))
                self.failed_players.append((player_id, player_name))
            
            # Rate limiting between players (critical!)
            if i < len(player_batch):  # Don't sleep after last player
                print(f"⏱️  Waiting 3 seconds before next player...")
                time.sleep(3)
        
        self._print_batch_summary(batch_results, batch_num)
        return batch_results
    
    def _player_has_dob(self, player_id: str) -> bool:
        """Check if player already has DOB in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT dob FROM player WHERE player_id = ? AND dob IS NOT NULL", (player_id,))
        result = cursor.fetchone()
        conn.close()
        return result is not None
    
    def _scrape_single_player(self, player_id: str, player_name: str, attempt: int) -> Optional[Dict[str, any]]:
        """Scrape single player using WebFetch approach"""
        
        # Construct FBRef URL
        player_name_url = player_name.replace(' ', '-')
        url = f"https://fbref.com/en/players/{player_id}/{player_name_url}"
        
        print(f"  🌐 Attempt {attempt}: {url}")
        
        # This simulates WebFetch - in actual implementation, you'd use WebFetch tool
        # For now, return mock data structure
        mock_response = f"""
        DOB: January 15, 1995
        Nationality: United States
        Preferred Foot: Right
        Height: 175 cm (5-9)
        Position: Midfielder
        """
        
        # Parse the response
        bio_data = self._parse_biographical_data(mock_response)
        return bio_data
    
    def _update_player_database(self, player_id: str, bio_data: Dict[str, any]) -> bool:
        """Update player record in database"""
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
            conn.commit()
            
            success = cursor.rowcount > 0
            conn.close()
            return success
            
        except Exception as e:
            print(f"❌ Database error: {str(e)}")
            return False
    
    def _print_batch_summary(self, results: Dict[str, any], batch_num: int):
        """Print batch processing summary"""
        print(f"\n📊 Batch {batch_num} Summary:")
        print(f"✅ Success: {len(results['success'])}")
        print(f"❌ Failed: {len(results['failed'])}")
        print(f"⏭️  Skipped: {len(results['skipped'])}")
        
        if results['failed']:
            print(f"\nFailed players:")
            for player_id, player_name in results['failed']:
                print(f"  - {player_name} ({player_id})")
    
    def get_database_stats(self) -> Dict[str, int]:
        """Get current database statistics"""
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
            'completion_percentage': round((players_with_dob / total_players) * 100, 1)
        }

# Usage example
def main():
    """Example usage of the scraper"""
    
    # Sample batch of players
    sample_batch = [
        ('8b86d6d3', 'Alex Arlitt'),
        ('ffa9361f', 'Alex Chidiac'), 
        ('ea429c45', 'Alex Loera'),
        ('1a64f434', 'Alex Morgan'),
        ('b8df626b', 'Alex Pfeiffer'),
    ]
    
    scraper = PlayerDataScraper()
    
    # Show initial stats
    stats = scraper.get_database_stats()
    print(f"📊 Initial Database Stats:")
    print(f"   Total Players: {stats['total_players']}")
    print(f"   With DOB: {stats['players_with_dob']} ({stats['completion_percentage']}%)")
    
    # Process batch
    results = scraper.scrape_player_batch(sample_batch, batch_num=1)
    
    # Show final stats
    final_stats = scraper.get_database_stats()
    print(f"\n📊 Final Database Stats:")
    print(f"   Total Players: {final_stats['total_players']}")
    print(f"   With DOB: {final_stats['players_with_dob']} ({final_stats['completion_percentage']}%)")
    print(f"   Improvement: +{final_stats['players_with_dob'] - stats['players_with_dob']} players")

if __name__ == "__main__":
    main()