#!/usr/bin/env python3
"""
NWSL Match Team Statistics Scraper
Extracts team-level statistics from FBRef match report HTML files
"""

import os
import sqlite3
import uuid
from bs4 import BeautifulSoup
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

class MatchTeamScraper:
    def __init__(self, db_path='data/processed/nwsldata.db'):
        self.db_path = db_path

    def scrape_match_team_stats(self, source, match_id):
        """
        Scrape team statistics from FBRef match report
        source: can be URL or local file path
        """
        print(f"🔍 Scraping team stats from: {source}")
        
        try:
            # Read HTML content
            if os.path.exists(source):
                with open(source, 'r', encoding='utf-8') as f:
                    html_content = f.read()
            else:
                print(f"❌ File not found: {source}")
                return False
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract team stats
            team_stats = self._extract_team_stats(soup, match_id)
            
            if team_stats:
                return self._save_team_stats(team_stats, match_id)
            else:
                print("❌ No team stats found in HTML")
                return False
                
        except Exception as e:
            print(f"❌ Error scraping team stats: {e}")
            return False

    def _extract_team_stats(self, soup, match_id):
        """Extract team statistics from the HTML"""
        team_stats = []
        
        # Look for team stats tables
        # FBRef uses various table IDs for team stats
        stats_table_ids = ['team_stats', 'team_stats_extra', 'stats_squads_standard_for', 'stats_squads_standard_against']
        
        for table_id in stats_table_ids:
            table = soup.find('table', {'id': table_id})
            if table:
                print(f"✅ Found team stats table: {table_id}")
                stats = self._parse_team_stats_table(table, match_id, table_id)
                if stats:
                    team_stats.extend(stats)
        
        # Also look for team stats in scorebox or match info
        scorebox = soup.find('div', class_='scorebox')
        if scorebox:
            scorebox_stats = self._parse_scorebox_stats(scorebox, match_id)
            if scorebox_stats:
                team_stats.extend(scorebox_stats)
        
        return team_stats

    def _parse_team_stats_table(self, table, match_id, table_type):
        """Parse a team statistics table"""
        stats = []
        
        try:
            # Find all rows in the table body
            tbody = table.find('tbody')
            if not tbody:
                return stats
            
            rows = tbody.find_all('tr')
            
            for row in rows:
                # Skip header rows
                if row.find('th', class_='left'):
                    continue
                
                cells = row.find_all(['th', 'td'])
                if len(cells) < 2:
                    continue
                
                # Extract team name from first cell
                team_cell = cells[0]
                team_name = team_cell.get_text(strip=True)
                
                if not team_name or team_name in ['Squad', 'Team']:
                    continue
                
                # Create team stats record
                stat_record = {
                    'team_stats_id': str(uuid.uuid4()),
                    'match_id': match_id,
                    'team_name': team_name,
                    'stat_type': table_type
                }
                
                # Extract stats from remaining cells
                headers = self._get_table_headers(table)
                
                for i, cell in enumerate(cells[1:], 1):
                    if i < len(headers):
                        header = headers[i].lower().replace(' ', '_')
                        value = cell.get_text(strip=True)
                        
                        # Map common stats to our database columns
                        if header in ['possession', 'poss', 'poss_%']:
                            stat_record['possession'] = self._clean_percentage(value)
                        elif header in ['shots', 'sh']:
                            stat_record['shots'] = self._clean_number(value)
                        elif header in ['shots_on_target', 'sot', 'sh_on_target']:
                            stat_record['shots_on_target'] = self._clean_number(value)
                        elif header in ['passes_completed', 'pass_cmp', 'cmp']:
                            stat_record['passes_completed'] = self._clean_number(value)
                        elif header in ['passes_attempted', 'pass_att', 'att']:
                            stat_record['passes_attempted'] = self._clean_number(value)
                        elif header in ['formation', 'form']:
                            stat_record['formation'] = value
                        elif header in ['manager', 'mgr']:
                            stat_record['manager'] = value
                        elif header in ['captain', 'cap']:
                            stat_record['captain'] = value
                
                stats.append(stat_record)
                print(f"✅ Extracted stats for {team_name}")
        
        except Exception as e:
            print(f"❌ Error parsing team stats table {table_type}: {e}")
        
        return stats

    def _parse_scorebox_stats(self, scorebox, match_id):
        """Parse team info from scorebox section"""
        stats = []
        
        try:
            # Look for team divs in scorebox
            team_divs = scorebox.find_all('div', class_='scorebox_team')
            
            for team_div in team_divs:
                team_name_elem = team_div.find('strong')
                if not team_name_elem:
                    continue
                
                team_name = team_name_elem.get_text(strip=True)
                
                stat_record = {
                    'team_stats_id': str(uuid.uuid4()),
                    'match_id': match_id,
                    'team_name': team_name,
                    'stat_type': 'scorebox'
                }
                
                # Look for formation info
                formation_elem = team_div.find('div', string=lambda x: x and 'Formation' in x)
                if formation_elem:
                    formation_text = formation_elem.get_text(strip=True)
                    if 'Formation:' in formation_text:
                        stat_record['formation'] = formation_text.replace('Formation:', '').strip()
                
                # Look for manager info
                manager_elem = team_div.find('div', string=lambda x: x and 'Manager' in x)
                if manager_elem:
                    manager_text = manager_elem.get_text(strip=True)
                    if 'Manager:' in manager_text:
                        stat_record['manager'] = manager_text.replace('Manager:', '').strip()
                
                stats.append(stat_record)
                print(f"✅ Extracted scorebox info for {team_name}")
        
        except Exception as e:
            print(f"❌ Error parsing scorebox stats: {e}")
        
        return stats

    def _get_table_headers(self, table):
        """Extract headers from table"""
        headers = []
        
        thead = table.find('thead')
        if thead:
            header_rows = thead.find_all('tr')
            for row in header_rows:
                header_cells = row.find_all(['th', 'td'])
                for cell in header_cells:
                    header_text = cell.get_text(strip=True)
                    if header_text:
                        headers.append(header_text)
        
        return headers

    def _clean_number(self, value):
        """Clean and convert numeric values"""
        if not value or value == '':
            return None
        
        # Remove non-numeric characters except decimal point
        cleaned = ''.join(c for c in value if c.isdigit() or c == '.')
        
        try:
            return int(cleaned) if '.' not in cleaned else float(cleaned)
        except (ValueError, TypeError):
            return None

    def _clean_percentage(self, value):
        """Clean percentage values"""
        if not value or value == '':
            return None
        
        # Remove % sign and convert
        cleaned = value.replace('%', '').strip()
        
        try:
            return float(cleaned)
        except (ValueError, TypeError):
            return None

    def _save_team_stats(self, team_stats, match_id):
        """Save team statistics to database"""
        if not team_stats:
            print("❌ No team stats to save")
            return False
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Check if match_team table exists and get its structure
            cursor.execute("PRAGMA table_info(match_team)")
            columns = [col[1] for col in cursor.fetchall()]
            
            saved_count = 0
            
            for stat in team_stats:
                # Check if team stats already exist for this match
                cursor.execute('''
                    SELECT COUNT(*) FROM match_team 
                    WHERE match_id = ? AND team_name = ?
                ''', (match_id, stat['team_name']))
                
                if cursor.fetchone()[0] > 0:
                    print(f"⏭️ Team stats already exist for {stat['team_name']} in match {match_id}")
                    continue
                
                # Build INSERT query based on available columns and data
                available_data = {k: v for k, v in stat.items() if k in columns and v is not None}
                
                if len(available_data) < 3:  # Need at least team_stats_id, match_id, team_name
                    continue
                
                placeholders = ', '.join(['?' for _ in available_data])
                column_names = ', '.join(available_data.keys())
                
                insert_query = f'''
                    INSERT INTO match_team ({column_names})
                    VALUES ({placeholders})
                '''
                
                cursor.execute(insert_query, list(available_data.values()))
                saved_count += 1
                print(f"✅ Saved team stats for {stat['team_name']}")
            
            conn.commit()
            conn.close()
            
            print(f"📊 Successfully saved {saved_count} team stat records for match {match_id}")
            return saved_count > 0
            
        except Exception as e:
            print(f"❌ Error saving team stats: {e}")
            return False

if __name__ == "__main__":
    scraper = MatchTeamScraper()
    
    # Test with a sample HTML file
    import glob
    
    html_files = glob.glob('data/raw_match_pages/*.html')
    if html_files:
        test_file = html_files[0]
        match_id = os.path.basename(test_file)[:8]
        
        print(f"🧪 Testing with file: {test_file}")
        print(f"🆔 Match ID: {match_id}")
        
        success = scraper.scrape_match_team_stats(test_file, match_id)
        if success:
            print("✅ Team stats scraper test successful!")
        else:
            print("❌ Team stats scraper test failed!")
    else:
        print("❌ No HTML files found for testing")