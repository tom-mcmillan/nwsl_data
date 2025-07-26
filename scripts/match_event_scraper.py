#!/usr/bin/env python3
"""
NWSL Match Event Scraper
Extracts match events (goals, cards, substitutions) from FBRef HTML files
"""

import sqlite3
import uuid
import re
from bs4 import BeautifulSoup
from datetime import datetime

class MatchEventScraper:
    def __init__(self, db_path='data/processed/nwsldata.db'):
        self.db_path = db_path
        
    def scrape_match_events(self, html_file_path, match_id):
        """Extract match events from FBRef HTML file"""
        print(f"🔍 Scraping match events from: {html_file_path}")
        
        try:
            with open(html_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            soup = BeautifulSoup(content, 'html.parser')
            
            # Extract team names from title
            teams = self._extract_teams_from_title(soup)
            if not teams:
                print("⚠️ Could not extract team names")
                return False
            
            # Find events section
            events_wrap = soup.find('div', id='events_wrap')
            if not events_wrap:
                print("⚠️ No events section found")
                return False
            
            # Extract all events
            events = self._extract_events(events_wrap, teams, match_id)
            
            if events:
                self._save_events_to_db(events, match_id)
                print(f"✅ Successfully extracted {len(events)} events")
                return True
            else:
                print("⚠️ No events found")
                return False
                
        except Exception as e:
            print(f"❌ Error scraping events: {e}")
            return False
    
    def _extract_teams_from_title(self, soup):
        """Extract team names from page title"""
        try:
            title_element = soup.find('title')
            if title_element:
                title_text = title_element.get_text()
                if ' vs. ' in title_text:
                    teams_part = title_text.split(' Match Report')[0]
                    if ' vs. ' in teams_part:
                        teams = teams_part.split(' vs. ')
                        if len(teams) >= 2:
                            home_team = teams[0].strip()
                            away_team = teams[1].strip()
                            print(f"🏠 Teams: {home_team} vs {away_team}")
                            return {'home': home_team, 'away': away_team, 'a': home_team, 'b': away_team}
            return None
        except Exception as e:
            print(f"⚠️ Could not extract teams from title: {e}")
            return None
    
    def _extract_events(self, events_wrap, teams, match_id):
        """Extract individual match events"""
        events = []
        
        # Find all event divs - look for divs with class containing 'event' and either 'a' or 'b'
        event_divs = []
        all_divs = events_wrap.find_all('div')
        for div in all_divs:
            classes = div.get('class', [])
            if 'event' in classes and ('a' in classes or 'b' in classes):
                event_divs.append(div)
        
        print(f"📋 Found {len(event_divs)} event divs")
        
        for event_div in event_divs:
            try:
                event_data = self._parse_event_div(event_div, teams, match_id)
                if event_data:
                    events.append(event_data)
            except Exception as e:
                print(f"⚠️ Error parsing event: {e}")
                continue
        
        return events
    
    def _parse_event_div(self, event_div, teams, match_id):
        """Parse individual event div"""
        try:
            # Get team (a or b)
            event_classes = event_div.get('class', [])
            team_code = None
            for cls in event_classes:
                if cls in ['a', 'b']:
                    team_code = cls
                    break
            
            if not team_code:
                return None
            
            team_name = teams.get(team_code)
            
            # Extract event text
            event_text = event_div.get_text().strip()
            
            # Parse minute - look for minute pattern, handle both directions of quotes
            minute_match = re.search(r"'?(\d+)'?", event_text)
            if not minute_match:
                return None
            minute = int(minute_match.group(1))
            
            # Parse score at time of event
            score_match = re.search(r"(\d+):(\d+)", event_text)
            home_score = int(score_match.group(1)) if score_match else None
            away_score = int(score_match.group(2)) if score_match else None
            
            # Determine event type from icon or text
            event_type = self._determine_event_type(event_div, event_text)
            
            # Extract player name
            player_name = self._extract_player_name(event_div, event_type)
            
            # Extract assist player (for goals)
            assist_player = None
            if event_type == 'goal' and 'Assist:' in event_text:
                assist_match = re.search(r'Assist:\s*([^—\n]+)', event_text)
                if assist_match:
                    assist_player = assist_match.group(1).strip()
            
            # Extract substitution info
            substituted_player = None
            if event_type == 'substitution' and ' for ' in event_text:
                sub_match = re.search(r'([^—\n]+) for ([^—\n]+)', event_text)
                if sub_match:
                    player_name = sub_match.group(1).strip()
                    substituted_player = sub_match.group(2).strip()
            
            return {
                'match_id': match_id,
                'team_name': team_name,
                'minute': minute,
                'event_type': event_type,
                'player_name': player_name,
                'assist_player': assist_player,
                'substituted_player': substituted_player,
                'home_score': home_score,
                'away_score': away_score,
                'event_text': event_text
            }
            
        except Exception as e:
            print(f"⚠️ Error parsing event div: {e}")
            return None
    
    def _determine_event_type(self, event_div, event_text):
        """Determine event type from icon or text"""
        # Look for event icon inside the event div
        icon_div = event_div.find('div', class_=re.compile(r'event_icon'))
        if icon_div:
            icon_classes = icon_div.get('class', [])
            if 'goal' in icon_classes:
                return 'goal'
            elif 'yellow_card' in icon_classes:
                return 'yellow_card'
            elif 'red_card' in icon_classes:
                return 'red_card'
            elif 'substitute_in' in icon_classes:
                return 'substitution'
        
        # Fallback to text analysis
        if '— Goal' in event_text:
            return 'goal'
        elif '— Yellow Card' in event_text:
            return 'yellow_card'
        elif '— Red Card' in event_text:
            return 'red_card'
        elif '— Substitute' in event_text:
            return 'substitution'
        
        return 'unknown'
    
    def _extract_player_name(self, event_div, event_type):
        """Extract player name from event div - look for <a> tags first"""
        try:
            # First try to find player name in <a> tags (more reliable)
            player_links = event_div.find_all('a')
            for link in player_links:
                href = link.get('href', '')
                if '/players/' in href:  # This is likely a player link
                    player_name = link.get_text().strip()
                    if player_name and len(player_name) > 1:
                        return player_name
            
            # Fallback to text extraction
            event_text = event_div.get_text().strip()
            lines = event_text.split('\n')
            for line in lines:
                line = line.strip()
                # Skip empty lines, minute markers, score lines, and metadata
                if (line and 
                    not re.match(r"^\d+'", line) and  # Skip minute markers
                    not re.match(r"^\d+:\d+", line) and  # Skip score lines  
                    not line.startswith(('Assist:', '—', 'for')) and
                    len(line) > 1 and
                    line not in ['Goal', 'Yellow Card', 'Red Card', 'Substitute']):
                    
                    # For substitutions, handle "Player for OtherPlayer" format
                    if event_type == 'substitution' and ' for ' in line:
                        return line.split(' for ')[0].strip()
                    
                    # Return the first valid player name found
                    return line
            return None
        except Exception as e:
            print(f"        ⚠️ Error extracting player name: {e}")
            return None
    
    def _save_events_to_db(self, events, match_id):
        """Save events to match_event table"""
        conn = sqlite3.connect(self.db_path)
        
        saved_count = 0
        for event in events:
            try:
                # Generate unique event ID
                event_id = f"evt_{str(uuid.uuid4())[:8]}"
                
                # Get player_id and team_id from database
                player_id = self._get_player_id(conn, event['player_name']) if event['player_name'] else None
                assist_player_id = self._get_player_id(conn, event['assist_player']) if event['assist_player'] else None
                substituted_player_id = self._get_player_id(conn, event['substituted_player']) if event['substituted_player'] else None
                team_id = self._get_team_id(conn, event['team_name']) if event['team_name'] else None
                
                # Insert event using actual table schema
                conn.execute('''
                    INSERT OR REPLACE INTO match_event (
                        event_id, match_id, team_id, team_name, player_id, player_name,
                        minute, event_type, description
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    event_id, match_id, team_id, event['team_name'],
                    player_id, event['player_name'], event['minute'], event['event_type'],
                    event['event_text']
                ))
                
                saved_count += 1
                print(f"  💾 {event['minute']}' {event['event_type']}: {event['player_name']} ({event['team_name']})")
                
            except Exception as e:
                print(f"⚠️ Failed to save event: {e}")
        
        conn.commit()
        conn.execute('PRAGMA wal_checkpoint')
        conn.close()
        
        print(f"✅ Saved {saved_count} events to database")
    
    def _get_team_id(self, conn, team_name):
        """Get team_id from database"""
        if not team_name:
            return None
        result = conn.execute('SELECT team_id FROM teams WHERE team_name = ?', (team_name,)).fetchone()
        return result[0] if result else None
    
    def _get_player_id(self, conn, player_name):
        """Get player_id from database"""
        if not player_name:
            return None
        result = conn.execute('SELECT player_id FROM player WHERE player_name = ?', (player_name,)).fetchone()
        return result[0] if result else None

# Example usage
if __name__ == "__main__":
    scraper = MatchEventScraper()
    
    # Test with a single file
    test_file = "data/raw_match_pages/match_report_2023_0b11cd8c.html"
    scraper.scrape_match_events(test_file, "0b11cd8c")