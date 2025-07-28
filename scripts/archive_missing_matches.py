#!/usr/bin/env python3
"""
Archive missing NWSL match reports using Henrik's hybrid scraping approach.

This script:
1. Reads prioritized URL lists from find_missing_match_urls.py
2. Implements BeautifulSoup + Selenium fallback strategy
3. Applies proper rate limiting and error handling
4. Updates html_inventory table for tracking
5. Organizes files by year automatically
"""

import requests
import sqlite3
import time
import random
import json
from pathlib import Path
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import argparse
from datetime import datetime

class MatchArchiver:
    def __init__(self, output_dir, db_path, headless=True):
        self.output_dir = Path(output_dir)
        self.db_path = Path(db_path)
        self.headless = headless
        
        # Henrik's recommended headers
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
        # Setup Selenium (lazy initialization)
        self.driver = None
        
        # Stats tracking
        self.stats = {
            'attempted': 0,
            'success_beautifulsoup': 0,
            'success_selenium': 0,
            'failed': 0,
            'rate_limited': 0,
            'already_exists': 0
        }
    
    def setup_selenium(self):
        """Initialize Selenium driver with Henrik's recommended options."""
        if self.driver is not None:
            return
        
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            print("✅ Selenium driver initialized")
        except Exception as e:
            print(f"⚠️  Selenium setup failed: {e}")
            print("   Continuing with BeautifulSoup only...")
            self.driver = None
    
    def get_file_path(self, match_id, year):
        """Get organized file path for match HTML."""
        year_dir = self.output_dir / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        return year_dir / f"{match_id}.html"
    
    def archive_with_beautifulsoup(self, url, match_id, year):
        """Try to archive using BeautifulSoup (Henrik's Method 1)."""
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            
            if response.status_code == 429:
                return 'rate_limited', f"Rate limited (429)", None
            
            if response.status_code != 200:
                return 'failed', f"HTTP {response.status_code}", None
            
            # Quick validation - check if this looks like a match report
            if 'Match Report' in response.text and 'FBref.com' in response.text:
                file_path = self.get_file_path(match_id, year)
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                
                return 'success', None, file_path
            else:
                return 'failed', 'Content does not appear to be a match report', None
                
        except requests.exceptions.RequestException as e:
            return 'failed', f"Request error: {str(e)}", None
    
    def archive_with_selenium(self, url, match_id, year):
        """Fallback to Selenium for JavaScript content (Henrik's Method 2)."""
        if self.driver is None:
            return 'failed', 'Selenium not available', None
        
        try:
            self.driver.get(url)
            
            # Wait for match report content to load
            wait = WebDriverWait(self.driver, 15)
            
            # Look for key elements that indicate successful load
            try:
                # Wait for either player stats table or match summary
                wait.until(
                    lambda driver: 
                    driver.find_element(By.CSS_SELECTOR, "table.stats_table") or
                    driver.find_element(By.ID, "content") or
                    "Match Report" in driver.title
                )
            except TimeoutException:
                return 'failed', 'Page content did not load within timeout', None
            
            # Check for rate limiting or error pages
            page_source = self.driver.page_source
            if 'Rate Limited Request' in page_source:
                return 'rate_limited', 'Rate limited (Selenium)', None
            
            if 'Match Report' in page_source and 'FBref.com' in page_source:
                file_path = self.get_file_path(match_id, year)
                
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(page_source)
                
                return 'success', None, file_path
            else:
                return 'failed', 'Content does not appear to be a match report (Selenium)', None
                
        except WebDriverException as e:
            return 'failed', f"Selenium error: {str(e)}", None
    
    def archive_match(self, match_info):
        """Archive a single match using hybrid approach."""
        match_id = match_info['match_id']
        year = match_info['year']
        url = match_info['url']
        
        # Check if file already exists
        expected_path = self.get_file_path(match_id, year)
        if expected_path.exists():
            self.stats['already_exists'] += 1
            return 'exists', 'File already exists', expected_path
        
        self.stats['attempted'] += 1
        
        print(f"📥 Archiving {match_id} ({year}): {match_info.get('matchup', 'Unknown matchup')}")
        
        # Method 1: Try BeautifulSoup first (Henrik's recommendation)
        status, error, file_path = self.archive_with_beautifulsoup(url, match_id, year)
        
        if status == 'success':
            self.stats['success_beautifulsoup'] += 1
            print(f"   ✅ Success with BeautifulSoup")
            return status, error, file_path
        
        elif status == 'rate_limited':
            self.stats['rate_limited'] += 1
            print(f"   ⏳ Rate limited - will retry later")
            return status, error, file_path
        
        # Method 2: Fallback to Selenium
        print(f"   🔄 BeautifulSoup failed ({error}), trying Selenium...")
        
        if self.driver is None:
            self.setup_selenium()
        
        if self.driver is not None:
            status, error, file_path = self.archive_with_selenium(url, match_id, year)
            
            if status == 'success':
                self.stats['success_selenium'] += 1
                print(f"   ✅ Success with Selenium")
                return status, error, file_path
            elif status == 'rate_limited':
                self.stats['rate_limited'] += 1
                print(f"   ⏳ Rate limited (Selenium)")
                return status, error, file_path
        
        # Both methods failed
        self.stats['failed'] += 1
        print(f"   ❌ Failed: {error}")
        return status, error, file_path
    
    def update_html_inventory(self, match_id, year, status, file_path, error_message):
        """Update the html_inventory table with results."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Convert status to inventory status
            inventory_status = {
                'success': 'good',
                'rate_limited': 'rate_limited', 
                'failed': 'error',
                'exists': 'good'
            }.get(status, 'error')
            
            relative_path = str(file_path.relative_to(self.output_dir.parent.parent)) if file_path else None
            filename = file_path.name if file_path else None
            file_size = file_path.stat().st_size if file_path and file_path.exists() else 0
            
            cursor.execute("""
                INSERT OR REPLACE INTO html_inventory 
                (filename, filepath, year, match_id, status, file_size, error_message,
                 has_player_stats, has_match_summary, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                filename,
                relative_path,
                year,
                match_id,
                inventory_status,
                file_size,
                error_message,
                1 if inventory_status == 'good' else 0,  # Assume good files have stats
                1 if inventory_status == 'good' else 0,  # Assume good files have summary
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            print(f"   ⚠️  Failed to update inventory: {e}")
    
    def apply_rate_limiting(self):
        """Apply Henrik's recommended rate limiting."""
        # Random delay between 5-8 seconds to appear more human
        delay = random.uniform(5.0, 8.0)
        print(f"   ⏸️  Waiting {delay:.1f}s...")
        time.sleep(delay)
    
    def archive_batch(self, matches, max_matches=None, start_index=0):
        """Archive a batch of matches."""
        if max_matches:
            matches = matches[start_index:start_index + max_matches]
        else:
            matches = matches[start_index:]
        
        print(f"🚀 Starting archival of {len(matches)} matches...")
        print(f"   Starting from index {start_index}")
        print("=" * 60)
        
        for i, match_info in enumerate(matches, start_index + 1):
            print(f"\n[{i}/{start_index + len(matches)}]", end=" ")
            
            status, error, file_path = self.archive_match(match_info)
            
            # Update database tracking
            self.update_html_inventory(
                match_info['match_id'], 
                match_info['year'], 
                status, 
                file_path, 
                error
            )
            
            # Rate limiting (except for last item or failures)
            if i < start_index + len(matches) and status not in ['failed', 'exists']:
                self.apply_rate_limiting()
            
            # Handle rate limiting
            if status == 'rate_limited':
                print(f"   🛑 Hit rate limit. Waiting 60 seconds before continuing...")
                time.sleep(60)
        
        self.print_summary()
    
    def print_summary(self):
        """Print archival summary."""
        total = sum(self.stats.values())
        print(f"\n" + "=" * 60)
        print(f"📊 ARCHIVAL SUMMARY")
        print("=" * 60)
        print(f"Total processed: {total}")
        print(f"✅ Success (BeautifulSoup): {self.stats['success_beautifulsoup']}")
        print(f"✅ Success (Selenium): {self.stats['success_selenium']}")
        print(f"📁 Already existed: {self.stats['already_exists']}")
        print(f"⏳ Rate limited: {self.stats['rate_limited']}")
        print(f"❌ Failed: {self.stats['failed']}")
        
        success_rate = (self.stats['success_beautifulsoup'] + self.stats['success_selenium']) / max(self.stats['attempted'], 1) * 100
        print(f"📈 Success rate: {success_rate:.1f}%")
    
    def cleanup(self):
        """Clean up resources."""
        if self.driver:
            self.driver.quit()
            print("🔒 Selenium driver closed")

def load_priority_list(json_file):
    """Load prioritized match list from JSON."""
    with open(json_file, 'r') as f:
        data = json.load(f)
    return data['matches']

def main():
    parser = argparse.ArgumentParser(description='Archive missing NWSL match reports')
    parser.add_argument('--priority-file', 
                       default='data/missing_matches/priority_1_recent_years.json',
                       help='JSON file with prioritized matches to archive')
    parser.add_argument('--output-dir',
                       default='data/raw_match_pages',
                       help='Directory to save HTML files')
    parser.add_argument('--db-path',
                       default='data/processed/nwsldata.db',
                       help='Path to SQLite database')
    parser.add_argument('--max-matches', type=int,
                       help='Maximum number of matches to archive (for testing)')
    parser.add_argument('--start-index', type=int, default=0,
                       help='Start index for resuming batch (0-based)')
    parser.add_argument('--no-headless', action='store_true',
                       help='Run Selenium in visible mode (for debugging)')
    
    args = parser.parse_args()
    
    # Convert paths
    script_dir = Path(__file__).parent.parent
    priority_file = script_dir / args.priority_file if not Path(args.priority_file).is_absolute() else Path(args.priority_file)
    output_dir = script_dir / args.output_dir if not Path(args.output_dir).is_absolute() else Path(args.output_dir)
    db_path = script_dir / args.db_path if not Path(args.db_path).is_absolute() else Path(args.db_path)
    
    print(f"📋 Priority file: {priority_file}")
    print(f"📁 Output directory: {output_dir}")
    print(f"🗄️  Database: {db_path}")
    print(f"🎯 Mode: {'Visible browser' if args.no_headless else 'Headless'}")
    
    # Load matches to archive
    try:
        matches = load_priority_list(priority_file)
        print(f"📥 Loaded {len(matches)} matches from priority list")
    except Exception as e:
        print(f"❌ Failed to load priority file: {e}")
        return 1
    
    # Initialize archiver
    archiver = MatchArchiver(output_dir, db_path, headless=not args.no_headless)
    
    try:
        # Archive matches
        archiver.archive_batch(matches, args.max_matches, args.start_index)
        
    except KeyboardInterrupt:
        print(f"\n⏹️  Archival interrupted by user")
        archiver.print_summary()
    
    except Exception as e:
        print(f"\n💥 Unexpected error: {e}")
        archiver.print_summary()
        return 1
    
    finally:
        archiver.cleanup()
    
    print(f"\n🏁 Archival complete!")
    return 0

if __name__ == "__main__":
    exit(main())