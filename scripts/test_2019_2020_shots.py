#!/usr/bin/env python3
"""
Test script to extract shot data from 2019-2020 HTML files using Selenium
Based on techniques from scraping.md
"""

import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3

class TestShotExtractor:
    def __init__(self):
        self.driver = None
        
    def setup_selenium(self):
        """Setup Selenium driver for dynamic content"""
        if self.driver is None:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            
            try:
                self.driver = webdriver.Chrome(options=chrome_options)
                return True
            except Exception as e:
                print(f"   ❌ Selenium setup failed: {e}")
                return False
        return True
        
    def close_selenium(self):
        """Close Selenium driver"""
        if self.driver:
            self.driver.quit()
            self.driver = None
    
    def test_file_with_selenium(self, html_file_path):
        """Test a single file with Selenium to see if shot data loads dynamically"""
        print(f"\n🔍 Testing: {html_file_path}")
        
        if not self.setup_selenium():
            return False
            
        try:
            # Convert file path to file:// URL for Selenium
            file_url = f"file://{os.path.abspath(html_file_path)}"
            print(f"   📂 Loading: {file_url}")
            
            self.driver.get(file_url)
            
            # Wait longer for potential dynamic loading
            print("   ⏳ Waiting for page to load...")
            time.sleep(10)  # Longer wait for dynamic content
            
            # Look for shot table containers
            shot_containers = [
                'all_shots',
                'shots_all', 
                'shots_home',
                'shots_away',
                'shots'
            ]
            
            found_data = False
            
            for container_id in shot_containers:
                try:
                    element = self.driver.find_element(By.ID, container_id)
                    if element:
                        print(f"   📊 Found container: {container_id}")
                        
                        # Get the HTML content
                        html_content = element.get_attribute('innerHTML')
                        
                        if html_content and len(html_content.strip()) > 100:
                            print(f"   ✅ Container has content: {len(html_content)} characters")
                            
                            # Try to find table elements
                            soup = BeautifulSoup(html_content, 'html.parser')
                            tables = soup.find_all('table')
                            
                            for i, table in enumerate(tables):
                                print(f"   📋 Table {i+1}: {len(str(table))} characters")
                                
                                # Try to parse with pandas
                                try:
                                    df = pd.read_html(str(table))[0]
                                    print(f"   📈 Successfully parsed table with {len(df)} rows, {len(df.columns)} columns")
                                    print(f"   📝 Columns: {list(df.columns)}")
                                    
                                    # Check if it looks like shot data
                                    shot_indicators = ['minute', 'player', 'xg', 'outcome', 'distance']
                                    col_str = str(df.columns).lower()
                                    matches = [indicator for indicator in shot_indicators if indicator in col_str]
                                    
                                    if matches:
                                        print(f"   🎯 Found shot indicators: {matches}")
                                        found_data = True
                                        
                                        # Show first few rows if not empty
                                        if len(df) > 0:
                                            print(f"   📊 Sample data:")
                                            print(df.head(3).to_string())
                                    
                                except Exception as e:
                                    print(f"   ⚠️ Could not parse table: {e}")
                            
                        else:
                            print(f"   ⚠️ Container empty or minimal content: {len(html_content) if html_content else 0} characters")
                            
                except Exception as e:
                    print(f"   ⚠️ Container {container_id} not found or error: {e}")
            
            if not found_data:
                print("   ❌ No shot data found in any container")
                
                # Check if page loaded properly
                page_source = self.driver.page_source
                if "sports-reference.com/429" in page_source:
                    print("   ⚠️ Page shows rate limiting error")
                elif "fbref.com" in page_source:
                    print("   ✅ Page appears to be valid FBRef content")
                else:
                    print("   ❓ Unknown page type")
                    
            return found_data
            
        except Exception as e:
            print(f"   ❌ Error testing file: {e}")
            return False

def main():
    extractor = TestShotExtractor()
    
    # Test a few 2019/2020 files to see if Selenium can extract shot data
    test_files = [
        "data/raw_match_pages/match_report_2020_2ff0b32e.html",
        "data/raw_match_pages/match_report_2020_69701919.html", 
        "data/raw_match_pages/match_report_2020_dc633d40.html"
    ]
    
    print("🧪 Testing 2019/2020 files with Selenium for dynamic shot data...")
    
    success_count = 0
    for file_path in test_files:
        if os.path.exists(file_path):
            if extractor.test_file_with_selenium(file_path):
                success_count += 1
        else:
            print(f"❌ File not found: {file_path}")
    
    extractor.close_selenium()
    
    print(f"\n📊 Results: {success_count}/{len(test_files)} files had extractable shot data")
    
    if success_count > 0:
        print("✅ Found shot data! Can proceed with full 2019/2020 extraction using Selenium")
    else:
        print("❌ No shot data found. 2019/2020 files may not contain shot tables")

if __name__ == "__main__":
    main()