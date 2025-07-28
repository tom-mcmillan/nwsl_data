#!/usr/bin/env python3
"""
Test scraping 2019 shot data directly from FBRef live URL
"""

import time
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from bs4 import BeautifulSoup

def test_beautifulsoup_scraping(url):
    """Test BeautifulSoup scraping first"""
    print(f"🔍 Testing BeautifulSoup scraping: {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        print(f"   📡 Response status: {response.status_code}")
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Look for shot containers
            shot_containers = ['all_shots', 'shots_all', 'shots_home', 'shots_away', 'shots']
            
            for container_id in shot_containers:
                container = soup.find('div', id=container_id)
                if container:
                    print(f"   📊 Found container: {container_id}")
                    
                    # Look for tables within the container
                    tables = container.find_all('table')
                    if tables:
                        for i, table in enumerate(tables):
                            print(f"   📋 Table {i+1} found with {len(str(table))} characters")
                            
                            try:
                                df = pd.read_html(str(table))[0]
                                print(f"   ✅ Parsed table: {len(df)} rows, {len(df.columns)} columns")
                                print(f"   📝 Columns: {list(df.columns)}")
                                
                                # Check for shot data indicators
                                col_str = str(df.columns).lower()
                                if any(indicator in col_str for indicator in ['minute', 'player', 'xg', 'outcome']):
                                    print(f"   🎯 Found shot data indicators!")
                                    if len(df) > 0:
                                        print(f"   📊 Sample data:")
                                        print(df.head(3).to_string())
                                        return True
                                    
                            except Exception as e:
                                print(f"   ⚠️ Could not parse table: {e}")
                    else:
                        print(f"   ⚠️ Container {container_id} found but no tables inside")
                        print(f"   📄 Container content preview: {str(container)[:200]}...")
                        
            print("   ❌ No shot tables found with BeautifulSoup")
            return False
            
        else:
            print(f"   ❌ HTTP Error: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"   ❌ BeautifulSoup error: {e}")
        return False

def test_selenium_scraping(url):
    """Test Selenium scraping with dynamic loading"""
    print(f"\n🔍 Testing Selenium scraping: {url}")
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    driver = None
    try:
        driver = webdriver.Chrome(options=chrome_options)
        print(f"   🚀 Loading page...")
        
        driver.get(url)
        print(f"   ⏳ Waiting for dynamic content...")
        time.sleep(10)  # Wait for dynamic loading
        
        # Look for shot containers
        shot_containers = ['all_shots', 'shots_all', 'shots_home', 'shots_away', 'shots']
        
        for container_id in shot_containers:
            try:
                container = driver.find_element(By.ID, container_id)
                if container:
                    print(f"   📊 Found container: {container_id}")
                    
                    # Get inner HTML
                    html_content = container.get_attribute('innerHTML')
                    if html_content and len(html_content.strip()) > 100:
                        print(f"   ✅ Container has content: {len(html_content)} characters")
                        
                        # Parse with BeautifulSoup
                        soup = BeautifulSoup(html_content, 'html.parser')
                        tables = soup.find_all('table')
                        
                        for i, table in enumerate(tables):
                            print(f"   📋 Table {i+1}: {len(str(table))} characters")
                            
                            try:
                                df = pd.read_html(str(table))[0]
                                print(f"   ✅ Parsed table: {len(df)} rows, {len(df.columns)} columns")
                                print(f"   📝 Columns: {list(df.columns)}")
                                
                                # Check for shot data
                                col_str = str(df.columns).lower()
                                shot_indicators = ['minute', 'player', 'xg', 'outcome', 'distance']
                                matches = [indicator for indicator in shot_indicators if indicator in col_str]
                                
                                if matches:
                                    print(f"   🎯 Found shot indicators: {matches}")
                                    if len(df) > 0:
                                        print(f"   📊 Sample data:")
                                        print(df.head(3).to_string())
                                        return True
                                        
                            except Exception as e:
                                print(f"   ⚠️ Could not parse table: {e}")
                    else:
                        print(f"   ⚠️ Container {container_id} empty or minimal content")
                        
            except Exception as e:
                print(f"   ⚠️ Container {container_id} not found: {e}")
        
        print("   ❌ No shot data found with Selenium")
        return False
        
    except Exception as e:
        print(f"   ❌ Selenium error: {e}")
        return False
        
    finally:
        if driver:
            driver.quit()

def main():
    url = "https://fbref.com/en/matches/afcd583a/Washington-Spirit-Sky-Blue-FC-April-13-2019-NWSL"
    
    print("🧪 Testing 2019 NWSL match for shot data availability...")
    print(f"🔗 URL: {url}")
    
    # Test both methods
    bs_success = test_beautifulsoup_scraping(url)
    selenium_success = test_selenium_scraping(url)
    
    print(f"\n📊 Results:")
    print(f"   BeautifulSoup: {'✅ Found shot data' if bs_success else '❌ No shot data'}")
    print(f"   Selenium: {'✅ Found shot data' if selenium_success else '❌ No shot data'}")
    
    if bs_success or selenium_success:
        print("\n🎉 Shot data is available for 2019! We can scrape it.")
        print("💡 Next steps: Create scraper to extract 2019/2020 shot data from live FBRef URLs")
    else:
        print("\n❌ No shot data found for 2019 match")
        print("💡 FBRef may not have shot-level data for 2019 NWSL matches")

if __name__ == "__main__":
    main()