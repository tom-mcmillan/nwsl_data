#!/usr/bin/env python3
"""Debug script to examine FBRef table headers."""

import requests
from bs4 import BeautifulSoup
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox") 
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
    return webdriver.Chrome(options=chrome_options)

def debug_table_headers(url):
    driver = setup_driver()
    
    try:
        print(f"Loading: {url}")
        driver.get(url)
        
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        
        # Click tabs to load all content
        tab_buttons = driver.find_elements(By.CSS_SELECTOR, "div.tablist button")
        for button in tab_buttons:
            try:
                button.click()
                time.sleep(1)
            except:
                continue
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Find all stat tables
        stat_tables = soup.find_all('table', {'id': re.compile(r'stats_[a-f0-9-]+_')})
        
        for table in stat_tables:
            table_id = table.get('id', '')
            print(f"\n=== TABLE: {table_id} ===")
            
            thead = table.find('thead')
            if not thead:
                print("No thead found")
                continue
                
            header_rows = thead.find_all('tr')
            print(f"Header rows: {len(header_rows)}")
            
            if len(header_rows) >= 2:
                print("Top headers:")
                top_headers = [th.get_text(strip=True) for th in header_rows[0].find_all(['th', 'td'])]
                for i, h in enumerate(top_headers):
                    print(f"  {i}: '{h}'")
                    
                print("Bottom headers:")
                bottom_headers = [th.get_text(strip=True) for th in header_rows[1].find_all(['th', 'td'])]
                for i, h in enumerate(bottom_headers):
                    print(f"  {i}: '{h}'")
                    
                print("Combined headers:")
                for i, (top, bottom) in enumerate(zip(top_headers, bottom_headers)):
                    if top and bottom and top != bottom:
                        combined = f"{top}_{bottom}"
                    elif bottom:
                        combined = bottom
                    elif top:
                        combined = top
                    else:
                        combined = f"col_{i}"
                    print(f"  {i}: '{combined}'")
            else:
                print("Single level headers:")
                headers = [th.get_text(strip=True) for th in header_rows[0].find_all(['th', 'td'])]
                for i, h in enumerate(headers):
                    print(f"  {i}: '{h}'")
                    
            # Show first data row
            tbody = table.find('tbody')
            if tbody:
                first_row = tbody.find('tr')
                if first_row:
                    cells = first_row.find_all(['td', 'th'])
                    print("First data row:")
                    for i, cell in enumerate(cells):
                        print(f"  {i}: '{cell.get_text(strip=True)}'")
                        
    finally:
        driver.quit()

if __name__ == "__main__":
    debug_table_headers("https://fbref.com/en/matches/414d2972/Portland-Thorns-FC-Gotham-FC-April-22-2025-NWSL")