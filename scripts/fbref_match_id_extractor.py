#!/usr/bin/env python3
"""
FBRef Match ID Extractor
Extracts actual FBRef match IDs from HTML files to use as consistent primary keys
"""

import os
import glob
import re
from bs4 import BeautifulSoup
import sqlite3
import json

class FBRefMatchIdExtractor:
    def __init__(self, db_path='data/processed/nwsldata.db'):
        self.db_path = db_path
        
    def extract_all_fbref_match_ids(self):
        """Extract FBRef match IDs from all HTML files"""
        print("🔍 Starting FBRef match ID extraction...")
        
        raw_pages_dir = 'data/raw_match_pages'
        html_files = glob.glob(os.path.join(raw_pages_dir, '*.html'))
        
        # Filter out the blocked file
        html_files = [f for f in html_files if os.path.getsize(f) > 5000]  # Skip tiny files
        
        print(f"📂 Processing {len(html_files)} valid HTML files")
        
        results = []
        successful = 0
        failed = 0
        
        for html_file in html_files:
            try:
                match_data = self._extract_fbref_match_id(html_file)
                if match_data:
                    results.append(match_data)
                    successful += 1
                    print(f"  ✅ {os.path.basename(html_file)}: {match_data['fbref_match_id']} ({match_data['teams']})")
                else:
                    failed += 1
                    print(f"  ❌ {os.path.basename(html_file)}: No match ID found")
            except Exception as e:
                failed += 1
                print(f"  ⚠️ {os.path.basename(html_file)}: Error - {e}")
        
        print(f"\n📊 Extraction Results:")
        print(f"   ✅ Successful: {successful}")
        print(f"   ❌ Failed: {failed}")
        print(f"   📈 Success Rate: {successful/(successful+failed)*100:.1f}%")
        
        # Save results to JSON for analysis
        output_file = 'data/processed/fbref_match_ids.json'
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"💾 Saved results to {output_file}")
        
        return results
    
    def _extract_fbref_match_id(self, html_file_path):
        """Extract FBRef match ID from a single HTML file"""
        try:
            with open(html_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            soup = BeautifulSoup(content, 'html.parser')
            
            # Method 1: Look for match ID in URL patterns
            # FBRef match URLs typically look like: /en/matches/[match_id]/Team-vs-Team-Date
            match_id = None
            
            # Check canonical URL in head
            canonical = soup.find('link', rel='canonical')
            if canonical:
                href = canonical.get('href', '')
                match_id = self._extract_id_from_url(href)
            
            # Method 2: Look for match ID in JavaScript data
            if not match_id:
                scripts = soup.find_all('script')
                for script in scripts:
                    if script.string:
                        # Look for match ID patterns in JS
                        js_match = re.search(r'match[_\-]?id["\']?\s*[:=]\s*["\']?([a-f0-9]{8})["\']?', script.string, re.IGNORECASE)
                        if js_match:
                            match_id = js_match.group(1)
                            break
            
            # Method 3: Look in meta tags
            if not match_id:
                meta_tags = soup.find_all('meta')
                for meta in meta_tags:
                    content_attr = meta.get('content', '')
                    if 'matches/' in content_attr:
                        match_id = self._extract_id_from_url(content_attr)
                        if match_id:
                            break
            
            # Method 4: Look for ID patterns in href attributes
            if not match_id:
                links = soup.find_all('a')
                for link in links:
                    href = link.get('href', '')
                    if '/matches/' in href:
                        match_id = self._extract_id_from_url(href)
                        if match_id:
                            break
            
            # Method 5: Look in page source for any 8-character hex patterns near "match"
            if not match_id:
                # Find patterns like "match": "a1b2c3d4" or similar
                pattern_matches = re.findall(r'match[^a-f0-9]*([a-f0-9]{8})', content, re.IGNORECASE)
                if pattern_matches:
                    match_id = pattern_matches[0]
            
            if match_id:
                # Extract additional match data
                title = soup.find('title')
                title_text = title.get_text() if title else ''
                
                # Extract teams from title
                teams = "Unknown vs Unknown"
                if ' vs. ' in title_text:
                    teams_part = title_text.split(' Match Report')[0]
                    if ' vs. ' in teams_part:
                        teams = teams_part.strip()
                
                # Extract date
                date_match = re.search(r'(\w+\s+\w+\s+\d+,\s+\d{4})', title_text)
                date_str = date_match.group(1) if date_match else 'Unknown date'
                
                return {
                    'filename': os.path.basename(html_file_path),
                    'fbref_match_id': match_id,
                    'teams': teams,
                    'date': date_str,
                    'title': title_text.strip()
                }
            
            return None
            
        except Exception as e:
            print(f"Error processing {os.path.basename(html_file_path)}: {e}")
            return None
    
    def _extract_id_from_url(self, url):
        """Extract 8-character hex ID from FBRef URL"""
        # Look for patterns like /matches/a1b2c3d4/ or /matches/a1b2c3d4-Team-vs-Team
        match = re.search(r'/matches/([a-f0-9]{8})(?:[/-]|$)', url)
        if match:
            return match.group(1)
        
        # Look for other patterns
        match = re.search(r'([a-f0-9]{8})', url)
        if match:
            potential_id = match.group(1)
            # Verify it looks like a hex ID (contains both letters and numbers ideally)
            if re.match(r'^[a-f0-9]{8}$', potential_id):
                return potential_id
        
        return None
    
    def analyze_results(self, results_file='data/processed/fbref_match_ids.json'):
        """Analyze the extracted match IDs"""
        try:
            with open(results_file, 'r') as f:
                results = json.load(f)
            
            print(f"\n📈 Analysis of {len(results)} extracted match IDs:")
            
            # Count unique IDs
            unique_ids = set(r['fbref_match_id'] for r in results)
            print(f"   🔑 Unique FBRef match IDs: {len(unique_ids)}")
            
            # Check for duplicates
            if len(unique_ids) != len(results):
                print(f"   ⚠️ Found {len(results) - len(unique_ids)} duplicate match IDs")
            
            # Show sample IDs
            print(f"   📝 Sample match IDs:")
            for result in results[:5]:
                print(f"      {result['fbref_match_id']}: {result['teams']} ({result['date']})")
            
            return results
            
        except Exception as e:
            print(f"Error analyzing results: {e}")
            return []

if __name__ == "__main__":
    extractor = FBRefMatchIdExtractor()
    results = extractor.extract_all_fbref_match_ids()
    if results:
        extractor.analyze_results()