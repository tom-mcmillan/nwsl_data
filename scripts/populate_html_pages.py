#!/usr/bin/env python3
"""
Populate the new html_pages table with current HTML file inventory.
"""

import os
import sqlite3
from pathlib import Path
import re
from bs4 import BeautifulSoup

def extract_match_id_from_filename(filename):
    """Extract match ID from filename patterns."""
    # Pattern 1: match_report_YEAR_MATCHID.html
    match = re.search(r'match_report_\d{4}_([a-f0-9]+)\.html', filename)
    if match:
        return match.group(1)
    
    # Pattern 2: MATCHID.html (8-character hex)
    match = re.search(r'^([a-f0-9]{8})\.html$', filename)
    if match:
        return match.group(1)
    
    return None

def extract_year_from_html(filepath):
    """Extract year from HTML content."""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read(5000)  # Read first 5KB
        
        soup = BeautifulSoup(content, 'html.parser')
        
        # Look for year in various places
        year_patterns = [
            r'(\d{4})',  # Any 4-digit year
        ]
        
        # Check title, headers, etc.
        text_elements = [soup.title, soup.find('h1'), soup.find('h2')]
        for element in text_elements:
            if element and element.text:
                for pattern in year_patterns:
                    years = re.findall(pattern, element.text)
                    for year in years:
                        year_int = int(year)
                        if 2013 <= year_int <= 2025:  # NWSL years
                            return year_int
        
        return None
        
    except Exception:
        return None

def analyze_html_file(filepath):
    """Analyze HTML file to determine status and capabilities."""
    try:
        file_size = filepath.stat().st_size
        
        # Very small files are probably errors
        if file_size < 50000:  # Less than 50KB
            return 'error', 0, 0, f"File too small: {file_size} bytes"
        
        # Read content to check for data
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read(10000)  # Read first 10KB
        
        # Check for error indicators
        error_indicators = [
            'Rate Limited Request',
            '429 error',
            'Page Not Found',
            '404 error',
            'Access Denied'
        ]
        
        for indicator in error_indicators:
            if indicator in content:
                return 'error', 0, 0, f"Contains: {indicator}"
        
        # Check for good content indicators
        has_player_stats = 1 if 'stats_table' in content or 'player' in content.lower() else 0
        has_match_summary = 1 if 'Match Report' in content and 'FBref' in content else 0
        
        if has_player_stats or has_match_summary:
            return 'good', has_player_stats, has_match_summary, None
        else:
            return 'unknown', 0, 0, "Content unclear"
            
    except Exception as e:
        return 'error', 0, 0, f"Analysis error: {str(e)}"

def populate_html_pages_table(db_path, base_dir):
    """Populate the html_pages table with current files."""
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    base_path = Path(base_dir)
    html_files = list(base_path.rglob("*.html"))
    
    print(f"📁 Found {len(html_files)} HTML files to process")
    print("=" * 60)
    
    processed = 0
    
    for filepath in html_files:
        # Extract basic info
        filename = filepath.name
        relative_path = str(filepath.relative_to(base_path.parent.parent))
        match_id = extract_match_id_from_filename(filename)
        year = extract_year_from_html(filepath)
        file_size = filepath.stat().st_size
        
        # Analyze file content
        status, has_player_stats, has_match_summary, error_message = analyze_html_file(filepath)
        
        # Insert into database
        cursor.execute("""
            INSERT INTO html_pages 
            (filename, filepath, year, match_id, status, file_size, error_message,
             has_player_stats, has_match_summary, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            filename,
            relative_path,
            year,
            match_id,
            status,
            file_size,
            error_message,
            has_player_stats,
            has_match_summary
        ))
        
        processed += 1
        if processed % 100 == 0:
            print(f"   Processed {processed}/{len(html_files)} files...")
    
    conn.commit()
    
    # Generate summary
    cursor.execute("""
        SELECT status, COUNT(*) as count, 
               ROUND(AVG(file_size), 0) as avg_size,
               MIN(file_size) as min_size,
               MAX(file_size) as max_size
        FROM html_pages 
        GROUP BY status 
        ORDER BY count DESC
    """)
    
    print(f"\n📊 POPULATION COMPLETE:")
    print("=" * 60)
    print("Status       | Count | Avg Size | Min Size | Max Size")
    print("-" * 60)
    
    total_files = 0
    for status, count, avg_size, min_size, max_size in cursor.fetchall():
        total_files += count
        print(f"{status:<12} | {count:>5} | {avg_size:>8.0f} | {min_size:>8} | {max_size:>8}")
    
    print("-" * 60)
    print(f"{'TOTAL':<12} | {total_files:>5}")
    
    conn.close()
    return total_files

def main():
    script_dir = Path(__file__).parent.parent
    db_path = script_dir / 'data/processed/nwsldata.db'
    base_dir = script_dir / 'data/raw_match_pages'
    
    print(f"🗄️  Database: {db_path}")
    print(f"📁 Base directory: {base_dir}")
    print()
    
    total = populate_html_pages_table(db_path, base_dir)
    print(f"\n🎉 Successfully populated html_pages table with {total} records!")

if __name__ == "__main__":
    main()