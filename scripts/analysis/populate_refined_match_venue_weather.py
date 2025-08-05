#!/usr/bin/env python3
"""
Populate refined match_venue_weather table with match data, venues, and timing.
Extracts kickoff times from unprocessed CSV files and calculates 2-hour match duration.
"""

import os
import re
import hashlib
import sqlite3
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_weather_id(match_id: str, venue_id: str) -> str:
    """Generate unique weather record ID."""
    content = f"weather_{match_id}_{venue_id}"
    hex_hash = hashlib.md5(content.encode()).hexdigest()[:8]
    return f"weather_{hex_hash}"

def extract_local_time(time_str):
    """Extract local time from format like '11:45 (12:45)' -> '12:45'"""
    if not time_str or pd.isna(time_str):
        return None
    
    # Look for time in parentheses (local venue time)
    match = re.search(r'\((\d{1,2}:\d{2})\)', str(time_str))
    if match:
        return match.group(1)
    
    # If no parentheses, use the time as-is
    time_match = re.search(r'(\d{1,2}:\d{2})', str(time_str))
    if time_match:
        return time_match.group(1)
    
    return None

def calculate_end_time(start_time_str):
    """Calculate end time as 2 hours after start time."""
    if not start_time_str:
        return None
    
    try:
        # Parse time and add 2 hours
        start_time = datetime.strptime(start_time_str, '%H:%M')
        end_time = start_time + timedelta(hours=2)
        return end_time.strftime('%H:%M')
    except:
        return None

def load_match_timing_data(unprocessed_dir="/Users/thomasmcmillan/projects/nwsl_data/data/unprocessed"):
    """Load all match timing data from unprocessed CSV files."""
    unprocessed_path = Path(unprocessed_dir)
    csv_files = list(unprocessed_path.glob("*.csv"))
    
    all_matches = []
    
    for csv_file in csv_files:
        if csv_file.name.startswith(('20', '2013')):  # Year-based files
            try:
                logging.info(f"Loading {csv_file.name}")
                df = pd.read_csv(csv_file, encoding='utf-8-sig')  # Handle BOM
                
                # Standardize column names (some files might have different cases)
                df.columns = df.columns.str.strip()
                
                # Look for required columns with flexible naming
                match_id_col = None
                time_col = None
                date_col = None
                
                for col in df.columns:
                    if 'match id' in col.lower() or 'match_id' == col.lower():
                        match_id_col = col
                    elif 'time' in col.lower():
                        time_col = col
                    elif 'date' in col.lower():
                        date_col = col
                
                if match_id_col and time_col and date_col:
                    # Extract relevant columns
                    matches = df[[match_id_col, date_col, time_col]].copy()
                    matches.columns = ['match_id', 'date', 'time']
                    matches = matches.dropna(subset=['match_id'])
                    all_matches.append(matches)
                    logging.info(f"  Added {len(matches)} matches from {csv_file.name}")
                else:
                    logging.warning(f"  Missing required columns in {csv_file.name}")
                    
            except Exception as e:
                logging.error(f"Error processing {csv_file.name}: {e}")
    
    if all_matches:
        combined_df = pd.concat(all_matches, ignore_index=True)
        logging.info(f"Total matches loaded: {len(combined_df)}")
        return combined_df
    else:
        logging.error("No match data loaded")
        return pd.DataFrame()

def get_venue_id_by_name(venue_name, db_path):
    """Get venue_id from venue table by venue name."""
    if not venue_name:
        return None
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Try exact match first
    cursor.execute("SELECT venue_id FROM venue WHERE venue_name = ?", (venue_name,))
    result = cursor.fetchone()
    
    if result:
        conn.close()
        return result[0]
    
    # Try fuzzy matching
    cursor.execute("SELECT venue_id, venue_name FROM venue")
    all_venues = cursor.fetchall()
    
    for venue_id, db_venue_name in all_venues:
        if venue_name.lower() in db_venue_name.lower() or db_venue_name.lower() in venue_name.lower():
            conn.close()
            return venue_id
    
    conn.close()
    return None

def extract_venue_from_match_html(match_id, html_files_dir="/Users/thomasmcmillan/projects/nwsl_data/notebooks/match_html_files"):
    """Extract venue name from a match HTML file."""
    html_file_path = Path(html_files_dir) / f"match_{match_id}.html"
    
    if not html_file_path.exists():
        return None
    
    try:
        with open(html_file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Look for venue information
        venue_pattern = r'<div><strong><small>Venue</small></strong>:\s*<small>([^<]+)</small></div>'
        venue_match = re.search(venue_pattern, html_content)
        
        if venue_match:
            return venue_match.group(1).strip()
        
        return None
        
    except Exception as e:
        logging.error(f"Error processing match {match_id}: {e}")
        return None

def populate_refined_match_venue_weather(db_path="/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"):
    """Populate the refined match_venue_weather table."""
    
    # Load match timing data from unprocessed files
    match_timing_df = load_match_timing_data()
    
    if match_timing_df.empty:
        logging.error("No match timing data loaded")
        return 0
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    records_added = 0
    venues_found = 0
    timing_extracted = 0
    
    logging.info(f"Processing {len(match_timing_df)} matches from CSV files")
    
    for _, row in match_timing_df.iterrows():
        match_id = str(row['match_id']).strip()
        match_date = str(row['date']).strip()
        time_str = str(row['time']).strip()
        
        # Extract venue from HTML file
        venue_name = extract_venue_from_match_html(match_id)
        
        if venue_name:
            # Get venue_id and location
            venue_id = get_venue_id_by_name(venue_name, db_path)
            
            if venue_id:
                # Get venue location
                cursor.execute("SELECT venue_location FROM venue WHERE venue_id = ?", (venue_id,))
                venue_location_result = cursor.fetchone()
                venue_location = venue_location_result[0] if venue_location_result else None
                
                # Extract local kickoff time
                start_time = extract_local_time(time_str)
                end_time = calculate_end_time(start_time) if start_time else None
                
                # Generate weather_id
                weather_id = generate_weather_id(match_id, venue_id)
                
                # Insert record
                cursor.execute("""
                    INSERT OR REPLACE INTO match_venue_weather (
                        weather_id, match_id, venue_id, venue_location,
                        date, start_time, end_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (weather_id, match_id, venue_id, venue_location,
                      match_date, start_time, end_time))
                
                records_added += 1
                venues_found += 1
                
                if start_time:
                    timing_extracted += 1
                
                if records_added % 100 == 0:
                    logging.info(f"Processed {records_added} matches...")
            else:
                logging.warning(f"Match {match_id}: venue '{venue_name}' not found in venue table")
        else:
            logging.warning(f"Match {match_id}: no venue information found")
    
    conn.commit()
    conn.close()
    
    logging.info(f"Successfully added {records_added} match-venue-weather records")
    logging.info(f"Venues found: {venues_found}")
    logging.info(f"Timing extracted: {timing_extracted}")
    
    return records_added

def main():
    """Main function to populate refined match-venue-weather data."""
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    
    # Populate refined data
    records_added = populate_refined_match_venue_weather(db_path)
    
    # Verify results
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM match_venue_weather")
    total_records = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM match_venue_weather WHERE start_time IS NOT NULL")
    records_with_time = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT venue_id) FROM match_venue_weather")
    unique_venues = cursor.fetchone()[0]
    
    # Sample data
    cursor.execute("""
        SELECT date, start_time, end_time, venue_location
        FROM match_venue_weather 
        WHERE start_time IS NOT NULL
        LIMIT 5
    """)
    samples = cursor.fetchall()
    
    conn.close()
    
    print(f"\n⚽ REFINED MATCH-VENUE-WEATHER TABLE POPULATED!")
    print(f"Total records: {total_records}")
    print(f"Records with kickoff times: {records_with_time}")
    print(f"Unique venues: {unique_venues}")
    print(f"Timing coverage: {100*records_with_time/total_records:.1f}%")
    
    if samples:
        print(f"\nSample match timings:")
        for date, start_time, end_time, venue_location in samples:
            print(f"  {date} {start_time}-{end_time} at {venue_location}")

if __name__ == "__main__":
    main()