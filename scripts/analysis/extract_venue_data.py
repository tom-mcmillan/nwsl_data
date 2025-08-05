#!/usr/bin/env python3
"""
Extract venue information from FBRef match HTML files.
Creates comprehensive venue database with locations and altitudes.
"""

import os
import re
import hashlib
import sqlite3
import logging
from pathlib import Path
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def generate_venue_id(venue_name: str) -> str:
    """Generate unique venue_id with venue_ prefix."""
    hex_hash = hashlib.md5(venue_name.encode()).hexdigest()[:8]
    return f"venue_{hex_hash}"

def extract_venue_from_html(html_file_path):
    """Extract venue information from a single HTML file."""
    try:
        with open(html_file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        # Look for venue information in the HTML
        # Pattern: <div><strong><small>Venue</small></strong>: <small>VENUE_NAME</small></div>
        venue_pattern = r'<div><strong><small>Venue</small></strong>:\s*<small>([^<]+)</small></div>'
        venue_match = re.search(venue_pattern, html_content)
        
        if venue_match:
            venue_name = venue_match.group(1).strip()
            return venue_name
        
        return None
        
    except Exception as e:
        logging.error(f"Error processing {html_file_path}: {e}")
        return None

def get_venue_details(venue_name):
    """Get detailed venue information including address and altitude."""
    # Comprehensive venue database with research from web search
    venue_database = {
        # Current NWSL Stadiums (2024-2025)
        "BMO Stadium, Los Angeles": {
            "venue_location": "1939 S Figueroa St, Los Angeles, CA 90007",
            "venue_altitude_ft": 285,  # feet above sea level
            "city": "Los Angeles",
            "state": "CA",
            "capacity": 22000,
            "surface_type": "Bermuda grass",
            "opened_year": 2018
        },
        "PayPal Park, San Jose": {
            "venue_location": "1123 Coleman Ave, San Jose, CA 95110", 
            "venue_altitude_ft": 80,
            "city": "San Jose",
            "state": "CA", 
            "capacity": 18000,
            "surface_type": "SISGrass hybrid",
            "opened_year": 2015
        },
        "SeatGeek Stadium, Bridgeview": {
            "venue_location": "7000 S Harlem Ave, Bridgeview, IL 60455",
            "venue_altitude_ft": 620,
            "city": "Bridgeview", 
            "state": "IL",
            "capacity": 20000,
            "surface_type": "Kentucky bluegrass",
            "opened_year": 2006
        },
        "Shell Energy Stadium, Houston": {
            "venue_location": "2200 Texas St, Houston, TX 77003",
            "venue_altitude_ft": 50,
            "city": "Houston",
            "state": "TX",
            "capacity": 22039,
            "surface_type": "Bermuda grass", 
            "opened_year": 2012
        },
        "CPKC Stadium, Kansas City": {
            "venue_location": "1 CPKC Way, Kansas City, KS 66111",
            "venue_altitude_ft": 750,
            "city": "Kansas City",
            "state": "KS",
            "capacity": 11500,
            "surface_type": "Grass",
            "opened_year": 2024
        },
        "Red Bull Arena, Harrison": {
            "venue_location": "600 Cape May St, Harrison, NJ 07029",
            "venue_altitude_ft": 100,
            "city": "Harrison",
            "state": "NJ", 
            "capacity": 25000,
            "surface_type": "FieldTurf",
            "opened_year": 2010
        },
        "WakeMed Soccer Park, Cary": {
            "venue_location": "201 Soccer Park Dr, Cary, NC 27511",
            "venue_altitude_ft": 400,
            "city": "Cary",
            "state": "NC",
            "capacity": 10000,
            "surface_type": "Grass",
            "opened_year": 2002
        },
        "Inter&Co Stadium, Orlando": {
            "venue_location": "655 W Church St, Orlando, FL 32805",
            "venue_altitude_ft": 100,
            "city": "Orlando", 
            "state": "FL",
            "capacity": 25500,
            "surface_type": "Grass",
            "opened_year": 2017
        },
        "Providence Park, Portland": {
            "venue_location": "1844 SW Morrison St, Portland, OR 97205", 
            "venue_altitude_ft": 165,
            "city": "Portland",
            "state": "OR",
            "capacity": 25218,
            "surface_type": "FieldTurf",
            "opened_year": 1926
        },
        "Lynn Family Stadium, Louisville": {
            "venue_location": "350 Adams St, Louisville, KY 40202",
            "venue_altitude_ft": 460,
            "city": "Louisville",
            "state": "KY", 
            "capacity": 15304,
            "surface_type": "Grass",
            "opened_year": 2020
        },
        "Snapdragon Stadium, San Diego": {
            "venue_location": "5500 Canyon Crest Dr, San Diego, CA 92182",
            "venue_altitude_ft": 460,
            "city": "San Diego",
            "state": "CA",
            "capacity": 35000,
            "surface_type": "Grass", 
            "opened_year": 2022
        },
        "Lumen Field, Seattle": {
            "venue_location": "800 Occidental Ave S, Seattle, WA 98134",
            "venue_altitude_ft": 170,
            "city": "Seattle",
            "state": "WA",
            "capacity": 68740,
            "surface_type": "FieldTurf",
            "opened_year": 2002
        },
        "America First Field, Sandy": {
            "venue_location": "9256 S State St, Sandy, UT 84070",
            "venue_altitude_ft": 4463,  # Highest altitude in NWSL!
            "city": "Sandy",
            "state": "UT", 
            "capacity": 20213,
            "surface_type": "Grass",
            "opened_year": 2008
        },
        "Audi Field, Washington": {
            "venue_location": "100 Potomac Ave SW, Washington, DC 20024",
            "venue_altitude_ft": 50,
            "city": "Washington",
            "state": "DC",
            "capacity": 20000,
            "surface_type": "Grass",
            "opened_year": 2018
        },
        
        # Historical/Secondary Venues
        "Yurcak Field, Piscataway": {
            "venue_location": "83 Rockafeller Rd, Piscataway, NJ 08854",
            "venue_altitude_ft": 130,
            "city": "Piscataway", 
            "state": "NJ",
            "capacity": 5000,
            "surface_type": "Grass",
            "opened_year": 1994
        },
        "Maryland SoccerPlex, Germantown": {
            "venue_location": "18031 Central Park Cir, Boyds, MD 20841",
            "venue_altitude_ft": 320,
            "city": "Boyds",
            "state": "MD", 
            "capacity": 5000,
            "surface_type": "Grass",
            "opened_year": 2001
        },
        "Rentschler Field, East Hartford": {
            "venue_location": "615 Silver Ln, East Hartford, CT 06118",
            "venue_altitude_ft": 180,
            "city": "East Hartford",
            "state": "CT",
            "capacity": 40000,
            "surface_type": "FieldTurf",
            "opened_year": 2003
        },
        "Sahlen's Stadium, Rochester": {
            "venue_location": "460 Oak St, Rochester, NY 14608", 
            "venue_altitude_ft": 510,
            "city": "Rochester",
            "state": "NY",
            "capacity": 13768,
            "surface_type": "Grass",
            "opened_year": 2006
        },
        "Wrigley Field, Chicago": {
            "venue_location": "1060 W Addison St, Chicago, IL 60613",
            "venue_altitude_ft": 600,
            "city": "Chicago",
            "state": "IL",
            "capacity": 41649,
            "surface_type": "Grass",
            "opened_year": 1914
        }
    }
    
    # Try exact match first
    if venue_name in venue_database:
        return venue_database[venue_name]
    
    # Try fuzzy matching for venues with slight variations
    for db_venue, details in venue_database.items():
        if venue_name.lower() in db_venue.lower() or db_venue.lower() in venue_name.lower():
            return details
    
    # Return basic info for unknown venues
    return {
        "venue_location": None,
        "venue_altitude_ft": None,
        "city": None,
        "state": None,
        "capacity": None,
        "surface_type": None,
        "opened_year": None
    }

def extract_venues_from_all_matches(html_files_dir="/Users/thomasmcmillan/projects/nwsl_data/notebooks/match_html_files"):
    """Extract venue information from all match HTML files."""
    html_files_path = Path(html_files_dir)
    html_files = list(html_files_path.glob("match_*.html"))
    
    venues_found = {}
    matches_processed = 0
    
    logging.info(f"Processing {len(html_files)} HTML match files")
    
    for html_file in html_files:
        matches_processed += 1
        venue_name = extract_venue_from_html(html_file)
        
        if venue_name:
            if venue_name not in venues_found:
                venues_found[venue_name] = 0
            venues_found[venue_name] += 1
            
            if matches_processed % 100 == 0:
                logging.info(f"Processed {matches_processed} files, found {len(venues_found)} unique venues")
    
    logging.info(f"Extraction complete: {matches_processed} matches processed, {len(venues_found)} unique venues found")
    
    # Show venue frequency
    sorted_venues = sorted(venues_found.items(), key=lambda x: x[1], reverse=True)
    logging.info("Top venues by match frequency:")
    for venue, count in sorted_venues[:10]:
        logging.info(f"  {venue}: {count} matches")
    
    return venues_found

def populate_venue_table(venues_found, db_path="/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"):
    """Populate the venue table with comprehensive venue data."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    venues_added = 0
    
    for venue_name in venues_found.keys():
        venue_id = generate_venue_id(venue_name)
        venue_details = get_venue_details(venue_name)
        
        cursor.execute("""
            INSERT OR REPLACE INTO venue (
                venue_id, venue_name, venue_location, venue_altitude_ft,
                city, state, capacity, surface_type, opened_year
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            venue_id,
            venue_name,
            venue_details["venue_location"],
            venue_details["venue_altitude_ft"],
            venue_details["city"],
            venue_details["state"],
            venue_details["capacity"],
            venue_details["surface_type"],
            venue_details["opened_year"]
        ))
        
        venues_added += 1
    
    conn.commit()
    conn.close()
    
    logging.info(f"Successfully added {venues_added} venues to the database")
    return venues_added

def main():
    """Main function to extract and populate venue data."""
    # Extract venues from HTML files
    venues_found = extract_venues_from_all_matches()
    
    # Populate venue table
    venues_added = populate_venue_table(venues_found)
    
    # Verify results
    conn = sqlite3.connect("/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db")
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM venue")
    total_venues = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM venue WHERE venue_altitude_ft IS NOT NULL")
    venues_with_altitude = cursor.fetchone()[0]
    
    cursor.execute("SELECT venue_name, venue_altitude_ft FROM venue ORDER BY venue_altitude_ft DESC LIMIT 5")
    highest_venues = cursor.fetchall()
    
    conn.close()
    
    print(f"\n🏟️ VENUE EXTRACTION COMPLETE!")
    print(f"Total venues in database: {total_venues}")
    print(f"Venues with altitude data: {venues_with_altitude}")
    print(f"\nHighest altitude venues:")
    for venue_name, altitude in highest_venues:
        if altitude:
            print(f"  {venue_name}: {altitude} feet above sea level")

if __name__ == "__main__":
    main()