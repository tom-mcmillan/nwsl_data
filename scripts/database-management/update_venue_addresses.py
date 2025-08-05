#!/usr/bin/env python3
"""
Update venue table with addresses from Soccer_Venues___Addresses.csv
"""

import pandas as pd
import sqlite3
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def update_venue_addresses(csv_path="/Users/thomasmcmillan/projects/nwsl_data/Soccer_Venues___Addresses.csv", 
                          db_path="/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"):
    """Update venue table with addresses from CSV file."""
    
    # Read the CSV file
    try:
        df = pd.read_csv(csv_path)
        logging.info(f"Loaded {len(df)} venue addresses from CSV")
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        return
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get current venues from database
    cursor.execute("SELECT venue_id, venue_name, venue_location FROM venue")
    db_venues = cursor.fetchall()
    
    logging.info(f"Found {len(db_venues)} venues in database")
    
    updates_made = 0
    exact_matches = 0
    fuzzy_matches = 0
    no_matches = 0
    
    # Process each venue from CSV
    for _, row in df.iterrows():
        csv_venue_name = row['Venue'].strip()
        csv_address = row['Street Address'].strip()
        
        if pd.isna(csv_venue_name) or pd.isna(csv_address):
            continue
            
        # Try to find matching venue in database
        matched_venue_id = None
        match_type = None
        
        # First try exact match
        for venue_id, db_venue_name, current_address in db_venues:
            if csv_venue_name == db_venue_name:
                matched_venue_id = venue_id
                match_type = "exact"
                break
        
        # If no exact match, try fuzzy matching
        if not matched_venue_id:
            for venue_id, db_venue_name, current_address in db_venues:
                # Check if venue names are similar (contains each other)
                if (csv_venue_name.lower() in db_venue_name.lower() or 
                    db_venue_name.lower() in csv_venue_name.lower()):
                    matched_venue_id = venue_id
                    match_type = "fuzzy"
                    break
        
        if matched_venue_id:
            # Update the venue address
            cursor.execute("""
                UPDATE venue 
                SET venue_location = ? 
                WHERE venue_id = ?
            """, (csv_address, matched_venue_id))
            
            updates_made += 1
            if match_type == "exact":
                exact_matches += 1
                logging.info(f"✓ EXACT: Updated '{csv_venue_name}' -> {csv_address}")
            else:
                fuzzy_matches += 1
                logging.info(f"≈ FUZZY: Updated '{csv_venue_name}' -> {csv_address}")
        else:
            no_matches += 1
            logging.warning(f"✗ NO MATCH: Could not find venue '{csv_venue_name}' in database")
    
    # Commit changes
    conn.commit()
    conn.close()
    
    # Summary
    logging.info(f"\n{'='*50}")
    logging.info(f"VENUE ADDRESS UPDATE SUMMARY")
    logging.info(f"{'='*50}")
    logging.info(f"Total CSV venues: {len(df)}")
    logging.info(f"Updates made: {updates_made}")
    logging.info(f"  - Exact matches: {exact_matches}")
    logging.info(f"  - Fuzzy matches: {fuzzy_matches}")
    logging.info(f"No matches found: {no_matches}")
    
    return updates_made

def verify_updates(db_path="/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"):
    """Verify the venue address updates."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check venues with and without addresses
    cursor.execute("SELECT COUNT(*) FROM venue WHERE venue_location IS NOT NULL AND venue_location != ''")
    venues_with_address = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM venue")
    total_venues = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM venue WHERE venue_location IS NULL OR venue_location = ''")
    venues_without_address = cursor.fetchone()[0]
    
    # Show some examples
    cursor.execute("""
        SELECT venue_name, venue_location 
        FROM venue 
        WHERE venue_location IS NOT NULL AND venue_location != ''
        ORDER BY venue_name
        LIMIT 10
    """)
    examples = cursor.fetchall()
    
    conn.close()
    
    print(f"\n🏟️ VENUE ADDRESS VERIFICATION")
    print(f"Total venues: {total_venues}")
    print(f"Venues with addresses: {venues_with_address}")
    print(f"Venues without addresses: {venues_without_address}")
    print(f"Coverage: {100*venues_with_address/total_venues:.1f}%")
    
    print(f"\nExample venue addresses:")
    for venue_name, address in examples:
        print(f"  {venue_name}: {address}")

def main():
    """Main function to update venue addresses."""
    # Update addresses from CSV
    updates_made = update_venue_addresses()
    
    # Verify the updates
    verify_updates()
    
    print(f"\n✅ Successfully updated {updates_made} venue addresses!")

if __name__ == "__main__":
    main()