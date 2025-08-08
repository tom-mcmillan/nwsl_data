#!/usr/bin/env python3
"""
Get weather data for NWSL matches using Wolfram Alpha API.
Queries weather conditions for each match at venue location during match time.
"""

import csv
import logging
import os
import re
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
from datetime import timedelta
from urllib.parse import urlencode

import requests
from dateutil import parser as dateparser

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def parse_time(date_str, time_str):
    """
    Parse date+time into a single datetime.
    Accepts things like '3/15/25' + '12:45', Excel serial numbers, or 'March 15 2025' + '12:45'.
    """
    try:
        # Handle Excel serial date numbers (common range 40000-50000 for 2009-2037)
        if isinstance(date_str, int | float) or (isinstance(date_str, str) and date_str.isdigit()):
            serial_num = int(date_str)
            if 40000 <= serial_num <= 50000:  # Reasonable range for NWSL dates
                # Excel epoch starts at 1900-01-01, but Excel incorrectly treats 1900 as a leap year
                # So we use 1899-12-30 as the base date
                from datetime import datetime, timedelta

                base_date = datetime(1899, 12, 30)
                actual_date = base_date + timedelta(days=serial_num)
                date_str = actual_date.strftime("%m/%d/%Y")
                logging.info(f"Converted Excel serial {serial_num} to {date_str}")

        return dateparser.parse(f"{date_str} {time_str}")
    except Exception as e:
        logging.error(f"Error parsing date/time '{date_str} {time_str}': {e}")
        return None


def extract_city_state(address):
    """Extract city and state from full address for weather queries."""
    if not address:
        return "Unknown"

    # Split by commas and get the city and state parts
    parts = [part.strip() for part in address.split(",")]
    if len(parts) >= 2:
        # For addresses like "Street Address, City, State ZIP"
        if len(parts) >= 3:
            city = parts[-2]  # Second to last is city
            state_part = parts[-1]  # Last part has state and zip
        else:
            # For addresses like "City, State ZIP"
            city = parts[-2] if len(parts) >= 2 else parts[0]
            state_part = parts[-1]
        # Extract state abbreviation (looking for 2-letter state codes)
        state_match = re.search(r"\b([A-Z]{2})\b", state_part)
        state = state_match.group(1) if state_match else state_part.split()[0]
        return f"{city} {state}" if state else city

    return address  # Fallback to full address


def query_wolfram_alpha_api(app_id, query):
    """
    Make direct HTTP request to Wolfram Alpha API.
    Returns XML response or None if error.
    """
    try:
        base_url = "https://api.wolframalpha.com/v2/query"
        params = {"input": query, "appid": app_id, "output": "XML", "format": "plaintext"}

        url = f"{base_url}?{urlencode(params)}"
        logging.info(f"Making API request: {query}")

        response = requests.get(url, timeout=30)
        response.raise_for_status()

        return response.text

    except Exception as e:
        logging.error(f"Error making API request: {e}")
        return None


def parse_wolfram_xml(xml_content):
    """
    Parse Wolfram Alpha XML response to extract weather data.
    Returns dictionary with weather information.
    """
    try:
        root = ET.fromstring(xml_content)
        weather_data = {}

        # Look for pods with weather information
        for pod in root.findall(".//pod"):
            title = pod.get("title", "").lower()
            logging.info(f"Found pod: {pod.get('title', 'Unknown')}")

            # Check if this pod contains weather information
            if any(keyword in title for keyword in ["weather", "conditions", "temperature", "current", "historical"]):
                for subpod in pod.findall(".//subpod"):
                    plaintext_elem = subpod.find("plaintext")
                    if plaintext_elem is not None and plaintext_elem.text:
                        text = plaintext_elem.text.strip()
                        logging.info(f"Pod content: {text}")

                        # Parse weather data from text
                        parsed_data = parse_weather_text(text)
                        weather_data.update(parsed_data)

        return weather_data

    except Exception as e:
        logging.error(f"Error parsing XML response: {e}")
        return {}


def get_weather_data(app_id, address, dt_start, dt_end):
    """
    Query Wolfram Alpha for weather data between dt_start and dt_end.
    Returns weather information as a dictionary.
    """
    try:
        # Extract city and state from address for simpler queries
        city_state = extract_city_state(address)

        # Use formats that match Wolfram Alpha examples from documentation
        queries = [
            f"weather in {city_state} on {dt_start.strftime('%B %d, %Y')}",
            f"weather {city_state} {dt_start.strftime('%B %d, %Y')}",
            f"historical weather {city_state} {dt_start.strftime('%B %d, %Y')}",
            f"weather conditions {city_state} {dt_start.strftime('%B %d %Y')}",
        ]

        for query in queries:
            xml_response = query_wolfram_alpha_api(app_id, query)
            if xml_response:
                weather_data = parse_wolfram_xml(xml_response)
                if weather_data:
                    logging.info(f"✓ Successfully parsed weather data: {weather_data}")
                    return weather_data

            # Rate limiting between query attempts
            time.sleep(1)

        return {}

    except Exception as e:
        logging.error(f"Error querying Wolfram Alpha: {e}")
        return {}


def parse_weather_text(text):
    """
    Parse weather information from Wolfram Alpha text response.
    Extract temperature, humidity, and precipitation data.
    """
    weather_data = {}
    lines = text.split("\n")

    for line in lines:
        line = line.strip().lower()

        # Temperature parsing
        if "°f" in line or "temperature" in line:
            temp_patterns = [
                r"(\d+)\s*°f",
                r"(\d+)\s*degrees fahrenheit",
                r"temperature.*?(\d+)",
                r"average:\s*(\d+)\s*°f",
                r"\((\d+)\s*to\s*\d+\)\s*°f",
            ]
            for pattern in temp_patterns:
                match = re.search(pattern, line)
                if match:
                    weather_data["temperature_f"] = int(match.group(1))
                    break

        # Humidity parsing
        if "%" in line and ("humidity" in line or "relative" in line):
            humidity_patterns = [
                r"(\d+)\s*%",
                r"humidity.*?(\d+)",
                r"average:\s*(\d+)%",
                r"\((\d+)\s*to\s*\d+\)%",
            ]
            for pattern in humidity_patterns:
                match = re.search(pattern, line)
                if match:
                    weather_data["humidity_pct"] = int(match.group(1))
                    break

        # Precipitation parsing
        if any(word in line for word in ["rain", "precipitation", "inches", "mm"]):
            precip_patterns = [
                r"(\d+\.?\d*)\s*(?:in|inches)",
                r"(\d+\.?\d*)\s*mm",
                r"precipitation.*?(\d+\.?\d*)",
            ]
            for pattern in precip_patterns:
                match = re.search(pattern, line)
                if match:
                    precip_val = float(match.group(1))
                    # Convert mm to inches if needed
                    if "mm" in line:
                        precip_val = precip_val / 25.4
                    weather_data["precipitation_in"] = round(precip_val, 2)
                    break

        # Wind speed parsing
        if "wind" in line and ("mph" in line or "km/h" in line):
            wind_patterns = [
                r"(\d+)\s*mph",
                r"(\d+)\s*km/h",
                r"wind.*?(\d+)",
                r"average:\s*(\d+)\s*mph",
                r"\((\d+)\s*to\s*\d+\)\s*mph",
            ]
            for pattern in wind_patterns:
                match = re.search(pattern, line)
                if match:
                    wind_val = int(match.group(1))
                    # Convert km/h to mph if needed
                    if "km/h" in line:
                        wind_val = int(wind_val * 0.621371)
                    weather_data["wind_speed_mph"] = wind_val
                    break

    return weather_data


def export_matches_to_csv(db_path, csv_path):
    """Export match data to CSV for weather lookup."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT weather_id, venue_location, date, start_time, end_time
        FROM match_venue_weather
        WHERE venue_location IS NOT NULL 
        AND start_time IS NOT NULL
        ORDER BY date, start_time
    """)

    matches = cursor.fetchall()
    conn.close()

    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(["weather_id", "address", "date", "start_time", "end_time"])
        # Ensure date values are treated as strings
        for match in matches:
            writer.writerow([str(field) if field is not None else "" for field in match])

    logging.info(f"Exported {len(matches)} matches to {csv_path}")
    return len(matches)


def update_weather_data_in_db(db_path, weather_results):
    """Update the match_venue_weather table with weather data."""
    if not weather_results:
        logging.warning("No weather results to update")
        return 0

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Add weather columns if they don't exist
    weather_columns = [
        "temperature_f INTEGER",
        "humidity_pct INTEGER",
        "precipitation_in REAL",
        "wind_speed_mph INTEGER",
    ]

    for col_def in weather_columns:
        col_name = col_def.split()[0]
        try:
            cursor.execute(f"ALTER TABLE match_venue_weather ADD COLUMN {col_def}")
            logging.info(f"Added column {col_name}")
        except sqlite3.OperationalError:
            # Column already exists
            pass

    updated_count = 0

    for weather_id, weather_data in weather_results.items():
        cursor.execute(
            """
            UPDATE match_venue_weather 
            SET temperature_f = ?, humidity_pct = ?, precipitation_in = ?, wind_speed_mph = ?
            WHERE weather_id = ?
        """,
            (
                weather_data.get("temperature_f"),
                weather_data.get("humidity_pct"),
                weather_data.get("precipitation_in"),
                weather_data.get("wind_speed_mph"),
                weather_id,
            ),
        )

        if cursor.rowcount > 0:
            updated_count += 1

    conn.commit()
    conn.close()

    logging.info(f"Updated weather data for {updated_count} matches")
    return updated_count


def main():
    """Main function to get weather data for NWSL matches."""

    # Check for command line arguments for batch processing
    batch_size = 50  # Process 50 matches at a time
    start_offset = 0

    if len(sys.argv) > 1:
        start_offset = int(sys.argv[1])
    if len(sys.argv) > 2:
        batch_size = int(sys.argv[2])

    # Check for Wolfram Alpha API key
    app_id = os.getenv("WOLFRAM_APPID")
    if not app_id:
        print(
            "Error: Please set WOLFRAM_APPID environment variable with your API key",
            file=sys.stderr,
        )
        print("Get your API key at: https://developer.wolframalpha.com/", file=sys.stderr)
        sys.exit(1)

    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    csv_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/matches_for_weather.csv"

    # Export matches to CSV for processing
    match_count = export_matches_to_csv(db_path, csv_path)

    if match_count == 0:
        logging.error("No matches found to process")
        return

    # Use app_id directly for API calls
    weather_results = {}

    with open(csv_path) as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)
        total_rows = len(all_rows)

        # Apply batch processing
        end_offset = min(start_offset + batch_size, total_rows)
        batch_rows = all_rows[start_offset:end_offset]

        print(f"Processing batch: {start_offset+1}-{end_offset} of {total_rows} total matches")

        processed = 0
        successful = 0

        for row in batch_rows:
            weather_id = row["weather_id"]
            address = row["address"]
            date_str = row["date"]
            start_time = row["start_time"]

            processed += 1
            overall_processed = start_offset + processed

            logging.info(f"Processing {overall_processed}/{total_rows}: {address} on {date_str} at {start_time}")

            # Parse datetime
            dt_start = parse_time(date_str, start_time)
            if not dt_start:
                logging.warning(f"Could not parse date/time for {weather_id}")
                continue

            dt_end = dt_start + timedelta(hours=2)

            # Get weather data
            weather_data = get_weather_data(app_id, address, dt_start, dt_end)

            if weather_data:
                weather_results[weather_id] = weather_data
                successful += 1
                logging.info(f"✓ Got weather data: {weather_data}")
            else:
                logging.warning(f"✗ No weather data found for {weather_id}")

            # Rate limiting - wait between API calls
            time.sleep(2.0)  # 2 second delay between calls

            # Progress update every 10 matches
            if processed % 10 == 0:
                logging.info(f"Batch progress: {processed}/{len(batch_rows)} processed, {successful} successful")

    # Update database with weather results
    updated_count = update_weather_data_in_db(db_path, weather_results)

    print("\n🌤️ WEATHER DATA BATCH COMPLETE!")
    print(f"Batch range: {start_offset+1}-{end_offset} of {total_rows}")
    print(f"Batch matches processed: {processed}")
    print(f"Successful weather lookups: {successful}")
    print(f"Database records updated: {updated_count}")
    print(f"Batch success rate: {100*successful/processed:.1f}%" if processed > 0 else "N/A")

    if end_offset >= total_rows:
        print("\n✅ ALL MATCHES PROCESSED!")
        # Clean up temporary CSV only when completely done
        os.remove(csv_path)
        logging.info("Cleaned up temporary CSV file")
    else:
        print(f"\n➡️  Next batch: python3 scripts/get_match_weather_data.py {end_offset}")
        print(f"Remaining: {total_rows - end_offset} matches")


if __name__ == "__main__":
    main()
