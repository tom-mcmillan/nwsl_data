#!/usr/bin/env python3
"""
Create team_venue_region table to link current NWSL teams with their venues and regions
"""

import hashlib
import sqlite3


def create_table(conn):
    """Create the team_venue_region table"""
    cursor = conn.cursor()

    create_sql = """
    CREATE TABLE IF NOT EXISTS team_venue_region (
        tvr_id TEXT PRIMARY KEY,
        team_id TEXT NOT NULL,
        venue_id TEXT NOT NULL,
        region_id TEXT NOT NULL,
        
        FOREIGN KEY (team_id) REFERENCES team(team_id),
        FOREIGN KEY (venue_id) REFERENCES venue(venue_id),
        FOREIGN KEY (region_id) REFERENCES region(region_id),
        
        UNIQUE(team_id, venue_id, region_id)
    );
    """

    cursor.execute(create_sql)
    conn.commit()
    print("team_venue_region table created successfully")


def generate_tvr_id(team_name, venue_name, region_city):
    """Generate hex ID for team-venue-region combination"""
    combined = f"{team_name}{venue_name}{region_city}"
    hash_obj = hashlib.md5(combined.encode())
    hex_hash = hash_obj.hexdigest()[:8]
    return f"tvr_{hex_hash}"


def insert_mappings(conn):
    """Insert the 14 current NWSL team mappings"""
    cursor = conn.cursor()

    # Based on the screenshot, here are the current team mappings:
    mappings = [
        # (team_name, venue_name, region_city)
        ("Angel City FC", "BMO Stadium", "Los Angeles"),
        ("Bay FC", "PayPal Park", "San Jose"),
        ("Chicago Stars FC", "SeatGeek Stadium", "Bridgeview"),
        ("Houston Dash", "Shell Energy Stadium", "Houston"),
        ("Kansas City Current", "CPKC Stadium", "Kansas City"),
        ("Gotham FC", "Sports Illustrated Stadium", "Harrison"),
        ("North Carolina Courage", "WakeMed Soccer Park", "Cary"),
        ("Orlando Pride", "Inter&amp;Co Stadium", "Orlando"),
        ("Portland Thorns FC", "Providence Park", "Portland"),
        ("Racing Louisville", "Lynn Family Stadium", "Louisville"),
        ("San Diego Wave FC", "Snapdragon Stadium", "San Diego"),
        ("Seattle Reign FC", "Lumen Field", "Seattle"),
        ("Utah Royals", "America First Field", "Sandy"),
        ("Washington Spirit", "Audi Field", "Washington"),
    ]

    # Get the actual IDs from database
    for team_name, venue_name, region_city in mappings:
        # Get team_id
        cursor.execute("SELECT team_id FROM team WHERE team_name = ?", (team_name,))
        team_result = cursor.fetchone()
        if not team_result:
            print(f"Warning: Team '{team_name}' not found")
            continue
        team_id = team_result[0]

        # Get venue_id (venue names in database include location)
        cursor.execute("SELECT venue_id FROM venue WHERE venue_name LIKE ?", (f"%{venue_name}%",))
        venue_result = cursor.fetchone()
        if not venue_result:
            print(f"Warning: Venue '{venue_name}' not found")
            continue
        venue_id = venue_result[0]

        # Get region_id
        cursor.execute("SELECT region_id FROM region WHERE city = ?", (region_city,))
        region_result = cursor.fetchone()
        if not region_result:
            print(f"Warning: Region '{region_city}' not found")
            continue
        region_id = region_result[0]

        # Generate tvr_id
        tvr_id = generate_tvr_id(team_name, venue_name, region_city)

        # Insert the mapping
        try:
            cursor.execute(
                """
                INSERT INTO team_venue_region (tvr_id, team_id, venue_id, region_id)
                VALUES (?, ?, ?, ?)
            """,
                (tvr_id, team_id, venue_id, region_id),
            )
            print(f"✓ {tvr_id}: {team_name} -> {venue_name} -> {region_city}")
        except sqlite3.IntegrityError as e:
            print(f"Error inserting {team_name}: {e}")

    conn.commit()


def verify_mappings(conn):
    """Verify the inserted mappings"""
    cursor = conn.cursor()

    cursor.execute("""
        SELECT tvr.tvr_id, t.team_name, v.venue_name, r.city, r.state
        FROM team_venue_region tvr
        JOIN team t ON tvr.team_id = t.team_id
        JOIN venue v ON tvr.venue_id = v.venue_id
        JOIN region r ON tvr.region_id = r.region_id
        ORDER BY t.team_name
    """)

    results = cursor.fetchall()
    print(f"\nTotal mappings created: {len(results)}")
    print("\nTeam-Venue-Region mappings:")
    for row in results:
        print(f"  {row[0]}: {row[1]} -> {row[2]} -> {row[3]}, {row[4]}")


def main():
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"

    conn = sqlite3.connect(db_path)

    try:
        create_table(conn)
        insert_mappings(conn)
        verify_mappings(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
