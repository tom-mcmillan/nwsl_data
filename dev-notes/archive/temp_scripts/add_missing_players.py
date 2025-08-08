#!/usr/bin/env python3
"""
Add missing players to the player table who appear in match_player but not in player table
This will resolve the NULL player_id values in the match_player table
"""

import logging
import sqlite3
import uuid

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# List of missing player names (deduplicated)
MISSING_PLAYERS = [
    "Gabrielle Robinson",
    "Olivia Wade-Katoa",
    "Amanda Allen",
    "Luana",
    "Lysianne Proulx",
    "Maddie Pokorny",
    "Regan Steigleder",
    "Laurel Ivory",
    "McKenzie Weinert",
    "Mya Jones",
    "Maya Doms",
    "Emily Gray",
    "Landy Mertz",
    "Alex Kerr",
    "Lauren",
    "Cloé Lacasse",
    "Hillary Beall",
    "Shaelan Murison",
    "Cristina Roque",
    "Madison Ayson",
    "Jenna Butler",
    "Zoe Matthews",
    "Rosella Ayane",
    "Milly Clegg",
    "Ángela Barón",
    "Heather Hinz",
    "Mille Gejl",
    "Rikke Madsen",
    "Mimmi Larsson",
    "Jenna Walker",
    "Haley Bugeja",
    "Caitlin Cosme",
    "Civana Kuhlmann",
    "Zaneta Wyne",
    "Sami Feller",
    "Rylan Childers",
    "Sydney Collins",
    "Kaylie Collins",
    "Millie Farrow",
    "Marley Canales",
    "Clara Robbins",
    "Madelyn Desiano",
    "Lindsi Jennings",
    "Mackenzie Pluck",
    "Mia Gyau",
    "Chai Cortez",
    "Maliah Morris",
    "Tori Hansen",
    "Brittany Isenhour",
    "Brenna Lovera",
    "Riley Tanner",
    "Thais Reiss",
    "Sophie Jones",
    "Giovanna DeMarco",
    "Emily Alvarado",
    "Kelsey Hill",
    "Cheyenne Shorts",
    "Natalie Viggiano",
    "Jadyn Edwards",
    "Jordan Thompson",
    "Alyssa Walker",
    "Cyera Hintzen",
    "Sarah Clark",
    "Shea Connors",
    "Kayla Morrison",
    "Marleen Schimmer",
    "Sh'nia Gordon",
    "Nicole Baxter",
    "Jada Talley",
    "Chelsie Dawber",
    "Sydney Pulver",
    "Channing Foster",
    "Mikenna McManus",
    "Jacqueline Altschuld",
    "Sophie French",
    "Paulina Gramaglia",
    "Allyson Swaby",
    "Fūka Nagano",
    "Valérie Gauvin",
    "Taylor Hansen",
    "Kelly Livingstone",
    "Jade Moore",
    "Amber Marshall",
    "Yuka Momiki",
    "Jenna Hellstrom",
    "Autumn Smithers",
    "Zoe Redei",
    "Cassie Rohan",
    "Julia Bingham",
    "Hannah Davison",
    "Jaye Boissiere",
    "Aminata Diallo",
    "Mariah Lee",
    "Adrienne Jordan",
    "Meghan McCool",
    "Jessie Scarpa",
    "Dani Rhodes",
    "Aerial Chavarin",
    "Hailey Harbison",
    "Peyton Perea",
    "Bridgette Andrzejewski",
    "Melissa Lowder",
    "Savanah Uveges",
    "Miranda Nild",
    "Mikaela Howell",
    "Kim Hazlett",
]


def add_missing_players_to_database():
    """Add missing players to the player table"""

    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get existing player names to avoid duplicates
    cursor.execute("SELECT player_name FROM player")
    existing_players = {row[0] for row in cursor.fetchall()}

    # Filter out players that already exist
    players_to_add = [name for name in set(MISSING_PLAYERS) if name not in existing_players]

    logger.info(f"Found {len(players_to_add)} unique players to add to the database")
    logger.info(f"Players already in database: {len(existing_players)}")

    if not players_to_add:
        logger.info("No new players to add - all players already exist in database")
        conn.close()
        return

    # Add each missing player
    added_count = 0
    for player_name in sorted(players_to_add):
        # Generate proper player_id
        player_id = f"p_{uuid.uuid4().hex[:8]}"

        try:
            cursor.execute(
                """
                INSERT INTO player (player_id, player_name)
                VALUES (?, ?)
            """,
                (player_id, player_name),
            )

            added_count += 1
            logger.info(f"Added player: {player_name} (ID: {player_id})")

        except sqlite3.IntegrityError as e:
            logger.warning(f"Could not add {player_name}: {e}")

    # Commit changes
    conn.commit()

    # Verify additions
    cursor.execute("SELECT COUNT(*) FROM player")
    total_players = cursor.fetchone()[0]

    logger.info(f"\n✅ Successfully added {added_count} new players")
    logger.info(f"📊 Total players in database: {total_players}")

    conn.close()

    return added_count


def update_match_player_links():
    """Update match_player table to link newly added players"""

    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Count NULL player_id values before update
    cursor.execute("SELECT COUNT(*) FROM match_player WHERE player_id IS NULL")
    null_before = cursor.fetchone()[0]

    logger.info(f"Found {null_before} match_player records with NULL player_id")

    # Update match_player records to link with player table
    cursor.execute("""
        UPDATE match_player 
        SET player_id = (
            SELECT p.player_id 
            FROM player p 
            WHERE p.player_name = match_player.player_name
        )
        WHERE player_id IS NULL 
        AND player_name IS NOT NULL
    """)

    updated_count = cursor.rowcount
    conn.commit()

    # Count NULL player_id values after update
    cursor.execute("SELECT COUNT(*) FROM match_player WHERE player_id IS NULL")
    null_after = cursor.fetchone()[0]

    logger.info(f"✅ Updated {updated_count} match_player records")
    logger.info(f"📊 NULL player_id records remaining: {null_after}")

    conn.close()

    return updated_count, null_after


def main():
    """Main function to add missing players and update links"""

    logger.info("🚀 Starting process to add missing players and update links")

    # Step 1: Add missing players to player table
    added_count = add_missing_players_to_database()

    # Step 2: Update match_player table to link with newly added players
    updated_count, remaining_nulls = update_match_player_links()

    logger.info("\n🎉 PROCESS COMPLETE!")
    logger.info(f"✅ Added {added_count} new players to player table")
    logger.info(f"✅ Updated {updated_count} match_player record links")
    logger.info(f"📊 Remaining NULL player_id records: {remaining_nulls}")

    if remaining_nulls == 0:
        logger.info("🏆 SUCCESS: All match_player records now have valid player_id links!")
    else:
        logger.info(f"⚠️  Note: {remaining_nulls} records still have NULL player_id (may need manual review)")


if __name__ == "__main__":
    main()
