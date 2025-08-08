#!/usr/bin/env python3
"""
Analyze HTML file structure to identify player stats table IDs
Following the FBRef scraping methodology from scraping.md
"""

import os

from bs4 import BeautifulSoup


def analyze_html_file(html_file_path):
    """Analyze a single HTML file to find all table IDs, following scraping.md methodology"""

    # Read HTML content
    with open(html_file_path, encoding="utf-8") as f:
        html_content = f.read()

    # Create BeautifulSoup object (following scraping.md methodology)
    soup = BeautifulSoup(html_content, "html.parser")

    # Find all tables in page (following scraping.md methodology)
    tables = soup.find_all("table")

    print(f"Analyzing {os.path.basename(html_file_path)}")
    print(f"Found {len(tables)} tables")
    print("\nTable IDs:")

    # Print all IDs for each table (following scraping.md methodology)
    table_ids = []
    for table in tables:
        table_id = table.get("id")
        if table_id:
            table_ids.append(table_id)
            print(f"  {table_id}")

    print(f"\nTotal tables with IDs: {len(table_ids)}")

    # Look specifically for player stats tables
    player_stats_tables = [
        tid
        for tid in table_ids
        if "stats_" in tid
        and any(keyword in tid for keyword in ["summary", "passing", "defense", "possession", "misc"])
    ]

    if player_stats_tables:
        print("\nPlayer stats tables found:")
        for tid in player_stats_tables:
            print(f"  {tid}")
    else:
        print("\nNo obvious player stats tables found")

    return table_ids, player_stats_tables


def analyze_sample_files(html_dir, num_files=3):
    """Analyze a few sample files to understand the structure"""

    html_files = [f for f in os.listdir(html_dir) if f.endswith(".html") and f.startswith("match_")]

    print(f"Analyzing first {num_files} HTML files from {len(html_files)} total files\n")
    print("=" * 60)

    all_table_ids = set()
    all_player_tables = set()

    for _i, html_file in enumerate(sorted(html_files)[:num_files]):
        html_path = os.path.join(html_dir, html_file)
        table_ids, player_tables = analyze_html_file(html_path)

        all_table_ids.update(table_ids)
        all_player_tables.update(player_tables)

        print("=" * 60)

    print(f"\nSUMMARY ACROSS {num_files} FILES:")
    print(f"Unique table IDs found: {len(all_table_ids)}")
    print(f"Unique player stats tables: {len(all_player_tables)}")

    print("\nAll unique table IDs:")
    for tid in sorted(all_table_ids):
        print(f"  {tid}")

    if all_player_tables:
        print("\nAll unique player stats tables:")
        for tid in sorted(all_player_tables):
            print(f"  {tid}")

    return all_table_ids, all_player_tables


if __name__ == "__main__":
    html_dir = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files"
    analyze_sample_files(html_dir, num_files=3)
