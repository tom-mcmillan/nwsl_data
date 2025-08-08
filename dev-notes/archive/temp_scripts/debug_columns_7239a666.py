#!/usr/bin/env python3
"""
Debug column structure for match 7239a666 to fix shirt_number and minutes_played extraction
"""

import pandas as pd
from bs4 import BeautifulSoup


def debug_columns_7239a666():
    """Debug the exact column structure to fix field extraction"""

    html_path = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files/match_7239a666.html"

    # Read HTML
    with open(html_path, encoding="utf-8") as f:
        html_content = f.read()

    soup = BeautifulSoup(html_content, "html.parser")

    # Get first summary table
    summary_table = soup.find("table", id="stats_6f666306_summary")

    if summary_table:
        print("🔍 DEBUGGING COLUMN STRUCTURE FOR MATCH 7239a666")

        # Convert to DataFrame to see exact column names
        df = pd.read_html(str(summary_table))[0]

        print(f"\nOriginal columns (MultiIndex): {isinstance(df.columns, pd.MultiIndex)}")
        print(f"Column shape: {df.columns.shape}")

        if isinstance(df.columns, pd.MultiIndex):
            print("\nMultiIndex levels:")
            for i, level in enumerate(df.columns.levels):
                print(f"  Level {i}: {list(level)}")

            print("\nAll column tuples:")
            for i, col in enumerate(df.columns):
                print(f"  {i:2d}: {col}")
        else:
            print(f"\nColumns: {list(df.columns)}")

        # Flatten columns to see final names
        if isinstance(df.columns, pd.MultiIndex):
            flattened = ["_".join(col).strip() if col[1] else col[0] for col in df.columns.values]
            print("\nFlattened column names:")
            for i, col in enumerate(flattened):
                print(f"  {i:2d}: '{col}'")

        # Look at first few rows to understand data
        print("\nFirst 3 rows of data:")
        print(df.head(3).to_string())

        # Try to identify shirt number and minutes columns
        potential_shirt_cols = []
        potential_minutes_cols = []

        columns_to_check = flattened if isinstance(df.columns, pd.MultiIndex) else df.columns

        for i, col in enumerate(columns_to_check):
            col_lower = str(col).lower()
            if any(term in col_lower for term in ["#", "number", "shirt", "jersey"]):
                potential_shirt_cols.append((i, col))
            if any(term in col_lower for term in ["min", "minute", "time"]):
                potential_minutes_cols.append((i, col))

        print(f"\nPotential shirt number columns: {potential_shirt_cols}")
        print(f"Potential minutes columns: {potential_minutes_cols}")

        # Show actual data from those columns
        if potential_shirt_cols:
            for idx, col_name in potential_shirt_cols:
                print(f"\nData from column '{col_name}' (index {idx}):")
                print(df.iloc[:5, idx].tolist())

        if potential_minutes_cols:
            for idx, col_name in potential_minutes_cols:
                print(f"\nData from column '{col_name}' (index {idx}):")
                print(df.iloc[:5, idx].tolist())


if __name__ == "__main__":
    debug_columns_7239a666()
