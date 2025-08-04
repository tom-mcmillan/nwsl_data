#!/usr/bin/env python3
"""
Debug the extraction for match 008e301f to see what data is actually available
"""

from bs4 import BeautifulSoup
import pandas as pd
import os

def debug_match_008e301f():
    """Debug what data is actually in the HTML for match 008e301f"""
    
    html_path = "/Users/thomasmcmillan/projects/nwsl_data_backup_data/notebooks/match_html_files/match_008e301f.html"
    
    # Read HTML
    with open(html_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Find all tables
    tables = soup.find_all("table")
    table_ids = [table.get("id") for table in tables if table.get("id")]
    
    print(f"Found {len(tables)} tables with these IDs:")
    for tid in table_ids:
        print(f"  {tid}")
    
    # Look at the summary tables specifically
    summary_tables = [tid for tid in table_ids if 'summary' in tid]
    print(f"\nSummary tables: {summary_tables}")
    
    # Examine the first summary table in detail
    if summary_tables:
        first_summary_id = summary_tables[0]
        print(f"\n🔍 Examining table: {first_summary_id}")
        
        summary_table = soup.find('table', id=first_summary_id)
        if summary_table:
            # Convert to DataFrame to see structure
            try:
                df = pd.read_html(str(summary_table))[0]
                print(f"Table shape: {df.shape}")
                print(f"Columns: {list(df.columns)}")
                print(f"\nFirst few rows:")
                print(df.head(3).to_string())
                
                # Check for goals/stats columns
                goal_cols = [col for col in df.columns if any(stat in str(col).lower() for stat in ['gls', 'goal', 'ast', 'assist', 'xg'])]
                print(f"\nPotential stats columns: {goal_cols}")
                
            except Exception as e:
                print(f"Error parsing table: {e}")
                
                # Try manual inspection
                print("Manual table inspection:")
                rows = summary_table.find_all('tr')[:3]  # First 3 rows
                for i, row in enumerate(rows):
                    cells = row.find_all(['th', 'td'])
                    cell_texts = [cell.get_text().strip() for cell in cells]
                    print(f"Row {i}: {cell_texts}")

if __name__ == "__main__":
    debug_match_008e301f()