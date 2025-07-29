#!/usr/bin/env python3
"""
Import city data into nwsldata.db
"""

import sqlite3
import pandas as pd
import os
import glob
import hashlib
import re

def create_city_table(conn):
    """Create the city table with all necessary columns"""
    cursor = conn.cursor()
    
    # Drop existing table if it exists
    cursor.execute("DROP TABLE IF EXISTS city")
    
    create_table_sql = """
    CREATE TABLE city (
        city_id TEXT PRIMARY KEY,
        "City" TEXT NOT NULL,
        "State" TEXT NOT NULL,
        "Data_Status" TEXT,
        
        -- Original fields preserved
        "City_Population" TEXT,
        "Metro_Population" TEXT,
        "Urban_Population" TEXT,
        "Demo_race" TEXT,
        "Demo_Hispanic origin" TEXT,
        "Demo_US citizens" TEXT,
        "College_Degree_Plus" TEXT,
        "High_School_Diploma" TEXT,
        "Median_Household_Income" TEXT,
        "Per_Capita_Income" TEXT,
        "Poverty_Rate" TEXT,
        "Median_Home_Price" TEXT,
        "Unemployment_Rate" TEXT,
        "Sales_Tax_Rate" TEXT,
        "Area" TEXT,
        "Elevation" TEXT,
        "Population_Density" TEXT,
        "Nearby_Cities_Count" INTEGER,
        
        -- Parsed numeric fields
        city_pop_numeric INTEGER,
        city_pop_rank INTEGER,
        metro_pop_numeric INTEGER,
        metro_pop_rank INTEGER,
        urban_pop_numeric INTEGER,
        urban_pop_rank INTEGER,
        hispanic_population_pct REAL,
        us_citizens_pct REAL,
        college_degree_pct REAL,
        high_school_diploma_pct REAL,
        household_income_numeric INTEGER,
        per_capita_income_numeric INTEGER,
        poverty_rate_pct REAL,
        home_price_numeric INTEGER,
        unemployment_rate_pct REAL,
        sales_tax_rate_pct REAL,
        area_sq_miles REAL,
        elevation_ft INTEGER,
        pop_density_per_sq_mile INTEGER,
        nearby_cities_numeric INTEGER,
        
        UNIQUE("City", "State")
    );
    """
    
    cursor.execute(create_table_sql)
    conn.commit()
    print("City table created successfully")

def import_city_csv_files(conn, data_dir):
    """Import all city CSV files into the database"""
    
    # Find all city summary CSV files
    csv_pattern = os.path.join(data_dir, "city_data_summary_*.csv")
    csv_files = glob.glob(csv_pattern)
    
    print(f"Found {len(csv_files)} CSV files to import")
    
    total_imported = 0
    
    for csv_file in csv_files:
        print(f"Processing: {os.path.basename(csv_file)}")
        
        try:
            # Read the CSV file
            df = pd.read_csv(csv_file)
            
            # Generate city_id values using hex hash of city name and state
            def generate_city_id(row):
                city_state = f"{row['City']}{row['State']}"
                # Create MD5 hash and take first 8 characters
                hash_obj = hashlib.md5(city_state.encode())
                hex_hash = hash_obj.hexdigest()[:8]
                return f"cty_{hex_hash}"
            
            df['city_id'] = df.apply(generate_city_id, axis=1)
            
            # Comprehensive parsing functions
            def parse_population_field(pop_str):
                """Parse population fields with millions/thousands and ranks"""
                if pd.isna(pop_str) or not pop_str:
                    return None, None
                
                pop_numeric = None
                rank_numeric = None
                
                # Handle millions like "1.387 million people"
                million_match = re.search(r'(\d+\.?\d*)\s*million\s+people', str(pop_str))
                if million_match:
                    pop_numeric = int(float(million_match.group(1)) * 1000000)
                else:
                    # Handle regular numbers like "17027 people"
                    number_match = re.search(r'(\d+)\s+people', str(pop_str))
                    if number_match:
                        pop_numeric = int(number_match.group(1))
                
                # Extract country rank
                rank_match = re.search(r'country rank:\s*[≈~]?(\d+)', str(pop_str))
                if rank_match:
                    rank_numeric = int(rank_match.group(1))
                
                return pop_numeric, rank_numeric
            
            def parse_percentage(pct_str):
                """Parse percentage fields like '6.7%' or '70.3% (1.73 × national average)'"""
                if pd.isna(pct_str) or not pct_str:
                    return None
                
                pct_match = re.search(r'(\d+\.?\d*)%', str(pct_str))
                if pct_match:
                    return float(pct_match.group(1))
                return None
            
            def parse_currency(curr_str):
                """Parse currency fields like '$92263.00 per year'"""
                if pd.isna(curr_str) or not curr_str:
                    return None
                
                # Remove commas and extract dollar amount
                curr_match = re.search(r'\$(\d+(?:,\d+)*(?:\.\d+)?)', str(curr_str))
                if curr_match:
                    return int(float(curr_match.group(1).replace(',', '')))
                return None
            
            def parse_area(area_str):
                """Parse area like '83.943 mi^2'"""
                if pd.isna(area_str) or not area_str:
                    return None
                
                area_match = re.search(r'(\d+\.?\d*)\s*mi\^?2', str(area_str))
                if area_match:
                    return float(area_match.group(1))
                return None
            
            def parse_elevation(elev_str):
                """Parse elevation like '177 ft'"""
                if pd.isna(elev_str) or not elev_str:
                    return None
                
                elev_match = re.search(r'(\d+)\s*ft', str(elev_str))
                if elev_match:
                    return int(elev_match.group(1))
                return None
            
            def parse_density(dens_str):
                """Parse density like '8780 people per square mile'"""
                if pd.isna(dens_str) or not dens_str:
                    return None
                
                dens_match = re.search(r'(\d+)\s*people per square mile', str(dens_str))
                if dens_match:
                    return int(dens_match.group(1))
                return None
            
            # Apply all parsing functions
            df[['city_pop_numeric', 'city_pop_rank']] = df['City_Population'].apply(
                lambda x: pd.Series(parse_population_field(x))
            )
            
            df[['metro_pop_numeric', 'metro_pop_rank']] = df['Metro_Population'].apply(
                lambda x: pd.Series(parse_population_field(x))
            )
            
            df[['urban_pop_numeric', 'urban_pop_rank']] = df['Urban_Population'].apply(
                lambda x: pd.Series(parse_population_field(x))
            )
            
            # Parse percentage fields
            df['hispanic_population_pct'] = df['Demo_Hispanic origin'].apply(parse_percentage)
            df['us_citizens_pct'] = df['Demo_US citizens'].apply(parse_percentage)
            df['college_degree_pct'] = df['College_Degree_Plus'].apply(parse_percentage)
            df['high_school_diploma_pct'] = df['High_School_Diploma'].apply(parse_percentage)
            df['poverty_rate_pct'] = df['Poverty_Rate'].apply(parse_percentage)
            df['unemployment_rate_pct'] = df['Unemployment_Rate'].apply(parse_percentage)
            df['sales_tax_rate_pct'] = df['Sales_Tax_Rate'].apply(parse_percentage)
            
            # Parse currency fields
            df['household_income_numeric'] = df['Median_Household_Income'].apply(parse_currency)
            df['per_capita_income_numeric'] = df['Per_Capita_Income'].apply(parse_currency)
            df['home_price_numeric'] = df['Median_Home_Price'].apply(parse_currency)
            
            # Parse geographic fields
            df['area_sq_miles'] = df['Area'].apply(parse_area)
            df['elevation_ft'] = df['Elevation'].apply(parse_elevation)
            df['pop_density_per_sq_mile'] = df['Population_Density'].apply(parse_density)
            df['nearby_cities_numeric'] = df['Nearby_Cities_Count']
            
            # Import into database
            df.to_sql('city', conn, if_exists='append', index=False)
            
            rows_imported = len(df)
            total_imported += rows_imported
            print(f"  Imported {rows_imported} rows")
            
        except Exception as e:
            print(f"  Error processing {csv_file}: {e}")
    
    print(f"Total rows imported: {total_imported}")

def main():
    # Database and data paths
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    data_dir = "/Users/thomasmcmillan/projects/nwsl_data/notebooks/city_data"
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    
    try:
        # Create city table
        create_city_table(conn)
        
        # Import CSV data
        import_city_csv_files(conn, data_dir)
        
        # Verify the import
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM city")
        count = cursor.fetchone()[0]
        print(f"Total cities in database: {count}")
        
        # Show sample data
        cursor.execute('SELECT city_id, "City", "State", city_pop_numeric, household_income_numeric, unemployment_rate_pct FROM city LIMIT 5')
        sample_data = cursor.fetchall()
        print("\nSample data:")
        for row in sample_data:
            pop = f"{row[3]:,}" if row[3] else "N/A"
            income = f"${row[4]:,}" if row[4] else "N/A"
            unemp = f"{row[5]}%" if row[5] else "N/A"
            print(f"  {row[0]}: {row[1]}, {row[2]} - Pop: {pop}, Income: {income}, Unemployment: {unemp}")
            
    finally:
        conn.close()

if __name__ == "__main__":
    main()