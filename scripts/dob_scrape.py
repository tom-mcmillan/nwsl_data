# ══════════════════════════════════════════════════════════════════════════════
#  IMPROVED DOB SCRAPER - Following Best Practices from scraping.md
# ══════════════════════════════════════════════════════════════════════════════

import sqlite3
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
from datetime import datetime
from tqdm import tqdm

def improved_dob_scraper():
    """Improved DOB scraper following scraping.md best practices"""

    # Get players who still need DOBs (those without real dates)
    db_path = "data/processed/nwsldata.db"
    conn = sqlite3.connect(db_path)

    players_needing_dobs = pd.read_sql_query("""
        SELECT player_id, player_name, dob
        FROM Player 
        WHERE dob LIKE '%-01-01'  -- Still have fake January 1st dates
           OR dob IS NULL
        ORDER BY player_name
    """, conn)

    print(f"📋 Found {len(players_needing_dobs)} players still needing real DOBs")

    # Use proper headers as recommended in the guide
    headers = {
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/91.0.4472.124 Safari/537.36'
        )
    }

    results = []

    batch_size = 25  # Smaller batches
    total_to_process = 50  # Limit test batch

    for i, row in tqdm(players_needing_dobs.head(total_to_process).iterrows(), 
                       total=total_to_process, desc="Scraping DOBs"):
        player_id = row['player_id']
        player_name = row['player_name']

        url_name = player_name.replace(' ', '-').replace("'", "")
        url = f"https://fbref.com/en/players/{player_id}/{url_name}"

        try:
            response = requests.get(url, headers=headers, timeout=20)

            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')

                born_text = soup.find(string=re.compile(r'Born:', re.IGNORECASE))
                if born_text:
                    all_text = soup.get_text()
                    date_patterns = re.findall(
                        r'\b((?:January|February|March|April|May|June|July|August|'
                        r'September|October|November|December)\s+\d{1,2},\s+\d{4})\b',
                        all_text
                    )

                    for date_str in date_patterns:
                        try:
                            date_obj = datetime.strptime(date_str, '%B %d, %Y')
                            if 1985 <= date_obj.year <= 2010:
                                results.append({
                                    'player_id': player_id,
                                    'player_name': player_name,
                                    'scraped_dob': date_str,
                                    'status': 'success'
                                })
                                break
                        except:
                            continue
                    else:
                        results.append({
                            'player_id': player_id,
                            'player_name': player_name,
                            'scraped_dob': None,
                            'status': 'no_valid_date'
                        })
                else:
                    results.append({
                        'player_id': player_id,
                        'player_name': player_name,
                        'scraped_dob': None,
                        'status': 'no_born_text'
                    })

            elif response.status_code == 429:
                print(f"⚠️  Rate limited at player {i+1}. Taking longer break...")
                results.append({
                    'player_id': player_id,
                    'player_name': player_name,
                    'scraped_dob': None,
                    'status': 'rate_limited'
                })
                time.sleep(30)  # Backoff

            else:
                results.append({
                    'player_id': player_id,
                    'player_name': player_name,
                    'scraped_dob': None,
                    'status': f'HTTP_{response.status_code}'
                })

        except Exception as e:
            results.append({
                'player_id': player_id,
                'player_name': player_name,
                'scraped_dob': None,
                'status': f'error_{str(e)[:20]}'
            })

        # Use 6 second delay between requests
        time.sleep(6)

        # Take longer break every 25 players
        if (i + 1) % batch_size == 0:
            print(f"📊 Processed {i+1} players. Taking 2 minute break...")
            time.sleep(120)

    # Save results
    results_df = pd.DataFrame(results)
    results_df.to_csv('dob_scraping_improved.csv', index=False)

    # Analysis
    successful = len(results_df[results_df['status'] == 'success'])
    print(f"\n📊 Results: {successful}/{len(results_df)} successful "
          f"({successful/len(results_df)*100:.1f}%)")

    return results_df

# Run improved scraper with first 50 players as test
print("🚀 Testing improved scraper with 6-second delays and batch breaks...")
results = improved_dob_scraper()
