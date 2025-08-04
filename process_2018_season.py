#!/usr/bin/env python3
"""
Process all 2018 season matches to populate match_player_summary statistics
Uses adapted script for 2018's reduced field set (24 fields vs 37 in modern seasons)
"""

import subprocess
import sys
import time

# List of all 2018 match_ids
match_ids_2018 = [
    "aeb17e74", "3a815c4f", "73b7a256", "254105a8", "82737f56", "a17c0ef0", "9951420b", "9d713186",
    "66c8a43d", "b73fabbe", "0d694f3a", "e7c7c23c", "2906b503", "52649c9c", "387eff31", "2a696e31",
    "57fa7105", "98d7db91", "76114fb2", "8928d94b", "ea9f09e6", "3e51f799", "9d83af85", "6bc282b7",
    "fede5444", "ba422734", "a946c0e2", "36e97512", "79f8e9a0", "4d850dbb", "7b04d31a", "1f61e83b",
    "8a883753", "a1c035b7", "e6c9e5d4", "359bf5ad", "d231b19e", "002d9eb4", "f6ec8a8b", "8a947bd0",
    "6f73f9a8", "38851d0d", "c605cbfa", "57df675c", "ed0821bd", "9c06de8d", "bf237d24", "f1ce78a1",
    "8f38f319", "2d29c0fd", "2fd28c8a", "5360f412", "51a251e2", "4f767234", "1ff11e78", "9d12b00f",
    "10edb34e", "f27037ec", "0790a1bc", "3adf7469", "1e7cfb64", "bc4c3562", "94508a03", "31b27eae",
    "d2459fcd", "66ccdb0a", "cd4fe8d3", "e87befd2", "4aa78b99", "a61b70d1", "cd5433b9", "b5da8f35",
    "8fcf96e7", "74151c11", "874207c8", "53526198", "eddbe752", "84d6dca9", "f36366a9", "f25117a0",
    "f542d0eb", "21526888", "55471e47", "f7ee4334", "09657304", "5f19ba81", "465459ea", "e1e516b9",
    "6e0ca93c", "50f8bec2", "58d26f0b", "adcabd51", "30406f36", "c0b6a639", "ec411397", "d33ff0d8",
    "605fa6ac", "15f03025", "5238c761", "39dbe62f", "f484f6f6", "5a7b7125", "db9ea114", "092fe7ea",
    "e6091451", "e6d49a20", "783a65d6", "16744a84", "e17daaeb", "2c32f3d3", "22f04f44"
]

def process_match(match_id):
    """Process a single match using the 2018-specific populate script."""
    try:
        result = subprocess.run(
            ['python', 'scripts/populate_match_player_summary_2018.py', match_id],
            capture_output=True, text=True, timeout=60
        )
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Timeout after 60 seconds"
    except Exception as e:
        return False, "", str(e)

def main():
    print(f"🚀 Starting 2018 season processing...")
    print(f"   Total matches: {len(match_ids_2018)}")
    print(f"   Expected records: 2,992")
    print(f"   📊 Using 2018-specific script (24 statistical fields)")
    
    success_count = 0
    failed_matches = []
    total_records_updated = 0
    
    start_time = time.time()
    
    for i, match_id in enumerate(match_ids_2018):
        print(f"   Processing {i+1:3d}/{len(match_ids_2018)}: {match_id}...", end=" ")
        
        success, stdout, stderr = process_match(match_id)
        
        if success:
            # Extract records updated count from output
            records_updated = 0
            if "Records updated:" in stdout:
                try:
                    line = [l for l in stdout.split('\n') if 'Records updated:' in l][0]
                    records_updated = int(line.split('Records updated:')[1].strip())
                except:
                    records_updated = 0
            
            total_records_updated += records_updated
            success_count += 1
            print(f"✅ ({records_updated} records)")
        else:
            failed_matches.append(match_id)
            print(f"❌ FAILED")
            if stderr:
                print(f"      Error: {stderr[:100]}")
    
    elapsed = time.time() - start_time
    
    print(f"\n📊 2018 SEASON PROCESSING COMPLETE:")
    print(f"   ✅ Successfully processed: {success_count}/{len(match_ids_2018)} matches")
    print(f"   📈 Total records updated: {total_records_updated}")
    print(f"   ⏱️  Processing time: {elapsed:.1f} seconds")
    print(f"   🚀 Average speed: {total_records_updated/elapsed:.1f} records/second")
    print(f"   🆕 Data structure: 2018 format (24 fields, advanced stats as NULL)")
    
    if failed_matches:
        print(f"   ❌ Failed matches ({len(failed_matches)}):")
        for match_id in failed_matches[:10]:  # Show first 10
            print(f"      - {match_id}")
        if len(failed_matches) > 10:
            print(f"      ... and {len(failed_matches)-10} more")
    
    return success_count, failed_matches, total_records_updated

if __name__ == "__main__":
    success_count, failed_matches, total_records = main()
    
    if success_count == len(match_ids_2018):
        print(f"\n🎉 100% SUCCESS! All 2018 matches processed successfully!")
        print(f"🔧 Successfully adapted to 2018's reduced field set!")
    else:
        print(f"\n⚠️  {len(failed_matches)} matches failed and may need manual review")