#!/usr/bin/env python3
"""
Process all 2014 season matches to populate match_player_summary statistics
Uses 2018 script since 2014 has identical 24-field format as 2015-2018
"""

import subprocess
import sys
import time

# List of all 2014 match_ids
match_ids_2014 = [
    "7239a666", "7ba0c3c1", "008e301f", "81b4c9d5", "d8b5b0d1", "9e8a54ad", "b5b00e0d", "95e1c7f8",
    "4b3ba6eb", "f926b2f9", "b9c7a9ae", "9fdfbc92", "7e1b42e4", "f1f8eb96", "ac4f0879", "c58e1bb2",
    "d93b2f45", "bd4dc2e9", "84e0b5c6", "d4e99bb3", "7e6de6ae", "7ab7b9be", "ce3b4b4f", "4ff45b34",
    "9823e37a", "a5b2fdaa", "87ab7a76", "1bd4c4d7", "e0327f5c", "67bb9c30", "a2b9d9b2", "e73cbe1b",
    "c7ad84b1", "c1d00c4f", "16ad16d3", "e5593c9b", "50ea6f32", "c2c37f2b", "0b4c0c04", "48b8a7f8",
    "9e9e47ba", "3d0b6cd7", "e6b6ab7c", "e8b0c877", "e2f5ec2e", "de7b8cf6", "4cbf5bf7", "e7d1a8b9",
    "23f26c6e", "9a27b2b4", "3b7cb6eb", "e9a73f4b", "e4e1b4c6", "5bd19c1f", "a3d9c08c", "cfc50daa",
    "7cd4eaec", "7e48d9ab", "bf98b6c6", "a8b91ae2", "4bce7334", "89f55cf1", "b6ed7e71", "e1f57c7b",
    "b1b6d967", "50c8a5f5", "5b4b7fa1", "e8e07b37", "e3f02ca4", "e3aa9b85", "3f8d3df5", "bb7b2975",
    "7ad3c0e9", "abe07b41", "40b8bd1f", "a7b7a1c3", "8f1d6eb0", "f037cc13", "07dedb30", "8e3b5a30",
    "b3de4b2e", "8b8b4a78", "43b9a33e", "7e1bc7a7", "36b5b6d5", "9c0b3c8f", "bf7cdc2c", "d88b2dec",
    "26ade36d", "8e7bb7c3", "c1c7a2ea", "4b7b97ce", "1e2dd36a", "a3c0c5a0", "4eb59bd1", "83c7dac3",
    "d5b2ab6b", "e07d6d5f", "bd9d8ba2", "3b7bb6be", "8b0aa9d3", "6bd7a4da", "03f1b6b1", "d1abb6f4",
    "ddc14bf3", "7ad7b2b7", "b3aa4b9e", "0ab6d9b7"
]

def process_match(match_id):
    """Process a single match using the 2018-compatible populate script."""
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
    print(f"🚀 Starting 2014 season processing...")
    print(f"   Total matches: {len(match_ids_2014)}")
    print(f"   Expected records: 2,700")
    print(f"   📊 Using 2018-compatible script (identical 24-field format)")
    print(f"   🎯 Aiming to extend perfect streak to 12 seasons!")
    
    success_count = 0
    failed_matches = []
    total_records_updated = 0
    
    start_time = time.time()
    
    for i, match_id in enumerate(match_ids_2014):
        print(f"   Processing {i+1:3d}/{len(match_ids_2014)}: {match_id}...", end=" ")
        
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
    
    print(f"\n📊 2014 SEASON PROCESSING COMPLETE:")
    print(f"   ✅ Successfully processed: {success_count}/{len(match_ids_2014)} matches")
    print(f"   📈 Total records updated: {total_records_updated}")
    print(f"   ⏱️  Processing time: {elapsed:.1f} seconds")
    print(f"   🚀 Average speed: {total_records_updated/elapsed:.1f} records/second")
    print(f"   🔄 Format compatibility: 2014 = 2015 = 2016 = 2017 = 2018 (24 fields)")
    
    if failed_matches:
        print(f"   ❌ Failed matches ({len(failed_matches)}):")
        for match_id in failed_matches[:10]:  # Show first 10
            print(f"      - {match_id}")
        if len(failed_matches) > 10:
            print(f"      ... and {len(failed_matches)-10} more")
    
    return success_count, failed_matches, total_records_updated

if __name__ == "__main__":
    success_count, failed_matches, total_records = main()
    
    if success_count == len(match_ids_2014):
        print(f"\n🎉 100% SUCCESS! All 2014 matches processed successfully!")
        print(f"🏆 HISTORIC MILESTONE: 12 CONSECUTIVE SEASONS!")
        print(f"🔧 Successfully leveraged 2018 script compatibility!")
        print(f"📈 Extending legacy format compatibility to 5 seasons (2014-2018)!")
    else:
        print(f"\n⚠️  {len(failed_matches)} matches failed and may need manual review")