#!/usr/bin/env python3
"""
Process all 2019 season matches to populate match_player_summary statistics
"""

import subprocess
import sys
import time

# List of all 2019 match_ids
match_ids_2019 = [
    "afcd583a", "67a57f59", "c9ec6863", "25004f7e", "9bf95ec5", "70b9c1b6", "87083ab3", "b66598e4",
    "ff7f188e", "8780d6a5", "b87d86b7", "ae49ad75", "85ba8579", "af3f157d", "5394bc1b", "efddeda8",
    "36c46e0d", "d53451cf", "86b6cc0a", "9a2b26a1", "23114faa", "37be7787", "d26ed7f0", "7f398f88",
    "cc8ebaaa", "0f1cb3d1", "4120af97", "c79ee9c4", "622f898e", "f8c8aea4", "6420bec8", "610a4c17",
    "1cef5979", "888d23a0", "bde3da3d", "f7b69a29", "6f3dc675", "1e61252e", "eb81709b", "ec9ceb9f",
    "7ee309e3", "05482155", "b30b11e9", "745bedf3", "49094e3a", "1520b6f5", "0262bb35", "b2b9405a",
    "51020dea", "8f3fbf96", "78fac894", "efcbf7b7", "2b75137d", "dbbdb47c", "4f5b874c", "e133d584",
    "6c56c1c8", "38048580", "125df7bb", "01cdf2c9", "f7ea6cf4", "3e2273da", "a0c570ff", "3903be81",
    "453b20ed", "3690a734", "073975b1", "6d4a68e6", "e4dad184", "202faad2", "67076783", "4aa7a9c5",
    "6536d5aa", "a6063bfa", "976b8d77", "e554a812", "a0d14941", "ef3f22f7", "3b62060e", "6f44cb0a",
    "2f8d4701", "3e58ee5e", "dd37453e", "6e16e67b", "b31185f6", "aa5085c0", "4d435ae3", "fdd56674",
    "8b7900bd", "508e1cf0", "1173feeb", "d6124086", "03f02a2d", "fba4e358", "1b8fd283", "2c25dcc1",
    "96746e28", "3653dfaf", "f33364ee", "4a9bc623", "d7245076", "731bfd8e", "f2a8492d", "a6299d40",
    "b3dca21a", "840ceaac", "3ccbf5a1", "9e58d38e", "6b7e06cd", "51f18293", "9f7344bb"
]

def process_match(match_id):
    """Process a single match using the populate script."""
    try:
        result = subprocess.run(
            ['python', 'scripts/populate_match_player_summary.py', match_id],
            capture_output=True, text=True, timeout=60
        )
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Timeout after 60 seconds"
    except Exception as e:
        return False, "", str(e)

def main():
    print(f"🚀 Starting 2019 season processing...")
    print(f"   Total matches: {len(match_ids_2019)}")
    print(f"   Expected records: 3,046")
    
    success_count = 0
    failed_matches = []
    total_records_updated = 0
    
    start_time = time.time()
    
    for i, match_id in enumerate(match_ids_2019):
        print(f"   Processing {i+1:3d}/{len(match_ids_2019)}: {match_id}...", end=" ")
        
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
    
    print(f"\n📊 2019 SEASON PROCESSING COMPLETE:")
    print(f"   ✅ Successfully processed: {success_count}/{len(match_ids_2019)} matches")
    print(f"   📈 Total records updated: {total_records_updated}")
    print(f"   ⏱️  Processing time: {elapsed:.1f} seconds")
    print(f"   🚀 Average speed: {total_records_updated/elapsed:.1f} records/second")
    
    if failed_matches:
        print(f"   ❌ Failed matches ({len(failed_matches)}):")
        for match_id in failed_matches[:10]:  # Show first 10
            print(f"      - {match_id}")
        if len(failed_matches) > 10:
            print(f"      ... and {len(failed_matches)-10} more")
    
    return success_count, failed_matches, total_records_updated

if __name__ == "__main__":
    success_count, failed_matches, total_records = main()
    
    if success_count == len(match_ids_2019):
        print(f"\n🎉 100% SUCCESS! All 2019 matches processed successfully!")
    else:
        print(f"\n⚠️  {len(failed_matches)} matches failed and may need manual review")