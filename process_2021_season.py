#!/usr/bin/env python3
"""
Process all 2021 season matches to populate match_player_summary statistics
"""

import subprocess
import sys
import time

# List of all 2021 match_ids
match_ids_2021 = [
    "92ccc792", "bf8c7152", "2e23bf3e", "14b12758", "072e422f", "78660c17", "76f87d7e", "c1330700",
    "b34794ed", "6ba30741", "b23322de", "9733a93c", "a5d7e67a", "28529cef", "be723b18", "2184074e",
    "adffaaa7", "0ec1b5bb", "fb5d227f", "b161f09c", "c19e2edc", "55fd0afd", "5a27b398", "7d903802",
    "8acfa339", "74666aca", "b818bb1d", "1011f557", "5648dd0c", "671d9115", "0f623827", "9f0e821c",
    "5dceef91", "f29a7ef1", "481dcee5", "2cd4ec75", "10f108c3", "40d971e5", "3548ce5a", "db88df87",
    "4ce881eb", "ab0a3b8e", "f8ee8767", "9b3974b4", "fa5f28b6", "08a226bf", "753a54ab", "151a8c6c",
    "138422ad", "986e95dc", "2a4f95fc", "c3692424", "c17893bb", "b2fdb217", "29064988", "9a7e9c60",
    "57127901", "f832fee9", "4d241e61", "80ff5d48", "e45b356c", "5c64fc50", "b4cacaf2", "2b4a0943",
    "a81c893d", "d11a6afc", "fe50bdd7", "800bc864", "2a7aad78", "242d98cb", "4ebf1b38", "b8c01225",
    "8dca4022", "81e7d577", "be46a00f", "714ba654", "aff251d2", "3ea31ead", "16e23f3c", "8d282de4",
    "2f020390", "8eb3b0dd", "0bfe8e38", "7fc53fb8", "fea90630", "d6191f62", "770ec49c", "165448bc",
    "509b89aa", "37d40689", "0bbd1959", "84de42bc", "94bcf409", "331b8375", "b2c4fa2c", "d887c826",
    "343ae440", "a6d4d95a", "b599c7e0", "6b5d492c", "5d320b9e", "74f63303", "e8cf14bf", "3f95b1ba",
    "fc2a3172", "5f985550", "dc4570b2", "b7041740", "2b50579e", "b5eddd77", "9d5e9ab5", "e62b6c8a",
    "c364a795", "1e851026", "500515e8", "14675a38", "94d3e20f", "b9817679", "cc66a2f1", "176d858d",
    "bd3531f7", "725b6b2b", "441fc804", "9211e53e", "d7eebcbd", "051a8bb7", "06d86a41", "24ddd8b3",
    "f0a6559b", "8d61a870", "7a7e6622", "8f77608d", "1d2f9fad", "cd30a848", "7f8b4663", "f6d763ba",
    "b67be4fe", "e97c6f39", "73ff619b", "3f04a185", "57b30c8a", "2a0b287c", "7837616f", "e0ef83c8",
    "888fb121", "119eeed3"
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
    print(f"🚀 Starting 2021 season processing...")
    print(f"   Total matches: {len(match_ids_2021)}")
    
    success_count = 0
    failed_matches = []
    total_records_updated = 0
    
    start_time = time.time()
    
    for i, match_id in enumerate(match_ids_2021):
        print(f"   Processing {i+1:3d}/{len(match_ids_2021)}: {match_id}...", end=" ")
        
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
    
    print(f"\n📊 2021 SEASON PROCESSING COMPLETE:")
    print(f"   ✅ Successfully processed: {success_count}/{len(match_ids_2021)} matches")
    print(f"   📈 Total records updated: {total_records_updated}")
    print(f"   ⏱️  Processing time: {elapsed:.1f} seconds")
    
    if failed_matches:
        print(f"   ❌ Failed matches ({len(failed_matches)}):")
        for match_id in failed_matches:
            print(f"      - {match_id}")
    
    return success_count, failed_matches, total_records_updated

if __name__ == "__main__":
    success_count, failed_matches, total_records = main()
    
    if success_count == len(match_ids_2021):
        print(f"\n🎉 100% SUCCESS! All 2021 matches processed successfully!")
    else:
        print(f"\n⚠️  {len(failed_matches)} matches failed and may need manual review")