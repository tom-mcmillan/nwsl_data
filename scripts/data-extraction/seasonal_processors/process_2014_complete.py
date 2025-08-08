#!/usr/bin/env python3
"""
Process all 2014 season matches with correct match IDs to populate match_player_summary statistics
Uses 2018 script since 2014 has identical 24-field format as 2015-2018
"""

import subprocess
import time

# Complete list of 2014 match_ids (108 matches)
match_ids_2014 = [
    "04d023e7",
    "d6aec4b1",
    "2fd9c845",
    "99453465",
    "f740ebaf",
    "6ed85fb8",
    "be1600aa",
    "e2a1b418",
    "66645e5e",
    "b3aab324",
    "9e8abda1",
    "db3aa3b0",
    "e3a360d3",
    "188f52c0",
    "35003fd2",
    "74564b15",
    "807c06e4",
    "1186ea77",
    "f6ec53f0",
    "2d5af9d6",
    "edfa82d4",
    "2ac75a24",
    "7f0a506f",
    "9a0ab07f",
    "f0b59875",
    "70f1e21b",
    "62cdbc09",
    "f2a8f16b",
    "63378f00",
    "38402333",
    "b118c20b",
    "eed6455b",
    "83cdf3e1",
    "ac3f3db5",
    "17061441",
    "c6df790b",
    "25049986",
    "19c171d0",
    "9f613a73",
    "a68c37d0",
    "a23cf271",
    "f31a9d7d",
    "fbe77a1f",
    "22486374",
    "43103b88",
    "7014113e",
    "890a4ab4",
    "dbca8ac5",
    "5ba2eba7",
    "b9ab40c2",
    "059fa801",
    "0bfa24ae",
    "c0417d1e",
    "2a83a3f6",
    "3ac4acb5",
    "bce76cd8",
    "66ca439b",
    "48d47dc9",
    "fa063275",
    "9feb3b94",
    "a45da2df",
    "6195a47a",
    "b6b656ae",
    "19e8a4b9",
    "77e8defe",
    "581cef24",
    "79fde10f",
    "a41a3eeb",
    "1a248e02",
    "69b3b00d",
    "47d5955b",
    "e03ef57f",
    "14529941",
    "8e775a64",
    "04480428",
    "4d031196",
    "dae49ac4",
    "6863d0dc",
    "30540f3f",
    "c00ea6a4",
    "a459e5c0",
    "c9c09e02",
    "56a01c6b",
    "5dd078d5",
    "5dd549e4",
    "70461c41",
    "f6a0f221",
    "0a10a72f",
    "e9e2fcf9",
    "92a0b833",
    "3fded0a5",
    "af4d51ac",
    "dcd5f391",
    "9ce230c1",
    "b0355b88",
    "e8af8ca3",
    "9830bc25",
    "812788e2",
    "21d682a9",
    "a835491d",
    "43d092f2",
    "f59d77ca",
    "dd2c669d",
    "e0dc82d9",
    "8b97808b",
    "0c35f2f9",
    "c8ac28b2",
    "ee9cc30e",
    "217a40ab",
    "f881e2c1",
    "60f738e9",
]


def process_match(match_id):
    """Process a single match using the 2018-compatible populate script."""
    try:
        result = subprocess.run(
            ["python", "scripts/populate_match_player_summary_2018.py", match_id],
            capture_output=True,
            text=True,
            timeout=60,
        )
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Timeout after 60 seconds"
    except Exception as e:
        return False, "", str(e)


def main():
    print("🚀 Starting 2014 season processing with CORRECT match IDs...")
    print(f"   Total matches: {len(match_ids_2014)}")
    print("   📊 Using 2018-compatible script (identical 24-field format)")
    print("   🎯 Processing complete 2014 statistical data!")

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
                    line = [l for l in stdout.split("\n") if "Records updated:" in l][0]
                    records_updated = int(line.split("Records updated:")[1].strip())
                except:
                    records_updated = 0

            total_records_updated += records_updated
            success_count += 1
            print(f"✅ ({records_updated} records)")
        else:
            failed_matches.append(match_id)
            print("❌ FAILED")
            if stderr:
                print(f"      Error: {stderr[:100]}")

    elapsed = time.time() - start_time

    print("\n📊 2014 SEASON PROCESSING COMPLETE:")
    print(f"   ✅ Successfully processed: {success_count}/{len(match_ids_2014)} matches")
    print(f"   📈 Total records updated: {total_records_updated}")
    print(f"   ⏱️  Processing time: {elapsed:.1f} seconds")
    print(f"   🚀 Average speed: {total_records_updated/elapsed:.1f} records/second")
    print("   🔄 Format compatibility: 2014 = 2015 = 2016 = 2017 = 2018 (24 fields)")

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
        print("\n🎉 100% SUCCESS! All 2014 matches processed successfully!")
        print("🏆 COMPLETE 2014 STATISTICAL DATABASE!")
        print("🔧 Successfully leveraged 2018 script compatibility!")
    else:
        print(f"\n⚠️  {len(failed_matches)} matches failed and may need manual review")
