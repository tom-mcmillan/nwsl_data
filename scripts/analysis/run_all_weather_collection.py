#!/usr/bin/env python3
"""
Run weather data collection for all NWSL matches in batches.
"""

import os
import subprocess
import sys
import time


def run_weather_batch(start_offset):
    """Run a single batch of weather data collection."""
    env = os.environ.copy()
    env["WOLFRAM_APPID"] = "6XGGHQ7K94"

    cmd = [sys.executable, "scripts/get_match_weather_data.py", str(start_offset), "100"]

    try:
        result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=1200)
        if result.returncode == 0:
            # Parse output for completion status
            if "ALL MATCHES PROCESSED!" in result.stdout:
                return True, result.stdout
            elif "Next batch:" in result.stdout:
                return False, result.stdout
            else:
                return False, f"Unexpected output: {result.stdout}"
        else:
            return False, f"Error: {result.stderr}"
    except subprocess.TimeoutExpired:
        return False, "Batch timed out after 10 minutes"
    except Exception as e:
        return False, f"Exception: {e}"


def main():
    """Run all weather data collection batches."""
    start_offset = 100  # We've already done 0-99
    batch_size = 100  # Larger batches to go faster
    total_processed = 100  # Already completed

    print("🌤️ Starting automated weather data collection for all NWSL matches")
    print(f"Already completed: {total_processed} matches")
    print(f"Starting from batch: {start_offset}")
    print("-" * 60)

    batch_count = 0
    while True:
        batch_count += 1
        print(f"\n📊 Running batch #{batch_count} (starting at match {start_offset + 1})")

        # Run the batch
        completed, output = run_weather_batch(start_offset)

        # Parse results from output
        lines = output.split("\n")
        for line in lines:
            if "Batch matches processed:" in line:
                processed = int(line.split(":")[1].strip())
                total_processed += processed
            elif "Successful weather lookups:" in line:
                successful = int(line.split(":")[1].strip())
            elif "Database records updated:" in line:
                int(line.split(":")[1].strip())
            elif "Batch success rate:" in line:
                success_rate = line.split(":")[1].strip()

        print(f"✓ Batch completed: {processed} processed, {successful} successful, {success_rate} success rate")
        print(f"📈 Total progress: {total_processed} matches completed")

        if completed:
            print("\n🎉 ALL MATCHES PROCESSED SUCCESSFULLY!")
            print(f"Final total: {total_processed} matches with weather data")
            break

        # Update offset for next batch
        start_offset += batch_size

        # Brief pause between batches
        print("⏳ Waiting 5 seconds before next batch...")
        time.sleep(5)


if __name__ == "__main__":
    main()
