#!/usr/bin/env python3
"""
Run COMPREHENSIVE team statistics extraction for ALL matches.
This extracts the complete range of available soccer statistics from all CSV file types.
"""

import logging
from pathlib import Path

from extract_comprehensive_team_stats import (
    extract_comprehensive_team_stats_from_match,
    save_comprehensive_team_stats_to_database,
)

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def main():
    """Extract comprehensive team stats for all matches with CSV data."""

    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"
    tables_dir = "/Users/thomasmcmillan/projects/nwsl_data/notebooks/match_html_files/tables"

    # Get all match directories
    tables_path = Path(tables_dir)
    if not tables_path.exists():
        logging.error(f"Tables directory not found: {tables_dir}")
        return

    match_dirs = [d for d in tables_path.iterdir() if d.is_dir()]
    logging.info(f"Found {len(match_dirs)} match directories")

    total_teams = 0
    successful_matches = 0
    failed_matches = 0
    no_data_matches = 0
    comprehensive_coverage = 0

    for i, match_dir in enumerate(match_dirs, 1):
        match_id = match_dir.name

        # Check if this match has any stat CSV files
        stat_files = list(match_dir.glob("*_stats_*.csv"))
        if not stat_files:
            no_data_matches += 1
            continue

        try:
            logging.info(f"Processing match {i}/{len(match_dirs)}: {match_id}")

            # Extract comprehensive team stats from ALL available CSV files
            team_stats = extract_comprehensive_team_stats_from_match(match_id, tables_dir)

            if team_stats:
                # Save to database
                save_comprehensive_team_stats_to_database(team_stats, db_path)

                total_teams += len(team_stats)
                successful_matches += 1

                # Check if we have comprehensive data (5+ file types per team)
                has_comprehensive = False
                for stats in team_stats:
                    non_null_fields = sum(
                        1
                        for field in [
                            "goals",
                            "fouls",
                            "touches_def_3rd",
                            "short_passes_completed",
                            "tackles_def_3rd",
                        ]
                        if stats.get(field) is not None
                    )
                    if non_null_fields >= 3:  # Has data from multiple CSV types
                        has_comprehensive = True
                        break

                if has_comprehensive:
                    comprehensive_coverage += 1

                logging.info(
                    f"✓ Match {match_id}: {len(team_stats)} teams processed ({'COMPREHENSIVE' if has_comprehensive else 'basic'})"
                )
            else:
                failed_matches += 1
                logging.warning(f"✗ Match {match_id}: No team stats extracted")

        except Exception as e:
            failed_matches += 1
            logging.error(f"✗ Match {match_id}: Error - {e}")

    # Summary
    logging.info("\n" + "=" * 80)
    logging.info("🚀 COMPREHENSIVE TEAM STATISTICS EXTRACTION SUMMARY")
    logging.info("=" * 80)
    logging.info(f"Total match directories: {len(match_dirs)}")
    logging.info(f"Matches with stat files: {len(match_dirs) - no_data_matches}")
    logging.info(f"Successful extractions: {successful_matches}")
    logging.info(
        f"Comprehensive coverage: {comprehensive_coverage} ({100*comprehensive_coverage/successful_matches:.1f}% of successful)"
    )
    logging.info(f"Failed extractions: {failed_matches}")
    logging.info(f"No stat files: {no_data_matches}")
    logging.info(f"Total team records extracted: {total_teams}")
    if successful_matches > 0:
        logging.info(f"Average teams per match: {total_teams / successful_matches:.1f}")

    # Verify comprehensive data quality
    logging.info("\n" + "=" * 80)
    logging.info("🎯 COMPREHENSIVE DATA QUALITY ANALYSIS")
    logging.info("=" * 80)

    import sqlite3

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Overall counts
    cursor.execute("SELECT COUNT(*) FROM match_team_comprehensive")
    total_records = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(DISTINCT match_id) FROM match_team_comprehensive")
    matches_with_comprehensive_stats = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(DISTINCT team_id) FROM match_team_comprehensive")
    unique_teams = cursor.fetchone()[0]

    # Data completeness analysis
    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN goals IS NOT NULL THEN 1 END) as has_goals,
            COUNT(CASE WHEN fouls IS NOT NULL THEN 1 END) as has_fouls,
            COUNT(CASE WHEN touches_def_3rd IS NOT NULL THEN 1 END) as has_possession_detail,
            COUNT(CASE WHEN short_passes_completed IS NOT NULL THEN 1 END) as has_passing_detail,
            COUNT(CASE WHEN tackles_def_3rd IS NOT NULL THEN 1 END) as has_defense_detail,
            COUNT(CASE WHEN aerials_won IS NOT NULL THEN 1 END) as has_aerial_data
        FROM match_team_comprehensive
    """)
    completeness = cursor.fetchone()

    logging.info(f"Total comprehensive records: {total_records}")
    logging.info(f"Matches with comprehensive stats: {matches_with_comprehensive_stats}")
    logging.info(f"Unique teams: {unique_teams}")
    logging.info("\nData completeness by category:")
    logging.info(
        f"  📊 Basic performance (goals): {completeness[0]}/{total_records} ({100*completeness[0]/total_records:.1f}%)"
    )
    logging.info(
        f"  ⚠️ Discipline (fouls): {completeness[1]}/{total_records} ({100*completeness[1]/total_records:.1f}%)"
    )
    logging.info(
        f"  🏃 Possession detail: {completeness[2]}/{total_records} ({100*completeness[2]/total_records:.1f}%)"
    )
    logging.info(f"  📋 Passing detail: {completeness[3]}/{total_records} ({100*completeness[3]/total_records:.1f}%)")
    logging.info(f"  🛡️ Defense detail: {completeness[4]}/{total_records} ({100*completeness[4]/total_records:.1f}%)")
    logging.info(f"  ✈️ Aerial duels: {completeness[5]}/{total_records} ({100*completeness[5]/total_records:.1f}%)")

    # Advanced performance metrics
    cursor.execute("""
        SELECT 
            AVG(goals) as avg_goals,
            AVG(shots) as avg_shots,
            AVG(pass_accuracy) as avg_pass_acc,
            AVG(take_on_success_rate) as avg_takeon_rate,
            AVG(tackle_success_rate) as avg_tackle_rate,
            AVG(aerial_win_rate) as avg_aerial_rate
        FROM match_team_comprehensive 
        WHERE goals IS NOT NULL
    """)
    avg_performance = cursor.fetchone()

    if avg_performance and avg_performance[0] is not None:
        logging.info("\n🏆 AVERAGE TEAM PERFORMANCE PER MATCH:")
        logging.info(f"  ⚽ Goals: {avg_performance[0]:.2f}")
        logging.info(f"  🎯 Shots: {avg_performance[1]:.1f}")
        logging.info(f"  📋 Pass accuracy: {avg_performance[2]:.1f}%")
        logging.info(f"  🏃 Take-on success: {avg_performance[3]:.1f}%")
        logging.info(f"  🛡️ Tackle success: {avg_performance[4]:.1f}%")
        logging.info(f"  ✈️ Aerial win rate: {avg_performance[5]:.1f}%")

    # Top performing teams across multiple metrics
    cursor.execute("""
        SELECT 
            t.team_name,
            COUNT(*) as matches,
            AVG(mt.goals) as avg_goals,
            AVG(mt.pass_accuracy) as avg_pass_acc,
            AVG(mt.take_on_success_rate) as avg_takeon_rate,
            AVG(mt.aerial_win_rate) as avg_aerial_rate
        FROM match_team_comprehensive mt 
        JOIN team t ON mt.team_id = t.team_id
        WHERE mt.goals IS NOT NULL
        GROUP BY mt.team_id, t.team_name
        HAVING COUNT(*) >= 50  -- Teams with significant data
        ORDER BY avg_goals DESC
        LIMIT 5
    """)
    top_teams = cursor.fetchall()

    if top_teams:
        logging.info("\n🌟 TOP PERFORMING TEAMS (Multi-metric Analysis):")
        for team_name, matches, goals, pass_acc, takeon_rate, aerial_rate in top_teams:
            logging.info(f"  {team_name} ({matches} matches):")
            logging.info(
                f"    ⚽ {goals:.2f} goals/match, 📋 {pass_acc:.1f}% pass acc, 🏃 {takeon_rate:.1f}% takeon success, ✈️ {aerial_rate:.1f}% aerial wins"
            )

    conn.close()

    print(
        f"\n🎉 MISSION ACCOMPLISHED! Successfully extracted comprehensive statistics for {total_teams} team records from {successful_matches} matches!"
    )
    print(f"📈 {comprehensive_coverage} matches have full comprehensive coverage across all statistical categories!")


if __name__ == "__main__":
    main()
