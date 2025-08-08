#!/usr/bin/env python3
"""
Create Match Team Summary from Existing Player Data
Aggregates team-level statistics from the existing match_player table.
This approach leverages your existing, clean player data rather than re-parsing CSVs.
"""

import hashlib
import logging
import sqlite3

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def generate_team_stats_id(match_id: str, team_id: str) -> str:
    """Generate unique team stats ID."""
    content = f"team_summary_{match_id}_{team_id}"
    hex_hash = hashlib.md5(content.encode()).hexdigest()[:8]
    return f"team_{hex_hash}"


def create_team_summary_table(db_path: str):
    """Create the team summary table with essential team statistics."""

    conn = sqlite3.connect(db_path)

    # Create simplified team summary table
    create_sql = """
    CREATE TABLE IF NOT EXISTS match_team_summary (
        team_stats_id TEXT PRIMARY KEY,
        match_id TEXT NOT NULL,
        team_id TEXT NOT NULL,
        match_date DATE,
        
        -- Basic Performance
        goals INTEGER DEFAULT 0,
        assists INTEGER DEFAULT 0,
        shots INTEGER DEFAULT 0,
        shots_on_target INTEGER DEFAULT 0,
        yellow_cards INTEGER DEFAULT 0,
        red_cards INTEGER DEFAULT 0,
        
        -- Core Passing
        passes_completed INTEGER DEFAULT 0,
        passes_attempted INTEGER DEFAULT 0,
        pass_accuracy REAL DEFAULT 0.0,
        progressive_passes INTEGER DEFAULT 0,
        
        -- Core Defensive
        tackles INTEGER DEFAULT 0,
        interceptions INTEGER DEFAULT 0,
        blocks INTEGER DEFAULT 0,
        clearances INTEGER DEFAULT 0,
        
        -- Core Possession
        touches INTEGER DEFAULT 0,
        carries INTEGER DEFAULT 0,
        take_ons_attempted INTEGER DEFAULT 0,
        take_ons_successful INTEGER DEFAULT 0,
        
        -- Discipline & Set Pieces
        fouls INTEGER DEFAULT 0,
        fouled INTEGER DEFAULT 0,
        offsides INTEGER DEFAULT 0,
        corners INTEGER DEFAULT 0,
        
        -- Calculated Metrics
        shot_accuracy REAL DEFAULT 0.0,
        take_on_success_rate REAL DEFAULT 0.0,
        
        -- Meta
        players_used INTEGER DEFAULT 0,
        
        FOREIGN KEY (match_id) REFERENCES match(match_id),
        FOREIGN KEY (team_id) REFERENCES team(team_id),
        UNIQUE(match_id, team_id)
    );
    """

    conn.execute(create_sql)
    conn.commit()
    conn.close()

    logging.info("✅ Created match_team_summary table")


def aggregate_team_stats_from_players(db_path: str):
    """
    Aggregate team statistics from existing match_player data.
    This is much more reliable than parsing CSV headers.
    """

    conn = sqlite3.connect(db_path)

    # SQL to aggregate team stats from match_player table using actual column names
    aggregate_sql = """
    INSERT OR REPLACE INTO match_team_summary (
        team_stats_id, match_id, team_id, match_date,
        goals, assists, shots, shots_on_target, yellow_cards, red_cards,
        passes_completed, passes_attempted, pass_accuracy, progressive_passes,
        tackles, interceptions, blocks, clearances,
        touches, carries, take_ons_attempted, take_ons_successful,
        fouls, fouled, offsides, corners,
        shot_accuracy, take_on_success_rate, players_used
    )
    SELECT 
        -- Generate team stats ID
        'team_' || substr(
            lower(hex(randomblob(4))), 1, 8
        ) as team_stats_id,
        
        mp.match_id,
        mp.team_id,
        m.match_date,
        
        -- Basic Performance (using actual column names)
        COALESCE(SUM(mp.summary_perf_gls), 0) as goals,
        COALESCE(SUM(mp.summary_perf_ast), 0) as assists,
        COALESCE(SUM(mp.summary_perf_sh), 0) as shots,
        COALESCE(SUM(mp.summary_perf_sot), 0) as shots_on_target,
        COALESCE(SUM(mp.summary_perf_crdy), 0) as yellow_cards,
        COALESCE(SUM(mp.summary_perf_crdr), 0) as red_cards,
        
        -- Core Passing (using actual column names)
        COALESCE(SUM(mp.summary_pass_cmp), 0) as passes_completed,
        COALESCE(SUM(mp.summary_pass_att), 0) as passes_attempted,
        CASE 
            WHEN SUM(mp.summary_pass_att) > 0 
            THEN ROUND((SUM(mp.summary_pass_cmp) * 100.0 / SUM(mp.summary_pass_att)), 1)
            ELSE 0.0 
        END as pass_accuracy,
        COALESCE(SUM(mp.summary_pass_prgp), 0) as progressive_passes,
        
        -- Core Defensive (using actual column names)
        COALESCE(SUM(mp.summary_perf_tkl), 0) as tackles,
        COALESCE(SUM(mp.summary_perf_int), 0) as interceptions,
        COALESCE(SUM(mp.summary_perf_blocks), 0) as blocks,
        COALESCE(SUM(mp.def_clr), 0) as clearances,
        
        -- Core Possession (using actual column names)
        COALESCE(SUM(mp.summary_perf_touches), 0) as touches,
        COALESCE(SUM(mp.summary_carry_carries), 0) as carries,
        COALESCE(SUM(mp.summary_take_att), 0) as take_ons_attempted,
        COALESCE(SUM(mp.summary_take_succ), 0) as take_ons_successful,
        
        -- Discipline & Set Pieces (using actual column names)
        COALESCE(SUM(mp.misc_fls), 0) as fouls,
        COALESCE(SUM(mp.misc_fld), 0) as fouled,
        COALESCE(SUM(mp.misc_off), 0) as offsides,
        COALESCE(SUM(mp.misc_crs), 0) as corners,
        
        -- Calculated Metrics
        CASE 
            WHEN SUM(mp.summary_perf_sh) > 0 
            THEN ROUND((SUM(mp.summary_perf_sot) * 100.0 / SUM(mp.summary_perf_sh)), 1)
            ELSE 0.0 
        END as shot_accuracy,
        CASE 
            WHEN SUM(mp.summary_take_att) > 0 
            THEN ROUND((SUM(mp.summary_take_succ) * 100.0 / SUM(mp.summary_take_att)), 1)
            ELSE 0.0 
        END as take_on_success_rate,
        
        -- Meta
        COUNT(*) as players_used
        
    FROM match_player mp
    JOIN match m ON mp.match_id = m.match_id
    WHERE mp.match_id IS NOT NULL 
      AND mp.team_id IS NOT NULL
    GROUP BY mp.match_id, mp.team_id, m.match_date
    ORDER BY mp.match_id, mp.team_id;
    """

    try:
        cursor = conn.execute(aggregate_sql)
        rows_affected = cursor.rowcount
        conn.commit()

        logging.info(f"✅ Aggregated team stats for {rows_affected} team-match combinations")

        # Get summary statistics
        summary_sql = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT match_id) as unique_matches,
            COUNT(DISTINCT team_id) as unique_teams,
            AVG(goals) as avg_goals_per_team,
            AVG(pass_accuracy) as avg_pass_accuracy,
            AVG(players_used) as avg_players_per_team
        FROM match_team_summary;
        """

        summary = conn.execute(summary_sql).fetchone()

        logging.info("📊 SUMMARY STATISTICS:")
        logging.info(f"   Total team records: {summary[0]}")
        logging.info(f"   Unique matches: {summary[1]}")
        logging.info(f"   Unique teams: {summary[2]}")
        logging.info(f"   Average goals per team: {summary[3]:.1f}")
        logging.info(f"   Average pass accuracy: {summary[4]:.1f}%")
        logging.info(f"   Average players per team: {summary[5]:.1f}")

        return rows_affected

    except Exception as e:
        logging.error(f"❌ Error aggregating team stats: {e}")
        conn.rollback()
        return 0

    finally:
        conn.close()


def validate_team_summary_data(db_path: str):
    """Validate the created team summary data."""

    conn = sqlite3.connect(db_path)

    # Check for data quality issues
    validation_sql = """
    SELECT 
        'Empty records' as check_type,
        COUNT(*) as count
    FROM match_team_summary 
    WHERE goals IS NULL AND shots IS NULL AND passes_completed IS NULL
    
    UNION ALL
    
    SELECT 
        'Matches with wrong team count' as check_type,
        COUNT(*) as count
    FROM (
        SELECT match_id, COUNT(*) as team_count
        FROM match_team_summary
        GROUP BY match_id
        HAVING COUNT(*) != 2
    )
    
    UNION ALL
    
    SELECT 
        'High-performing teams (>5 goals)' as check_type,
        COUNT(*) as count
    FROM match_team_summary 
    WHERE goals > 5
    
    UNION ALL
    
    SELECT 
        'Perfect pass accuracy teams' as check_type,
        COUNT(*) as count
    FROM match_team_summary 
    WHERE pass_accuracy = 100.0 AND passes_attempted > 10;
    """

    validation_results = conn.execute(validation_sql).fetchall()

    logging.info("🔍 DATA VALIDATION:")
    for check_type, count in validation_results:
        logging.info(f"   {check_type}: {count}")

    # Show sample data
    sample_sql = """
    SELECT 
        match_id, team_id, goals, shots, passes_completed, passes_attempted, 
        pass_accuracy, players_used
    FROM match_team_summary 
    ORDER BY match_id 
    LIMIT 5;
    """

    sample_data = conn.execute(sample_sql).fetchall()

    logging.info("📋 SAMPLE DATA:")
    for row in sample_data:
        logging.info(
            f"   Match {row[0][:8]}, Team {row[1][:8]}: {row[2]} goals, {row[3]} shots, {row[6]:.1f}% pass accuracy, {row[7]} players"
        )

    conn.close()


def clean_old_comprehensive_table(db_path: str):
    """Clean out empty records from the old comprehensive table."""

    conn = sqlite3.connect(db_path)

    try:
        # Count empty records
        count_sql = """
        SELECT COUNT(*) FROM match_team_comprehensive 
        WHERE goals IS NULL AND shots IS NULL AND passes_completed IS NULL;
        """
        empty_count = conn.execute(count_sql).fetchone()[0]

        if empty_count > 0:
            # Delete empty records
            delete_sql = """
            DELETE FROM match_team_comprehensive 
            WHERE goals IS NULL AND shots IS NULL AND passes_completed IS NULL;
            """

            conn.execute(delete_sql)
            conn.commit()

            logging.info(f"🧹 Cleaned {empty_count} empty records from match_team_comprehensive")
        else:
            logging.info("🧹 No empty records found in match_team_comprehensive")

    except Exception as e:
        logging.error(f"❌ Error cleaning comprehensive table: {e}")

    finally:
        conn.close()


if __name__ == "__main__":
    # Configuration
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"

    logging.info("🚀 CREATING TEAM SUMMARY FROM PLAYER DATA")
    logging.info("=" * 60)

    # Step 1: Create the team summary table
    create_team_summary_table(db_path)

    # Step 2: Aggregate team stats from existing match_player data
    team_records = aggregate_team_stats_from_players(db_path)

    if team_records > 0:
        # Step 3: Validate the data
        validate_team_summary_data(db_path)

        # Step 4: Clean the old comprehensive table (optional)
        clean_old_comprehensive_table(db_path)

        logging.info("=" * 60)
        logging.info("🎉 SUCCESS! Team summary table created from player data")
        logging.info(f"📊 {team_records} team records created")
        logging.info("💡 This approach leverages your existing, clean match_player data")
        logging.info("✨ Much more reliable than parsing complex CSV headers!")

    else:
        logging.error("❌ Failed to create team summary data")
