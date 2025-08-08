#!/usr/bin/env python3
"""
Create Minimal Team Summary - Starting with 2013 fields only
"""

import logging
import sqlite3

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def create_minimal_team_summary_table(db_path: str):
    """Create minimal team summary table with only 2013 fields."""
    conn = sqlite3.connect(db_path)

    create_sql = """
    CREATE TABLE IF NOT EXISTS match_team_summary (
        team_stats_id TEXT PRIMARY KEY,
        match_id TEXT NOT NULL,
        team_id TEXT NOT NULL,
        match_date DATE,
        
        -- 2013 Basic Stats
        goals INTEGER DEFAULT 0,           -- Gls
        assists INTEGER DEFAULT 0,        -- Ast
        penalty_goals INTEGER DEFAULT 0,  -- PK
        penalty_attempts INTEGER DEFAULT 0, -- PKatt
        shots INTEGER DEFAULT 0,          -- Sh
        shots_on_target INTEGER DEFAULT 0, -- SoT
        yellow_cards INTEGER DEFAULT 0,   -- CrdY
        red_cards INTEGER DEFAULT 0,      -- CrdR
        fouls INTEGER DEFAULT 0,          -- Fls
        fouled INTEGER DEFAULT 0,         -- Fld
        offsides INTEGER DEFAULT 0,       -- Off
        corners INTEGER DEFAULT 0,        -- Crs
        
        FOREIGN KEY (match_id) REFERENCES match(match_id),
        FOREIGN KEY (team_id) REFERENCES team(team_id),
        UNIQUE(match_id, team_id)
    );
    """

    conn.execute(create_sql)
    conn.commit()
    conn.close()

    logging.info("✅ Created minimal match_team_summary table with 2013 fields")


if __name__ == "__main__":
    db_path = "/Users/thomasmcmillan/projects/nwsl_data/data/processed/nwsldata.db"

    logging.info("🚀 CREATING MINIMAL TEAM SUMMARY TABLE")
    logging.info("=" * 50)

    create_minimal_team_summary_table(db_path)

    logging.info("🎉 Table created! Ready for 2013 data")
