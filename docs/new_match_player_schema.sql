-- Comprehensive match_player table matching FBRef structure exactly
CREATE TABLE match_player (
    -- identifiers
    match_player_id TEXT PRIMARY KEY,
    match_id        TEXT  NOT NULL,
    player_id       TEXT,
    player_name     TEXT NOT NULL,
    team_id         TEXT NOT NULL,
    team_name       TEXT,

    -- basic bio / lineup
    shirt_number    INTEGER,
    position        TEXT,
    minutes_played  INTEGER,

    /* ---------- SUMMARY TAB ---------- */
    -- Performance
    summary_perf_gls        INTEGER,
    summary_perf_ast        INTEGER,
    summary_perf_pk         INTEGER,
    summary_perf_pkatt      INTEGER,
    summary_perf_sh         INTEGER,
    summary_perf_sot        INTEGER,
    summary_perf_crdy       INTEGER,
    summary_perf_crdr       INTEGER,
    summary_perf_touches    INTEGER,
    summary_perf_tkl        INTEGER,
    summary_perf_int        INTEGER,
    summary_perf_blocks     INTEGER,

    -- Expected
    summary_exp_xg          REAL,
    summary_exp_npxg        REAL,
    summary_exp_xag         REAL,

    -- Shot-/Goal-Creating
    summary_sca_sca         INTEGER,
    summary_sca_gca         INTEGER,

    -- Passing snippet
    summary_pass_cmp        INTEGER,
    summary_pass_att        INTEGER,
    summary_pass_cmp_pct    REAL,
    summary_pass_prgp       INTEGER,

    -- Carries & Take-ons
    summary_carry_carries   INTEGER,
    summary_carry_prgc      INTEGER,
    summary_take_att        INTEGER,
    summary_take_succ       INTEGER,

    /* ---------- PASSING TAB ---------- */
    -- Totals
    passing_total_cmp       INTEGER,
    passing_total_att       INTEGER,
    passing_total_cmp_pct   REAL,
    passing_total_totdist   INTEGER,
    passing_total_prgdist   INTEGER,

    -- Range splits
    passing_short_cmp       INTEGER,
    passing_short_att       INTEGER,
    passing_short_cmp_pct   REAL,
    passing_medium_cmp      INTEGER,
    passing_medium_att      INTEGER,
    passing_medium_cmp_pct  REAL,
    passing_long_cmp        INTEGER,
    passing_long_att        INTEGER,
    passing_long_cmp_pct    REAL,

    -- Value add
    passing_ast             INTEGER,
    passing_xag             REAL,
    passing_xa              REAL,
    passing_kp              INTEGER,
    passing_final_third     INTEGER,   -- "1/3"
    passing_ppa             INTEGER,
    passing_crspa           INTEGER,
    passing_prgp            INTEGER,

    /* ---------- PASS TYPES TAB ---------- */
    -- Pass-Types breakdown
    pass_types_att          INTEGER,
    pass_types_live         INTEGER,
    pass_types_dead         INTEGER,
    pass_types_fk           INTEGER,
    pass_types_tb           INTEGER,
    pass_types_sw           INTEGER,
    pass_types_crs          INTEGER,
    pass_types_ti           INTEGER,
    pass_types_ck           INTEGER,

    -- Corner kicks
    corner_in               INTEGER,
    corner_out              INTEGER,
    corner_str              INTEGER,

    -- Outcomes
    pass_outcome_cmp        INTEGER,
    pass_outcome_off        INTEGER,
    pass_outcome_blocks     INTEGER,

    /* ---------- DEFENSIVE ACTIONS TAB ---------- */
    -- Tackles
    def_tkl                 INTEGER,
    def_tklw                INTEGER,
    def_tkl_def_3rd         INTEGER,
    def_tkl_mid_3rd         INTEGER,
    def_tkl_att_3rd         INTEGER,

    -- Challenges
    def_chal_tkl            INTEGER,
    def_chal_att            INTEGER,
    def_chal_tkl_pct        REAL,
    def_chal_lost           INTEGER,

    -- Blocks
    def_blocks_total        INTEGER,
    def_blocks_sh           INTEGER,
    def_blocks_pass         INTEGER,

    -- Other defensive bits
    def_int                 INTEGER,
    def_tkl_int             INTEGER,
    def_clr                 INTEGER,
    def_err                 INTEGER,

    /* ---------- POSSESSION TAB ---------- */
    -- Touches
    poss_touches            INTEGER,
    poss_touches_def_pen    INTEGER,
    poss_touches_def_3rd    INTEGER,
    poss_touches_mid_3rd    INTEGER,
    poss_touches_att_3rd    INTEGER,
    poss_touches_att_pen    INTEGER,
    poss_touches_live       INTEGER,

    -- Take-ons
    poss_take_att           INTEGER,
    poss_take_succ          INTEGER,
    poss_take_succ_pct      REAL,
    poss_take_tkld          INTEGER,
    poss_take_tkld_pct      REAL,

    -- Carries
    poss_carry_carries      INTEGER,
    poss_carry_totdist      INTEGER,
    poss_carry_prgdist      INTEGER,
    poss_carry_prgc         INTEGER,
    poss_carry_final_third  INTEGER,   -- "1/3"
    poss_carry_cpa          INTEGER,
    poss_carry_mis          INTEGER,
    poss_carry_dis          INTEGER,

    -- Receiving
    poss_rec_rec            INTEGER,
    poss_rec_prgr           INTEGER,

    /* ---------- MISC STATS TAB ---------- */
    misc_crdy               INTEGER,
    misc_crdr               INTEGER,
    misc_2crdy              INTEGER,
    misc_fls                INTEGER,
    misc_fld                INTEGER,
    misc_off                INTEGER,
    misc_crs                INTEGER,
    misc_int                INTEGER,
    misc_tklw               INTEGER,
    misc_pkwon              INTEGER,
    misc_pkcon              INTEGER,
    misc_og                 INTEGER,
    misc_recov              INTEGER,

    /* ---------- AERIAL DUELS (MISC TAB) ---------- */
    aerial_won              INTEGER,
    aerial_lost             INTEGER,
    aerial_won_pct          REAL,

    -- Foreign key constraints
    FOREIGN KEY (match_id) REFERENCES matches(match_id),
    FOREIGN KEY (player_id) REFERENCES player(player_id),
    FOREIGN KEY (team_id) REFERENCES teams(team_id)
);