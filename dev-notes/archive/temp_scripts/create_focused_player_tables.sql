-- Create 6 focused player stats tables based on FBRef structure
-- These complement the existing match_player table

-- 1. SUMMARY TAB - Core performance stats
CREATE TABLE match_player_summary (
    match_player_summary_id TEXT PRIMARY KEY,
    match_player_id TEXT NOT NULL,
    match_id TEXT NOT NULL,
    player_id TEXT,
    player_name TEXT NOT NULL,
    team_id TEXT NOT NULL,
    
    -- Basic match info
    shirt_number INTEGER,
    position TEXT,
    age TEXT,
    minutes_played INTEGER,
    
    -- Performance stats
    goals INTEGER,
    assists INTEGER,
    penalty_kicks INTEGER,
    penalty_kicks_attempted INTEGER,
    shots INTEGER,
    shots_on_target INTEGER,
    yellow_cards INTEGER,
    red_cards INTEGER,
    touches INTEGER,
    tackles INTEGER,
    interceptions INTEGER,
    blocks INTEGER,
    
    -- Expected stats
    xg REAL,
    npxg REAL,
    xag REAL,
    
    -- Shot/Goal Creating Actions
    sca INTEGER,
    gca INTEGER,
    
    -- Passing snippet
    passes_completed INTEGER,
    passes_attempted INTEGER,
    pass_completion_pct REAL,
    progressive_passes INTEGER,
    
    -- Carries & Take-ons snippet
    carries INTEGER,
    progressive_carries INTEGER,
    take_ons_attempted INTEGER,
    take_ons_successful INTEGER,
    
    FOREIGN KEY (match_player_id) REFERENCES match_player(match_player_id),
    FOREIGN KEY (match_id) REFERENCES match(match_id),
    FOREIGN KEY (player_id) REFERENCES player(player_id)
);

-- 2. PASSING TAB - Detailed passing statistics
CREATE TABLE match_player_passing (
    match_player_passing_id TEXT PRIMARY KEY,
    match_player_id TEXT NOT NULL,
    
    -- Total passing
    total_completed INTEGER,
    total_attempted INTEGER,
    total_completion_pct REAL,
    total_distance INTEGER,
    progressive_distance INTEGER,
    
    -- Short passes (5-15 yards)
    short_completed INTEGER,
    short_attempted INTEGER,
    short_completion_pct REAL,
    
    -- Medium passes (15-30 yards)
    medium_completed INTEGER,
    medium_attempted INTEGER,
    medium_completion_pct REAL,
    
    -- Long passes (30+ yards)
    long_completed INTEGER,
    long_attempted INTEGER,
    long_completion_pct REAL,
    
    -- Value-added passing
    assists INTEGER,
    xag REAL,
    xa REAL,
    key_passes INTEGER,
    final_third_passes INTEGER,
    penalty_area_passes INTEGER,
    cross_penalty_area_passes INTEGER,
    progressive_passes INTEGER,
    
    FOREIGN KEY (match_player_id) REFERENCES match_player(match_player_id)
);

-- 3. PASS TYPES TAB - Pass type breakdowns
CREATE TABLE match_player_pass_types (
    match_player_pass_types_id TEXT PRIMARY KEY,
    match_player_id TEXT NOT NULL,
    
    -- Pass types
    pass_attempts INTEGER,
    live_passes INTEGER,
    dead_passes INTEGER,
    free_kicks INTEGER,
    through_balls INTEGER,
    switches INTEGER,
    crosses INTEGER,
    throw_ins INTEGER,
    corner_kicks INTEGER,
    
    -- Corner kick types
    corner_kicks_in INTEGER,
    corner_kicks_out INTEGER,
    corner_kicks_straight INTEGER,
    
    -- Outcomes
    completed INTEGER,
    offsides INTEGER,
    blocked INTEGER,
    
    FOREIGN KEY (match_player_id) REFERENCES match_player(match_player_id)
);

-- 4. DEFENSIVE ACTIONS TAB - All defensive metrics
CREATE TABLE match_player_defensive_actions (
    match_player_defensive_actions_id TEXT PRIMARY KEY,
    match_player_id TEXT NOT NULL,
    
    -- Tackles
    tackles INTEGER,
    tackles_won INTEGER,
    tackles_def_3rd INTEGER,
    tackles_mid_3rd INTEGER,
    tackles_att_3rd INTEGER,
    
    -- Challenges
    challenge_tackles INTEGER,
    challenges_attempted INTEGER,
    challenge_tackle_pct REAL,
    challenges_lost INTEGER,
    
    -- Blocks
    blocks INTEGER,
    shots_blocked INTEGER,
    passes_blocked INTEGER,
    
    -- Other defensive actions
    interceptions INTEGER,
    tackles_plus_interceptions INTEGER,
    clearances INTEGER,
    errors INTEGER,
    
    FOREIGN KEY (match_player_id) REFERENCES match_player(match_player_id)
);

-- 5. POSSESSION TAB - Touches, carries, take-ons
CREATE TABLE match_player_possession (
    match_player_possession_id TEXT PRIMARY KEY,
    match_player_id TEXT NOT NULL,
    
    -- Touches
    touches INTEGER,
    touches_def_penalty_area INTEGER,
    touches_def_3rd INTEGER,
    touches_mid_3rd INTEGER,
    touches_att_3rd INTEGER,
    touches_att_penalty_area INTEGER,
    touches_live_ball INTEGER,
    
    -- Take-ons
    take_ons_attempted INTEGER,
    take_ons_successful INTEGER,
    take_on_success_pct REAL,
    times_tackled INTEGER,
    times_tackled_pct REAL,
    
    -- Carries
    carries INTEGER,
    total_carrying_distance INTEGER,
    progressive_carrying_distance INTEGER,
    progressive_carries INTEGER,
    carries_final_third INTEGER,
    carries_penalty_area INTEGER,
    miscontrols INTEGER,
    dispossessed INTEGER,
    
    -- Receiving
    passes_received INTEGER,
    progressive_passes_received INTEGER,
    
    FOREIGN KEY (match_player_id) REFERENCES match_player(match_player_id)
);

-- 6. MISCELLANEOUS STATS TAB - Cards, fouls, aerial duels
CREATE TABLE match_player_misc (
    match_player_misc_id TEXT PRIMARY KEY,
    match_player_id TEXT NOT NULL,
    
    -- Cards
    yellow_cards INTEGER,
    red_cards INTEGER,
    second_yellow_cards INTEGER,
    
    -- Fouls
    fouls_committed INTEGER,
    fouls_drawn INTEGER,
    offsides INTEGER,
    crosses INTEGER,
    interceptions INTEGER,
    tackles_won INTEGER,
    
    -- Penalties
    penalty_kicks_won INTEGER,
    penalty_kicks_conceded INTEGER,
    own_goals INTEGER,
    
    -- Other
    ball_recoveries INTEGER,
    
    -- Aerial duels
    aerial_duels_won INTEGER,
    aerial_duels_lost INTEGER,
    aerial_duel_win_pct REAL,
    
    FOREIGN KEY (match_player_id) REFERENCES match_player(match_player_id)
);

-- Create indexes for efficient querying
CREATE INDEX idx_match_player_summary_match_id ON match_player_summary(match_id);
CREATE INDEX idx_match_player_summary_player_id ON match_player_summary(player_id);
CREATE INDEX idx_match_player_summary_match_player_id ON match_player_summary(match_player_id);

CREATE INDEX idx_match_player_passing_match_player_id ON match_player_passing(match_player_id);
CREATE INDEX idx_match_player_pass_types_match_player_id ON match_player_pass_types(match_player_id);
CREATE INDEX idx_match_player_defensive_actions_match_player_id ON match_player_defensive_actions(match_player_id);
CREATE INDEX idx_match_player_possession_match_player_id ON match_player_possession(match_player_id);
CREATE INDEX idx_match_player_misc_match_player_id ON match_player_misc(match_player_id);