-- Comprehensive match_player table with ALL FBRef fields from screenshots

-- Summary tab fields:
-- Player, #, Nation, Pos, Age, Min, Gls, Ast, PK, PKatt, Sh, SoT, CrdY, CrdR, Touches, Tkl, Int, Blocks, xG, npxG, xAG, SCA, GCA, Cmp, Att, Cmp%, PrgP, Carries, PrgC, Att, Succ

-- Passing tab fields:
-- Player, #, Nation, Pos, Age, Min, Cmp, Att, Cmp%, TotDist, PrgDist, Cmp, Att, Cmp%, Cmp, Att, Cmp%, Cmp, Att, Cmp%, Ast, xAG, xA, KP, 1/3, PPA, CrsPA, PrgP

-- Pass Types tab fields:  
-- Player, #, Nation, Pos, Age, Min, Att, Live, Dead, FK, TB, Sw, Crs, TI, CK, In, Out, Str, Cmp, Off, Blocks

-- Defensive Actions tab fields:
-- Player, #, Nation, Pos, Age, Min, Tkl, TklW, Def 3rd, Mid 3rd, Att 3rd, Tkl, Att, Tkl%, Lost, Blocks, Sh, Pass, Int, Tkl+Int, Clr, Err

-- Possession tab fields:
-- Player, #, Nation, Pos, Age, Min, Touches, Def Pen, Def 3rd, Mid 3rd, Att 3rd, Att Pen, Live, Att, Succ, Succ%, Tkld, Tkld%, Carries, TotDist, PrgDist, PrgC, 1/3, CPA, Mis, Dis, Rec, PrgR

-- Miscellaneous Stats tab fields:
-- Player, #, Nation, Pos, Age, Min, CrdY, CrdR, 2CrdY, Fls, Fld, Off, Crs, Int, TklW, PKwon, PKcon, OG, Recov, Won, Lost, Won%

-- Additional fields needed:
ALTER TABLE match_player ADD COLUMN player_number INTEGER DEFAULT NULL;
ALTER TABLE match_player ADD COLUMN nation TEXT DEFAULT NULL;
ALTER TABLE match_player ADD COLUMN position TEXT DEFAULT NULL;
ALTER TABLE match_player ADD COLUMN age TEXT DEFAULT NULL;

-- Performance stats (missing)
ALTER TABLE match_player ADD COLUMN penalty_goals INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN penalty_attempts INTEGER DEFAULT 0;

-- Passing detailed stats (missing)
ALTER TABLE match_player ADD COLUMN total_pass_distance INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN progressive_pass_distance INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN short_pass_accuracy REAL DEFAULT 0.0;
ALTER TABLE match_player ADD COLUMN medium_pass_accuracy REAL DEFAULT 0.0;
ALTER TABLE match_player ADD COLUMN long_pass_accuracy REAL DEFAULT 0.0;
ALTER TABLE match_player ADD COLUMN assists_expected REAL DEFAULT 0.0;
ALTER TABLE match_player ADD COLUMN key_passes INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN passes_into_final_third INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN passes_into_penalty_area INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN crosses_into_penalty_area INTEGER DEFAULT 0;

-- Pass Types stats
ALTER TABLE match_player ADD COLUMN pass_attempts_total INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN live_ball_passes INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN dead_ball_passes INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN free_kick_passes INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN through_balls INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN switches INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN crosses INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN throw_ins INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN corner_kicks INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN corner_kicks_in INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN corner_kicks_out INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN corner_kicks_straight INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN passes_completed_total INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN passes_offside INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN passes_blocked INTEGER DEFAULT 0;

-- Defensive Actions detailed stats
ALTER TABLE match_player ADD COLUMN tackles_won INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN tackles_def_3rd INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN tackles_mid_3rd INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN tackles_att_3rd INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN dribble_tackles INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN dribble_tackle_attempts INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN dribble_tackle_pct REAL DEFAULT 0.0;
ALTER TABLE match_player ADD COLUMN dribble_tackles_lost INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN blocked_shots INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN blocked_passes INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN tackles_interceptions INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN errors INTEGER DEFAULT 0;

-- Possession detailed stats
ALTER TABLE match_player ADD COLUMN touches_def_pen INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN touches_def_3rd INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN touches_mid_3rd INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN touches_att_3rd INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN touches_att_pen INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN touches_live_ball INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN dribble_success_pct REAL DEFAULT 0.0;
ALTER TABLE match_player ADD COLUMN dribbles_tackled INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN dribbles_tackled_pct REAL DEFAULT 0.0;
ALTER TABLE match_player ADD COLUMN carry_distance INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN progressive_carry_distance INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN carries_into_final_third INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN carries_into_penalty_area INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN miscontrols INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN dispossessed INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN passes_received INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN progressive_passes_received INTEGER DEFAULT 0;

-- Miscellaneous Stats
ALTER TABLE match_player ADD COLUMN second_yellow_cards INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN offsides INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN crosses_total INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN interceptions_total INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN tackles_won_total INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN penalty_kicks_won INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN penalty_kicks_conceded INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN own_goals INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN ball_recoveries INTEGER DEFAULT 0;
ALTER TABLE match_player ADD COLUMN aerial_duels_won_pct REAL DEFAULT 0.0;