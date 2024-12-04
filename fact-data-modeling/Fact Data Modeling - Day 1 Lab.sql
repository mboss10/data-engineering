SELECT * FROM game_details gd

/* 
 * Identify the grain of the `game_details` table
 */

SELECT 	
	game_id 
	,team_id 
	,player_id 
	,count(1)
FROM game_details gd 
GROUP BY 1,2,3
HAVING count(1) > 1 

/*
 * Since we have duplicates in `game_details` table we are going to create a CTE to deduplicate the data first, before building our query for our fact table
 */

WITH cte_deduped AS (
	SELECT
		g.game_date_est
		,g.season
		,g.home_team_id
		,gd.*
		,row_number() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS row_num
	FROM game_details gd 
	INNER JOIN games g ON gd.game_id = g.game_id
)
SELECT 	
	game_date_est AS dim_game_date
	,season AS dim_season
	,team_id AS dim_team_id
	,player_id AS dim_player_id
	,player_name AS dim_player_name
	,start_position AS dim_start_position
	,team_id = home_team_id AS dim_is_playing_at_home
	,COALESCE(POSITION('DNP'IN "comment"),0) > 0 AS dim_did_not_play
	,COALESCE(POSITION('DND'IN "comment"),0) > 0 AS dim_did_not_dress
	,COALESCE(POSITION('NWT'IN "comment"),0) > 0 AS dim_not_with_team
	,split_part("min",':',1)::REAL + split_part("min",':',2)::REAL/60 AS m_minutes
	,fgm AS m_fgm
	,fga AS m_fga
	,fg3m AS m_fg3m
	,fg3a AS m_fg3a
	,ftm AS m_ftm
	,fta AS m_fta
	,oreb AS m_oreb
	,dreb AS m_dreb
	,reb AS m_reb
	,ast AS m_ast
	,stl AS m_stl
	,blk AS m_blk
	,"TO" AS m_turnovers
	,pf AS m_pf
	,pts AS m_pts
	,plus_minus AS m_plus_minus
FROM cte_deduped 
WHERE row_num = 1

/* 
 * Now create our DDL for our fact table
 */


CREATE TABLE fct_game_details (
	dim_game_date date
	,dim_season integer
	,dim_team_id integer
	,dim_player_id integer
	,dim_player_name TEXT
	,dim_start_position TEXT
	,dim_is_playing_at_home boolean
	,dim_did_not_play boolean
	,dim_did_not_dress boolean
	,dim_not_with_team boolean
	,m_minutes REAL
	,m_fgm integer
	,m_fga integer
	,m_fg3m integer
	,m_fg3a integer
	,m_ftm integer
	,m_fta integer
	,m_oreb integer
	,m_dreb integer
	,m_reb integer
	,m_ast integer
	,m_stl integer
	,m_blk integer
	,m_turnovers integer
	,m_pf integer
	,m_pts integer
	,m_plus_minus integer
	,PRIMARY KEY (dim_game_date, dim_player_id, dim_team_id)
)

/*
 * Let's use our query to feed our fact table
 */

INSERT INTO fct_game_details
WITH cte_deduped AS (
	SELECT
		g.game_date_est
		,g.season
		,g.home_team_id
		,gd.*
		,row_number() OVER (PARTITION BY gd.game_id, team_id, player_id ORDER BY g.game_date_est) AS row_num
	FROM game_details gd 
	INNER JOIN games g ON gd.game_id = g.game_id
)
SELECT 	
	game_date_est AS dim_game_date
	,season AS dim_season
	,team_id AS dim_team_id
	,player_id AS dim_player_id
	,player_name AS dim_player_name
	,start_position AS dim_start_position
	,team_id = home_team_id AS dim_is_playing_at_home
	,COALESCE(POSITION('DNP'IN "comment"),0) > 0 AS dim_did_not_play
	,COALESCE(POSITION('DND'IN "comment"),0) > 0 AS dim_did_not_dress
	,COALESCE(POSITION('NWT'IN "comment"),0) > 0 AS dim_not_with_team
	,split_part("min",':',1)::REAL + split_part("min",':',2)::REAL/60 AS m_minutes
	,fgm AS m_fgm
	,fga AS m_fga
	,fg3m AS m_fg3m
	,fg3a AS m_fg3a
	,ftm AS m_ftm
	,fta AS m_fta
	,oreb AS m_oreb
	,dreb AS m_dreb
	,reb AS m_reb
	,ast AS m_ast
	,stl AS m_stl
	,blk AS m_blk
	,"TO" AS m_turnovers
	,pf AS m_pf
	,pts AS m_pts
	,plus_minus AS m_plus_minus
FROM cte_deduped 
WHERE row_num = 1


