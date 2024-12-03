CREATE TYPE vertex_type
AS ENUM ('player','team','game')

CREATE TABLE vertices (
	identifier TEXT,
	type vertex_type,
	properties JSON,
	PRIMARY KEY (identifier, type)
)

CREATE TYPE edge_type AS
	ENUM ('play_against',
			'shares_team',
			'plays_in',
			'plays_on'
	)
	
CREATE TABLE edges (
	subject_identifier TEXT,
	subject_type vertex_type,
	object_identifier TEXT,
	object_type vertex_type,
	edge_type edge_type,
	properties JSON,
	PRIMARY KEY (subject_identifier,
				subject_type,
				object_identifier,
				object_type,
				edge_type)
)

INSERT INTO vertices
SELECT
	game_id AS identifier
	,'game'::vertex_type AS "type"
	,json_build_object(
		'pts_home',pts_home
		,'pts_away',pts_away 
		,'winning_team',CASE WHEN home_team_wins =1 THEN home_team_id ELSE visitor_team_id END 		
	) AS properties
FROM games



/*
 * Determining player_id with more than one player_name
 */
WITH cte AS (
SELECT player_id
FROM game_details gd 
GROUP BY player_id 
HAVING count(DISTINCT player_name)>1	
)
SELECT
	DISTINCT gd.player_id 
	,gd.player_name 
FROM game_details gd 
INNER JOIN cte ON cte.player_id = gd.player_id 
ORDER BY player_id

/*
 * since there are player_id with more than one player name, we'll use max(player_name) in our query
 */
INSERT INTO vertices 
WITH players_agg AS (
	SELECT 
		player_id 
		,max(player_name) AS player_name
		,count(1) AS number_of_games
		,sum(pts) AS total_points
		,array_agg(DISTINCT team_id) AS teams 
	FROM game_details gd 
	GROUP BY player_id
)
SELECT 
	player_id AS identifier
	,'player'::vertex_type AS "type"
	,json_build_object(
		'player_name',player_name
		,'number_of_games',number_of_games
		,'total_points',total_points
		,'teams',teams
	)
FROM players_agg


/*
 * insert teams into vertices table
 */
INSERT INTO vertices 
WITH cte_deduped AS (
	SELECT 
		*
		,ROW_NUMBER() OVER (PARTITION BY team_id) AS row_num
	FROM teams t2 
)
SELECT 
	team_id AS identifier
	,'team'::vertex_type AS "type"
	,jsonb_build_object(
		'abbreviation',abbreviation 
		,'nickname', nickname 
		,'city', city 
		,'arena', arena 
		,'year_founded' ,yearfounded 
	) 
FROM cte_deduped
WHERE row_num = 1

/* 
 * Let's look at the vertices table 
 */

SELECT 
	"type"
	,count(*)
FROM vertices 
GROUP BY "type"

/* 
 * Let's build the plays_in edge_type
 */

INSERT INTO edges
WITH deduped AS (
	SELECT 
		*
		,row_number() OVER (PARTITION BY player_id, game_id) AS row_num
	FROM game_details gd 
)
SELECT 
	player_id AS subject_identifier
	,'player'::vertex_type AS subject_type
	,game_id AS object_identifier 
	,'game'::vertex_type AS object_type
	,'plays_in'::edge_type AS edge_type
	,jsonb_build_object(
		'start_position' ,start_position 
		,'pts' ,pts 
		,'team_id' ,team_id 
		,'team_abbreviation' ,team_abbreviation 
	) AS properties 
FROM deduped
WHERE row_num = 1

/*
 * Select query on vertices and edges
 */

SELECT 	
	v.properties->>'player_name'
	,MAX(CAST(e.properties->>'pts' AS INTEGER)) 
FROM vertices v
INNER JOIN edges e ON v.identifier = e.subject_identifier AND v."type" = e.subject_type
GROUP BY 1
ORDER BY 2 DESC 

/* 
 * 
 */ 

INSERT INTO edges 
WITH deduped AS (
	SELECT 
		*
		,row_number() OVER (PARTITION BY player_id, game_id) AS row_num
	FROM game_details gd 
),
filtered AS (
	SELECT * FROM deduped
	WHERE row_num = 1
),
aggregated AS (
SELECT 
	f1.player_id AS subject_player_id
	,MAX(f1.player_name) AS subject_player_name
	,f2.player_id AS object_player_id
	,MAX(f2.player_name) AS object_player_name
	,CASE WHEN f1.team_abbreviation = f2.team_abbreviation 
		THEN 'shares_team'::edge_type
	ELSE 'play_against'::edge_type
	END AS edge_type
	,count(1) AS num_games
	,sum(f1.pts) AS subject_points
	,sum(f2.pts) AS object_points
FROM 
	filtered f1 
INNER JOIN filtered f2 ON f1.game_id = f2.game_id AND f1.player_id <> f2.player_id
WHERE f1.player_id > f2.player_id
GROUP BY f1.player_id
	,f2.player_id
	,CASE WHEN f1.team_abbreviation = f2.team_abbreviation 
		THEN 'shares_team'::edge_type
	ELSE 'play_against'::edge_type
	END
)
SELECT 
	subject_player_id AS subject_identifier
	,'player'::vertex_type AS subject_type
	,object_player_id AS object_identifier
	,'player'::vertex_type AS object_type
	,edge_type AS edge_type
	,jsonb_build_object(
		'num_games', num_games
		,'subject_name',subject_player_name
		,'object_name', object_player_name
		,'subject_points', subject_points
		,'object_points', object_points
	) 
FROM aggregated


