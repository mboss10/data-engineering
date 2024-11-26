select 
	*
from 	
	player_seasons ps 

/* creating a type (array) for data that will change over the season */	
create type season_stats as (
	season integer
	, go integer
	, pts real
	, reb real
	, ast real
)

/* I made a typo on gp (games played) attribute so I will alter my type to fix this */
ALTER TYPE season_stats RENAME ATTRIBUTE go TO gp 

/* Creating a new players table */
create table players (
	player_name text
	,height text
	,college text
	,country text
	,draft_year text
	,draft_round text
	,draft_number text
	,season_stats season_stats[]
	,current_season integer
	,primary key(player_name,current_season)
)

/* Figuring out what is our first season in the data --> 1996 */
select 
	min(season)
from
	player_seasons ps 
	
/* building a "pipeline" to fill the players table */
insert into players 
with yesterday as (
	select 
		*
	from 
		players p 
	where 	
		p.current_season = 2001
),
today as (
	select 
		*
	from 
		player_seasons ps 
	where 	
		ps.season = 2002
)
select 
	coalesce(t.player_name, y.player_name) as player_name
	,coalesce(t.height, y.height) as height
	,coalesce(t.college, y.college) as college
	,coalesce(t.country, y.country) as country
	,coalesce(t.draft_year, y.draft_year) as draft_year
	,coalesce(t.draft_round , y.draft_round ) as draft_round
	,coalesce(t.draft_number, y.draft_number) as draft_number
	,case when y.season_stats is null 
		then 
			array[row(
				t.season
				,t.gp
				,t.pts
				,t.reb
				,t.ast
			)::season_stats]
	when t.season is not null 
		then
			y.season_stats || array[row(
				t.season
				,t.gp
				,t.pts
				,t.reb
				,t.ast
			)::season_stats]
	else 	
		y.season_stats
	end as season_stats
	,coalesce(t.season,y.current_season+1) as current_season
from 
	today t 
full outer join 
	yesterday y on t.player_name = y.player_name
	

select 
	* 
from 
	players p 
where 
	p.current_season = 2002
	and player_name = 'Michael Jordan'
	

select 
	player_name
	,season_stats.*
from
	players p 
	,unnest(season_stats) as season_stats
where 
	p.current_season = 2001

	
drop table players 

create type scoring_class as enum('star','good','average','bad')

/* Creating a new players table with 2 nnew columns  */
create table players (
	player_name text
	,height text
	,college text
	,country text
	,draft_year text
	,draft_round text
	,draft_number text
	,season_stats season_stats[]
	,scoring_class scoring_class
	,years_since_last_season integer
	,current_season integer
	,primary key(player_name,current_season)
)


/* building a "pipeline" to fill the new players table */
insert into players 
with yesterday as (
	select 
		*
	from 
		players p 
	where 	
		p.current_season = 1998
),
today as (
	select 
		*
	from 
		player_seasons ps 
	where 	
		ps.season = 1999
)
select 
	coalesce(t.player_name, y.player_name) as player_name
	,coalesce(t.height, y.height) as height
	,coalesce(t.college, y.college) as college
	,coalesce(t.country, y.country) as country
	,coalesce(t.draft_year, y.draft_year) as draft_year
	,coalesce(t.draft_round , y.draft_round ) as draft_round
	,coalesce(t.draft_number, y.draft_number) as draft_number
	,case when y.season_stats is null 
		then 
			array[row(
				t.season
				,t.gp
				,t.pts
				,t.reb
				,t.ast
			)::season_stats]
	when t.season is not null 
		then
			y.season_stats || array[row(
				t.season
				,t.gp
				,t.pts
				,t.reb
				,t.ast
			)::season_stats]
	else 	
		y.season_stats
	end as season_stats
	,case when t.season is not null
		then
			case when t.pts > 25 then 'star'
			when t.pts > 18 then 'good'
			when t.pts > 10 then 'average'
			else 'bad'
			end::scoring_class
		else
			y.scoring_class
	end as scoring_class
	,case when t.season is not null
		then 0
	else
		coalesce(y.years_since_last_season, 0) + 1
	end as years_since_last_season
	,coalesce(t.season,y.current_season+1) as current_season
from 
	today t 
full outer join 
	yesterday y on t.player_name = y.player_name

/**** end of pipeline ****/
	
select * from players where current_season = 1999
and player_name = 'Michael Jordan'

/* Which player has the biggest improvement from first season to latest season? */

select 	
	player_name
	,season_stats[1].pts as first_season
	,season_stats[cardinality(season_stats)].pts as latest_season
	,season_stats[cardinality(season_stats)].pts / (case when season_stats[1].pts = 0 then 1 else season_stats[1].pts end) as improvement
from players 
where current_season = 1999
and scoring_class in ('star','good')
order by improvement desc 
	