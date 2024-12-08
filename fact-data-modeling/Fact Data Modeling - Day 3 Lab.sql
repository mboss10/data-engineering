/* 
 * Creating the reduced facts table
 */

CREATE TABLE array_metrics (
	user_id NUMERIC
	,month_start date 
	,metric_name TEXT 
	,metric_array REAL[]
	,PRIMARY KEY (user_id, month_start, metric_name)
)

/*
 * Query to fill the reduced facts table
 */

INSERT INTO array_metrics
WITH daily_aggregate AS (
	SELECT 
		user_id
		,date(event_time) AS date
		,count(1) AS num_site_hits
	FROM events
	WHERE date(event_time) = date('2023-01-04')
	AND user_id IS NOT NULL
	GROUP BY user_id, date(event_time)
),
yesterday_array AS (
	SELECT * FROM array_metrics
	WHERE month_start = date('2023-01-01')
)
SELECT 
	COALESCE(da.user_id, ya.user_id) AS user_id
	,COALESCE(ya.month_start, date_trunc('month', da.date)) AS month_start
	,'site_hits' AS metric_name
	,CASE WHEN ya.metric_array IS not NULL THEN 
		ya.metric_array || ARRAY[COALESCE(da.num_site_hits,0)]
	WHEN ya.metric_array IS NULL 
		THEN array_fill(0,ARRAY[COALESCE(date - date(date_trunc('month',date)), 0)]) || ARRAY[COALESCE(da.num_site_hits,0)]
	END AS metric_array
FROM daily_aggregate da
FULL OUTER JOIN yesterday_array ya ON da.user_id = ya.user_id
ON CONFLICT (user_id, month_start, metric_name)
DO 
	UPDATE SET metric_array = excluded.metric_array 

	
DELETE FROM array_metrics

SELECT * FROM array_metrics

SELECT CARDINALITY(metric_array), count(1)
FROM array_metrics
GROUP BY 1


/* 
 * Query to aggregate on array_metrics
 */

WITH agg AS (
SELECT 
	metric_name
	,month_start
	,ARRAY[sum(metric_array[1])
		,sum(metric_array[2])
		,sum(metric_array[3])
		,sum(metric_array[4])] AS summed_array
FROM array_metrics 
GROUP BY metric_name, month_start
)
SELECT 
	metric_name
	,month_start + CAST(CAST(INDEX - 1 AS TEXT) || 'day' AS INTERVAL) AS date
	,elem AS value
FROM agg
CROSS JOIN UNNEST(agg.summed_array)
WITH ORDINALITY AS a(elem,index)