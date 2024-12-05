-- events table contains a log of actions by users taken against pages of a website
SELECT 
min(event_time), max(event_time)
FROM events e 

/* 
 * Create a table 
 */
DROP TABLE users_cumulated

CREATE TABLE users_cumulated (
	user_id TEXT 
-- The list of dates in the past where the user was active
	,dates_active date[]
-- The current date for the user
	,date date
	,PRIMARY KEY (user_id, date)
)


/* 
 * Building the cumulative query to fill the table
 */

INSERT INTO users_cumulated 
WITH yesterday AS (
	SELECT *
	FROM users_cumulated
	WHERE date = date('2023-01-30')
),
today AS (
	SELECT 
	CAST(user_id AS TEXT) AS user_id
	,date(CAST(event_time AS timestamp)) AS date_active
	FROM events e 
	WHERE date(CAST(event_time AS timestamp)) = date('2023-01-31')
	AND user_id IS NOT NULL 
	GROUP BY user_id
	,date(CAST(event_time AS timestamp)) 
)
SELECT 
	COALESCE(t.user_id, y.user_id) AS user_id
	,CASE WHEN y.dates_active IS NULL
		THEN array[t.date_active]
		WHEN t.date_active IS NULL THEN y.dates_active 
		ELSE ARRAY[t.date_active] || y.dates_active
	END AS dates_active
	,COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
FROM today t 
FULL OUTER JOIN yesterday y ON t.user_id = y.user_id

-- query to review our data
SELECT * FROM users_cumulated 
WHERE date = '2023-01-31'


/* 
 * Generate a date list for 30 days
 */
SELECT *
FROM generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') 


WITH users AS (
	SELECT * 
	FROM users_cumulated 
	WHERE date = '2023-01-31'
),
series AS (
	SELECT *
	FROM generate_series('2023-01-01', '2023-01-31', INTERVAL '1 day') AS series_date
),
placeholder_ints AS (
SELECT 
	CASE WHEN 
		dates_active @> ARRAY [date(series_date)]
	THEN pow(2, 32 - (date - date(series_date)))
	ELSE 0
	END AS placeholder_int_value
	,*
FROM users CROSS JOIN series 
--WHERE user_id = '439578290726747300'
)
SELECT 
	user_id
	,CAST(CAST(sum(placeholder_int_value) AS bigint) AS bit(32))
	,bit_count(CAST(CAST(sum(placeholder_int_value) AS bigint) AS bit(32))) > 0 AS dim_is_monthly_active
	,bit_count(cast('11111110000000000000000000000000' AS BIT(32)) & CAST(CAST(sum(placeholder_int_value) AS bigint) AS bit(32)))>0 AS dim_is_weekly_active
	,bit_count(cast('10000000000000000000000000000000' AS BIT(32)) & CAST(CAST(sum(placeholder_int_value) AS bigint) AS bit(32)))>0 AS dim_is_daily_active
FROM placeholder_ints
GROUP BY user_id