SELECT * FROM pteta_v2.vehicle

SELECT * FROM pteta_v2.route

SELECT * FROM pteta_v2.OWNER

SELECT date_trunc('day', gd.gpstime), COUNT(*) 
	FROM pteta_v2.gpsdata as gd
	GROUP BY date_trunc('day', gd.gpstime)
LIMIT 100


SELECT *
	FROM pteta_v2.gpsdata as gd
	ORDER BY gd.gpstime DESC
LIMIT 100


SELECT *
	FROM pteta_v2.gpsdata as gd
	ORDER BY gd.gpstime DESC
LIMIT 100


SELECT *
	FROM pteta_v2.gpsdata as gd
	WHERE gd.gpstime > '2023-02-03 00:00:00'
	ORDER BY gd.edited DESC
LIMIT 150


SELECT date_trunc('MINUTE', gd.gpstime), count(*)
	FROM pteta_v2.gpsdata as gd
	WHERE gd.gpstime > '2023-01-13 10:00:00'
	GROUP BY date_trunc('MINUTE', gd.gpstime)
LIMIT 250

SELECT date_trunc('MINUTE', gd.edited), count(*)
	FROM pteta_v2.gpsdata as gd
	WHERE gd.edited > '2023-02-03 04:00:00'
	GROUP BY date_trunc('MINUTE', gd.edited)
LIMIT 250


SELECT date_trunc('milliseconds', gd.edited), count(*)
	FROM pteta_v2.gpsdata as gd
	WHERE gd.edited > '2023-02-05 16:25:00'
	GROUP BY date_trunc('milliseconds', gd.edited)
LIMIT 250


date_trunc('day', gd.gpstime)



