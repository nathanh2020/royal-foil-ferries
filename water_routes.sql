-- the airports by water table is all airports within 30 miles of the ocean.
-- this first query creates a table called water routes
-- the rlength table is all route information in addition to length
-- the water routes table is a result of an inner join of the airports by water table to each airport on the route
CREATE TABLE IF NOT EXISTS water_routes AS (SELECT 	start_airports.name AS start_airport, start_airports.iata AS start_iata, destination_airports.name AS destination_airport, 
							destination_airports.iata AS destination_iata, r.airline, r.distance, 
					  		ROW_NUMBER() OVER(ORDER BY start_airports.name) as route_id, 
					  		start_airports.icao AS start_icao, destination_airports.icao AS destination_icao
						FROM rlength r
						INNER JOIN (SELECT aw.name, aa.iata, aa.icao 
									FROM airports_by_water aw 
									LEFT JOIN airports aa 
									ON aw.name=aa.name) AS start_airports
						ON r.source_airport=start_airports.iata
						INNER JOIN (SELECT aw.name, aa.iata, aa.icao 
									FROM airports_by_water aw 
									LEFT JOIN airports aa 
									ON aw.name=aa.name) AS destination_airports
						ON r.destination_airport=destination_airports.iata
					 	WHERE distance < 200 AND distance > 50); --kms

-- self join to remove any return flight duplicates
-- add airline info
CREATE TABLE IF NOT EXISTS water_routes_filtered AS (
SELECT w1.start_airport, w1.start_iata, w1.destination_airport, w1.destination_iata, w1.distance,
		ROW_NUMBER() OVER(ORDER BY w1.start_airport ASC) AS route_id, w1.airline AS airline_iata,
		w1.start_icao, w1.destination_icao, airlines.airline_icao as airline_icao
FROM water_routes w1
LEFT JOIN water_routes w2
ON w1.start_iata = w2.destination_iata AND w1.destination_iata = w2.start_iata AND w1.row_number < w2.row_number
LEFT JOIN airlines
ON w1.airline = airlines.airline_iata
WHERE w2.start_iata IS NULL AND airlines.airline_iata IS NOT NULL AND airlines.airline_icao IS NOT NULL
ORDER BY w1.distance ASC
LIMIT 900) --restriction based on my api query limits
