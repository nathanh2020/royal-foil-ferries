SELECT rv.id::integer as route_id, mon, tue, wed, thu, fri, sat, sun, wr.start_airport, 
	wr.start_iata, wr.destination_airport, wr.destination_iata, wr.distance as distance_km,
	wr.airline_iata, wr.start_icao, wr.destination_icao, wr.airline_icao, 
	al.airline, sap.city start_city, sap.lat start_lat, sap.lon start_lon,
	dap.city dest_city, dap.lat dest_lat, dap.lon dest_lon
FROM route_volume rv -- this table came out of epynomous python script
INNER JOIN water_routes_filtered wr -- inner join with filtered water routes to add back flight data that was lost
ON rv.id::integer = wr.route_id
INNER JOIN airlines as al -- inner join with airlines to add back airline data
ON wr.airline_icao = al.airline_icao
INNER JOIN airports as sap -- add back airports data for source
ON wr.start_icao = sap.icao
INNER JOIN airports as dap -- add back destination data
ON wr.destination_icao = dap.icao
WHERE wr.airline_icao != 'NaN' -- remove rows with missing/empty values
AND (rv.mon + rv.tue + rv.wed + rv.thu + rv.fri + rv.sat + rv.sun) > 0 -- were volume is not null
AND al.active = 'Y' -- on an active airline
ORDER BY (mon, tue, wed, thu, fri, sat, sun) DESC -- order by total volume descending

