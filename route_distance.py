import pandas as pd
from sqlalchemy import create_engine

# create an engine object to connect to the database
engine = create_engine('postgresql://user:password@localhost:port/database')

# join routes table and airports table to get source coordinates and destination coordinates
df = pd.read_sql_query('SELECT r.airline, r.source_airport, r.source_id, r.destination_airport, r.destination_id, '
                       'r.stops, a1.lat AS source_lat, a1.lon AS source_lon, a2.lat AS dest_lat, a2.lon AS dest_lon, '
                       '(radians(a2.lon) - radians(a1.lon)) * cos((radians(a1.lat) + radians(a2.lat))/2) AS x, '
                       '(radians(a2.lat) - radians(a1.lat)) AS y '
                       'FROM routes r '
                       'LEFT JOIN airports a1 ON r.source_id = a1.id '
                       'LEFT JOIN airports a2 ON r.destination_id = a2.id '
                       'WHERE a1.lat IS NOT NULL AND a2.lat IS NOT NULL', con=engine)

# write to sql database as rcoord
df.to_sql('rcoord', engine, if_exists='replace', index=False)

# calculate the length of the route using an equirectangular approximation
df = pd.read_sql_query('SELECT *, 6371 * sqrt(POWER(x,2) + POWER(y,2)) distance '
                       'FROM rcoord', engine)

# write to sql database as record
df.to_sql('rlength', engine, if_exists='replace', index=False)

# commit and close
engine.connect().commit()
engine.dispose()
