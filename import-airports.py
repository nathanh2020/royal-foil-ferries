import pandas as pd
import psycopg2

# connect to database
conn = psycopg2.connect(
    host="localhost",
    database="sailing",
    user="postgres",
    password="crusher2020"
)
cursor = conn.cursor()

# create postgresql database airports
cursor.execute('DROP TABLE IF EXISTS airports')

cursor.execute('CREATE TABLE IF NOT EXISTS airports '
               '(id numeric, '
               'name TEXT, '
               'city TEXT,'
               'country TEXT, '
               'iata TEXT, '
               'icao TEXT, '
               'lat NUMERIC, '
               'lon NUMERIC '
               ')')

# import airport database to pandas dataframe
dfa = pd.read_csv('/Users/nathanhousberg/Desktop/airports.dat.txt', header=None)

# insert the data into the postgresql database
for row in dfa.itertuples():
    cursor.execute('INSERT INTO airports ( id, name, city, country, iata, icao, lat, lon) '
                   'VALUES ( %s, %s, %s, %s, %s, %s, %s, %s)',
                   (row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8]))

# create postgresql database routes
cursor.execute('DROP TABLE IF EXISTS routes')

cursor.execute('CREATE TABLE IF NOT EXISTS routes '
               '(airline TEXT, '
               'airline_id NUMERIC, '
               'source_airport TEXT, '
               'source_id numeric, '
               'destination_airport TEXT, '
               'destination_id numeric, '
               'stops numeric '
               ')')

# import routes database to pandas dataframe
dfr = pd.read_csv('/Users/nathanhousberg/Desktop/routes.dat.txt', header=None, na_values='\\N')

# insert the data into postgresql database
for row in dfr.itertuples():
    cursor.execute('INSERT INTO routes (airline, airline_id, source_airport, source_id, '
                   'destination_airport, destination_id, stops) '
                   'VALUES (%s, %s, %s, %s, %s, %s, %s)',
                   (row[1], row[2], row[3], row[4], row[5], row[6], row[8]))


#import airlines
cursor.execute('DROP TABLE IF EXISTS airlines')

cursor.execute('CREATE TABLE IF NOT EXISTS airlines '
               '(id numeric, '
               'airline text, '
               'airline_iata text,'
               'airline_icao text, '
               'active text '
               ')')

# import airlines data to pandas dataframe
dfairline = pd.read_csv('/Users/nathanhousberg/Desktop/airlines.dat.txt', header=None, na_values='\\N')

# insert the data into postgresql database
for row in dfairline.itertuples():
    cursor.execute('INSERT INTO airlines (id, airline, airline_iata, airline_icao, active) '
                   'VALUES ( %s, %s, %s, %s, %s)',
                   (row[1], row[2], row[4], row[5], row[8]))

# commit the changes to the PostgreSQL database and close the connection
conn.commit()
conn.close()
