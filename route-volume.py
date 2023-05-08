import requests as r
import pandas as pd
import json
import os
import psycopg2

# upload selected routes
df = pd.read_csv('~/Desktop/selected_routes.csv')  # assumes there are headers

# connect to AirLabs api
api_key = 'api_key'

# define dictionary to fill
flight_data = {}

# query Airlabs api for each row in the selected routes data frame
# need to adjust code because I only get 250 queries per minute (use a count and break)
count = 0
for i in range(800, 885, 1):  # len(df) is 885, need to work in batches because of free api tier I am using
    route_id = str(df.loc[i, 'route_id'])
    dep_iata = str(df.loc[i, 'start_iata'])
    dep_icao = str(df.loc[i, 'start_icao'])
    arr_icao = str(df.loc[i, 'destination_icao'])
    arr_iata = str(df.loc[i, 'destination_iata'])
    airline_icao = str(df.loc[i, 'airline_icao'])
    airline_iata = str(df.loc[i, 'airline_iata'])

    url = 'https://airlabs.co/api/v9/routes?api_key='+api_key+'&dep_iata='+dep_iata+'&dep_icao='+dep_icao+'' \
          '&arr_icao='+arr_icao+'&arr_aita='+arr_iata+'&airline_icao='+airline_icao+'&airline_iata='+airline_iata

    request = r.get(url)
    json_query = request.json()

    all_response = json_query.get('response')

    # flight data is a dict containing lists of json response data for each route number
    flight_data[route_id] = all_response

    # break statement to chunk the query into approved bits
    count += 1
    if count == 213:
        break

# write json to hardrive location as backup because of minimal query attempts
file_path = 'flight_data4.json'
if os.path.exists(file_path):
    os.remove(file_path)
out_file = open(file_path, 'w')
json.dump(flight_data, out_file, indent=6)
out_file.close()

# count total flights for each day of the week along each route
route_counts = {}
for x in flight_data:
    route_counts[x] = {day: 0 for day in ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']}
    day_counts = route_counts[x]
    for y in route_counts[x]:
        filtered_data = [item for item in flight_data[x] if y in item['days']]
        day_counts[y] = len(filtered_data)

# connect to database
conn = psycopg2.connect(
    host="localhost",
    database="sailing",
    user="user",
    password="password"
)
cursor = conn.cursor()

# create table for data
cursor.execute('CREATE TABLE IF NOT EXISTS route_volume '
               '(id text, '
               'mon numeric, '
               'tue numeric, '
               'wed numeric, '
               'thu numeric, '
               'fri numeric, '
               'sat numeric, '
               'sun numeric)')

# insert data into table
for item in route_counts:
    insert_statement = 'INSERT INTO route_volume (id, mon, tue, wed, thu, fri, sat, sun) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
    values = [item] + [route_counts[item][day] for day in ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']]
    cursor.execute(insert_statement, values)

conn.commit()
conn.close()
