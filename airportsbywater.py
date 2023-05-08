import pandas as pd
from sqlalchemy import create_engine
import geopandas as gpd
from shapely.geometry import Point

# Load the shapefile of the ocean polygons
ocean = gpd.read_file('/Users/__/__/__/ne_50m_ocean.shp')

# Set the CRS to EPSG:4326 if it's not already in this format
ocean = ocean.to_crs('EPSG:4326')

# create an engine object to connect to the database
engine = create_engine('postgresql://user:password@localhost:port/database')

# SQL query to retrieve airport data
query = 'SELECT name, lat, lon FROM airports'

# create GeoDataFrame from SQL query
df = pd.read_sql(query, engine)
airport_gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat), crs='EPSG:4326')

import math

# need to calculate the length of a degree of latitude at a given latitude
def degrees_to_meters(lat):
    R = 6373.0
    return (2 * R * 1000 * math.pi) / 360.0 * math.cos(math.radians(lat))

# create new column to store the length
airport_gdf['lat_degree_length'] = airport_gdf['lat'].apply(degrees_to_meters)

# create a 30 mile buffer around each airport
buffered_airports = airport_gdf.apply(lambda row: Point(row['geometry']).buffer(30 * 1609.34 / row['lat_degree_length']), axis=1)

#create new data frame with buffered airports
buffered_airports_gdf = gpd.GeoDataFrame({'name': airport_gdf['name'], 'geometry': buffered_airports}, crs=airport_gdf.crs)

# perform a spatial join between the airport buffers and the ocean polygons
intersections = gpd.sjoin(buffered_airports_gdf, ocean, predicate='intersects')

# export to sql 
intersections['name'].to_sql('airports_by_water', engine, if_exists='replace', index=False)
print(intersections.head(5))
print(intersections.columns)
