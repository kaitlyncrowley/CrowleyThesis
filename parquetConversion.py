import os
import geopandas as gpd
from datetime import datetime

def convert_geojson_to_parquet(directory, parquetDirectory):
    
    for filename in os.listdir(directory):
   
        if filename.endswith('.geojson'):
            file_path = os.path.join(directory, filename)
            parquet_path = os.path.join(parquetDirectory, filename.replace('.geojson', '.parquet'))

            # Read the GeoJSON file
            print(file_path)
            gdf = gpd.read_file(file_path)

            # Convert to Parquet
            gdf.to_parquet(parquet_path)
            print(f"Converted {file_path} to {parquet_path}")

# Define the directory containing your .geojson files
directory = '/sciclone/geounder/gRoads2/filteredGeoJsons4'
parquetDirectory = '/sciclone/geounder/gRoads2/parquetRoads'

# edited the function to put the parquet files in a different folder
convert_geojson_to_parquet(directory, parquetDirectory)