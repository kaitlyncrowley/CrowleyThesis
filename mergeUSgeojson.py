import pandas as pd
import geopandas as gpd
import os
import requests
from datetime import datetime


state_list = ['alabama', 'alaska', 'arizona', 'arkansas', 'norcal', 'socal', 'colorado', 'connecticut', 'delaware', 'district-of-colombia', 'florida', 
            'hawaii', 'idaho', 'illinois', 'indiana', 'iowa', 'kansas', 'kentucky', 'louisiana', 'maine', 'maryland', 'massachusetts', 'michigan', 'minnesota', 
            'mississippi', 'missouri', 'montana', 'nebraska', 'nevada', 'new-hampshire', 'new-jersey', 'new-mexico', 'new-york', 'north-carolina', 'north-dakota',
            'ohio', 'oklahoma', 'oregon', 'pennsylvania', 'puerto-rico', 'rhode-island', 'south-carolina', 'south-dakota', 'tennessee', 'texas', 'utah', 'vermont'
            'virginia', 'washington', 'west-virginia', 'wisconsin', 'wyoming']

drivingGeojsonFolder = '/sciclone/geounder/gRoads2/filteredGeoJsons4'
frames = []

for filename in os.listdir(drivingGeojsonFolder):
    state_name = filename[:-20]
    if state_name in state_list:
        filepath = os.path.join(drivingGeojsonFolder, filename)  # Corrected path construction
        frames.append(gpd.read_file(filepath))

# Check if the list is not empty before trying to create a GeoDataFrame
if frames:
    # Create a GeoDataFrame by concatenating multiple GeoDataFrames
    united_states = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True))

    # Write the GeoDataFrame to a GeoJSON file
    united_states.to_file('/sciclone/geounder/gRoads2/filteredGeoJsons4/united-states.geojson', driver='GeoJSON')
else:
    print("No files found for the specified states.")