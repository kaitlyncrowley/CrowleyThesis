import pandas as pd
#import geopandas as gpd
import os
import requests
#import pyrosm
#import osgeo
from pyrosm import OSM
from pyrosm import get_data


path = "https://download.geofabrik.de/index-v1.json"
r = requests.get(path)


parent_continents = ['africa', 'antartica', 'asia', 'central-america', 'europe', 'north-america', 'south-america', 'australia-oceania']

# Note: for the exceptions, guyana, guernsey-jersey, and american-oceania all need to be downloaded differently, us-virgin-islands
# The others are redundant files in geofabrik
exceptions = ['south-africa-and-lesotho', 'alps', 'britain-and-ireland', 'dach', 'us', 'guyana', 
                'guernsey-jersey', 'american-oceania', 'us-midwest', 'us-northeast',
                'us-pacific', 'us-south', 'us-west', 'guyana', 'united-kingdom', 'us/california', 'us/us-virgin-islands']
osm_keys_to_keep = "highway"


for i in r.json()['features']:
    properties = i['properties']
    urls = properties['urls']
    identifier = properties['id']
    
    if identifier == 'antarctica':#or identifier == 'australia-oceania':
        osm = OSM(get_data(identifier, directory = '/sciclone/geounder/gRoads2/sourceData3'))
    elif identifier == 'norcal': 
        osm = OSM(get_data('northern_california', directory = '/sciclone/geounder/gRoads2/sourceData3'))
    elif identifier == 'socal':
        osm = OSM(get_data('southern_california', directory = '/sciclone/geounder/gRoads2/sourceData3'))
    elif 'parent' in properties:
        #if properties['parent'] in parent_continents and identifier == 'australia-oceania':
         #   print('identifier australia-oceania: ', identifier)
         #   pbf_link = urls['pbf']
         #   #response = requests.get(urls['pbf'])
         #   if '/' in identifier:
         #       updated_id = identifier.replace('/', '-')
         #       osm = OSM(get_data(identifier, directory = '/sciclone/geounder/gRoads2/sourceData'))   
         #   else:
         #       osm = OSM(get_data(identifier, directory = '/sciclone/geounder/gRoads2/sourceData'))
        
        if properties['parent'] in parent_continents and identifier not in exceptions:
            parent = properties['parent']
            print('PARENT 1: ', parent)
            print('identifier: ', identifier)
            pbf_link = urls['pbf']
            #response = requests.get(urls['pbf'])
            if properties['parent'] == 'north-america' and 'iso3166-2' in properties and identifier != 'us/california':
                updated_id = identifier.replace('us/','')
                osm = OSM(get_data(updated_id, directory = '/sciclone/geounder/gRoads2/sourceData3'))
            #elif identifier == 'us/us-virgin-islands':
                #updated_id = identifier.replace('us/','')
                #osm = OSM(get_data(updated_id, directory = '/sciclone/geounder/gRoads2/sourceData2'))


            #elif properties['parent'] == 'north-america' and (identifier == 'socal' or identifier == 'norcal'):
                #fp = OSM(get_data(identifier, directory = '/sciclone/geounder/gRoads2/sourceData2'))

            elif '/' in identifier:
                updated_id = identifier.replace('/', '-')
                osm = OSM(get_data(updated_id, directory = '/sciclone/geounder/gRoads2/sourceData3'))
                #my_filter = osm.get_network(network_type='driving')
            
            else:
                fp = get_data(identifier, directory = '/sciclone/geounder/gRoads2/sourceData3')
                osm = OSM(fp)
                #my_filter = osm.get_network(network_type='driving')
                
                
# AUSTRALIA-OCEANIA aren't working