import os
import numpy as np
import pandas as pd
import geopandas as gpd
from datetime import datetime
import iso3166
#import osrm
import requests




from dask.distributed import Client
from dask_jobqueue import PBSCluster

#These are the arguments you would normally specify in your job file.
#Effectively, dask will be running a qsub *for* you, numerous times, as 
#per your specifications.
#Of note, cores and processes should be similar to what you request
#in your resource spec.  Here, I have one process per core (no hyperthreading),
#but in practice most of the cores on the HPC can have 2 processes each, 
#so setting processes to 24 would be a more effective allocation if you have
#enough memory.  Note we're requesting to use all the memory on each Vortex node (32gb).
cluster_kwargs = {
    "name": "exDaskCluster",
    "shebang": "#!/bin/tcsh",
    "resource_spec": "nodes=1:bora:ppn=20", #20 in bora, 12 vortex
    "walltime": "24:00:00",
    "cores": 20, #20 in bora, 12 vortex
    "processes": 1, #1 for bora?, 1 vortex
    "memory": "128GB", #for bora
    #"memory": "32GB",
    "interface": "ib0",
}

cluster = PBSCluster(**cluster_kwargs)
#Here we tell the program how far to scale our cluster, and this is where the magic comes in.
cluster.scale(8) # 4 for bora, vortex?
client = Client(cluster)


def tenClosest(allUrbanCentroids, degurbaPoint):
    index = degurbaPoint.index.start
    # Calculate distances for every urbanCentroid to all degurba points
    distances = allUrbanCentroids.apply(lambda row: degurbaPoint.geometry.distance(row.geometry), axis=1)

    # Find the 10 closest points
    closest_points_index = distances[index].nsmallest(10).index
    return(index, closest_points_index)


################# Need to figure out exactly what to return
### Want to concatenate to the previous results???
def closestOSRM(index, allUrbanCentroids, degurbaPoint, closest_points_index):
    travel_distances = []
    travel_times = []

    # Get the coordinates for the degurba point of interest to enter into osrm
    degurbX = degurbaPoint.geometry.x[index]
    degurbY = degurbaPoint.geometry.y[index]
    # Loop through the 10 closest points and get the coordinates for each of the urban centers to input into osrm
    for k in closest_points_index:
        urbanX = allUrbanCentroids.iloc[k].geometry.x
        urbanY = allUrbanCentroids.iloc[k].geometry.y
        url = 'http://router.project-osrm.org/route/v1/driving/' + str(degurbX) + ',' + str(degurbY) + ';' + str(urbanX) + ',' + str(urbanY) + '?overview=false&steps=false'
        r = requests.get(url)
        res = r.json()
        travel_distances.append(res['routes'][0]['legs'][0]['distance'])
        travel_times.append(res['routes'][0]['legs'][0]['duration'])
        
        # Find the minimum travel distance between the degurba point of interest and the 10 cities
        # For the travel time, I decided to not take the minimum travel time overall and just get the travel time associated with the shortest distance
        # Working under the assumption that, in most cases, the minimum travel distance corresponds to mimimum travel time
        degurbaPointsDist['ClosestUrbanDist'][index] = min(travel_distances)
        timeIndex = travel_distances.index(min(travel_distances))
        degurbaPointsDist['TravelTime'][index] = travel_times[timeIndex]

        # Another return value --> create a csv for the single row
        # Does this make us lose precision? --> decrease number of decimals in the coordinates
        coordinate = degurbaPoint.geometry.iloc[0].wkt
        csv_list = [coordinate, min(travel_distances), travel_times[timeIndex]]
        df = pd.DataFrame(csv_list).T
        df.columns = ['geometry', 'urbanDist', 'travelTime']
        df.to_csv('/sciclone/geounder/gRoads2/urbanDistanceCalculations/point' + str(index) + '.csv')

    return(csv_list, degurbaPointsDist)


def urbanDistances(allUrbanCentroids, degurbaPoint):
    index, cpi = tenClosest(allUrbanCentroids, degurbaPoint)
    csv_list, degurbaPointsDist = closestOSRM(index, allUrbanCentroids, degurbaPoint, cpi)
    return(csv_list, degurbaPointsDist)
    
# For testing, only Nepal degurba
# Would need to figure out how to even load in the global degurba point file
degurbaPoints = gpd.read_file('/Users/kaitlyncrowley/Desktop/Spring2024/HonorsThesis/urbanAreas/nepalDegurbaPoints.geojson')
urbanCentroids = gpd.read_file('/Users/kaitlyncrowley/Desktop/Spring2024/HonorsThesis/urbanAreas/urbanCentroids.geojson')

urbanCentroids = urbanCentroids.set_crs(crs="ESRI:54009", allow_override=True)
degurbaPoints = degurbaPoints.set_crs(crs="ESRI:54009", allow_override=True)

urbanCentroidsDist = urbanCentroids.to_crs(crs = 4326) #Plate 4326
degurbaPointsDist = degurbaPoints.to_crs(crs = 4326)

degurbaPointsDist['ClosestUrbanDist'] = 0
degurbaPointsDist['TravelTime'] = 0


    
# Need to edit this to include the two arguments (urbanCentroidsDist and degurbaPointsDist)
futures = client.map(urbanDistances, degurbaPointsDist)
result = [A.result() for A in futures]
print(result)
    