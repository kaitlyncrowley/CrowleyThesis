import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, box
import rasterio


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



######## Note ##############
# This code actually runs pretty fast (at least for Nepal by itself), so I don't know if it needs to be parallelized
# Many of the cells are 0s (no roads) but that probably isn't as much the case in other countries

###### Also need to finalize the best projection, but that should be easy to change
# Right now, the distance that the function gives is in meters (based on Mollweide), but I believe Mollweide is equal area, not equal distance 
## However, I don't know if that matters as much since this task involves such small grid cells

# Can I make the degurba point file into parquet?
degurbaPoints = gpd.read_file('/Users/kaitlyncrowley/Desktop/Spring2024/HonorsThesis/urbanAreas/nepalDegurbaPoints.geojson')
degurbaPoints = degurbaPoints.set_crs(crs="ESRI:54009", allow_override=True)
roads = gpd.read_parquet('/Users/kaitlyncrowley/Desktop/Spring2024/HonorsThesis/cellDistance/nepalDrivingRoads.parquet')
roads = roads.to_crs(crs="ESRI:54009")

# Determine the lat long
# Not quite sure the best place to extract the coordinates
coordinates = degurbaPoints.get_coordinates()

# Can easily edit to make the inputs x and y, 
def buildGridCell(coordinatePair):
    x = coordinatePair.x
    y = coordinatePair.y
    
    bbox_size = 1000 # 1km = 1000m
    
    # Calculate the coordinates of the bounding box
    minx = x - bbox_size / 2
    miny = y - bbox_size / 2
    maxx = x + bbox_size / 2
    maxy = y + bbox_size / 2

    minx = minx.iloc[0] if hasattr(minx, 'iloc') else minx
    miny = miny.iloc[0] if hasattr(miny, 'iloc') else miny
    maxx = maxx.iloc[0] if hasattr(maxx, 'iloc') else maxx
    maxy = maxy.iloc[0] if hasattr(maxy, 'iloc') else maxy

    bounding_box = box(minx, miny, maxx, maxy)

    return(bounding_box)

def calcRoadLen(roads, bounding_box):
    clippedRoads = gpd.clip(roads, bounding_box)
    total_len = sum(clippedRoads['geometry'].length)
    return(total_len)


# Could easily combine the two functions
def cellRoadLenCalc(coordinatePair, roads):
    boundingBox = buildGridCell(coordinatePair)
    totalLength = calcRoadLen(roads, boundingBox)
    return(totalLength)


    
# May need to edit this to take the two inputs into account
# Also not sure the best way to save the lengths to make the raster creation easiest
futures = client.map(cellRoadLenCalc, coordinates, roads)
result = [A.result() for A in futures]
print(result)
    
