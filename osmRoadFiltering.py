import os
import numpy as np
import geopandas as gpd
from datetime import datetime



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
#os.system("conda deactivate; ogr2ogr")

pbfFolder = '/sciclone/geounder/gRoads2/sourceData3/'

# assign directory
#def pbfTogeoJson(pbfPath, filename):
#    command = "conda activate /sciclone/home/kmcrowley/.conda/envs/roadsFiltering; ogr2ogr -f GeoJSON /Users/kaitlyncrowley/Desktop/Fall2023/HonorsThesis/geojsons/" + filename[:-15]+ ".geojson" + pbfPath + " lines"
#    print(command)
#    os.system(command)

def pLogger(id, type, message):
    path = "/sciclone/geounder/gRoads2/logs3/"
    with open(path + str(id) + ".log", "a") as f:
        f.write(str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + ": (" + str(type) + ") " + str(message) + "\n")


def pbfTogeoJsonDASK(input):
    if os.path.isfile(input[1]):
        os.remove(input[1])
    command = 'source "/usr/local/anaconda3-2021.05/etc/profile.d/conda.csh"; module load anaconda3/2021.05; conda activate /sciclone/home/kmcrowley/.conda/envs/roadsFiltering; ogr2ogr -f GeoJSON ' + input[1] + " " + input[0] + " lines"
    print(command)
    os.system(command)
        

def filtergeoJson(input):
    try:
        pLogger(input[2], "INFO", input)
        jsonOSM = gpd.read_file(input[1])
        pLogger(input[2], "INFO", jsonOSM.head())
        roadsSubset = ['motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'residential', 'motorway_link', 'trunk_link',
                    'primary_link', 'secondary_link', 'tertiary_link', 'living_street', 'track']

        roadways = jsonOSM.loc[jsonOSM['highway'].isin(roadsSubset)]
        pLogger(input[2], "INFO", roadways.head())
        inputSplit = input[1].split('/')
        pLogger(input[2], "INFO", inputSplit[-1][:-8])
        roadways.to_file('/sciclone/geounder/gRoads2/filteredGeoJsons4/' + inputSplit[-1][:-8] + 'DrivingRoads.geojson')
    except Exception as e:
        pLogger(input[2], "ERROR", str(e))

def combinedConvertFilter(input):
    pLogger(input[2], "INFO", input)
    try:
        pLogger(input[2], "INFO", "CONVERSION STARTING")
        pbfTogeoJsonDASK(input)
        pLogger(input[2], "INFO", "FILTER STARTING")
        filtergeoJson(input)
        
        return("Success, probably")
    except Exception as e: 
        pLogger(input[2], "INFO", e)
        return("Failure: " + str(e))


# iterate over files in
# that directory
listOfPbfs=[]

for filename in os.listdir(pbfFolder):
    sub_list = []
    pbfPath = os.path.join(pbfFolder, filename)

    # checking if it is a file
    if os.path.isfile(pbfPath) and not filename.startswith('.'):
        sub_list.append(pbfPath)
        destination = '/sciclone/geounder/gRoads2/convertedGeoJsons4/' + filename[:-15] + '.geojson'
        sub_list.append(destination)
        sub_list.append(filename[:-15])
        listOfPbfs.append(sub_list)

print(listOfPbfs)
#A = client.map(pbfTogeoJsonDASK, listOfPbfs)
#output = client.submit(A)

#listOfPbfs = [["/sciclone/geounder/gRoads2/sourceData2/hungary-latest.osm.pbf","/sciclone/geounder/gRoads2/sourceData2/hungary.geojson","HUNGARY"]]

futures = client.map(combinedConvertFilter, listOfPbfs)
result = [A.result() for A in futures]
print(result)

#Note that because of how our torque system interacts with dask, you will always get a "cancelled" error
#(sometimes "futures" or both) at the end of a dask script.  These lines are just so you can see your outputs
#above the error.
print("======================")
print("======================")
print("END OF OUTPUT")
print("======================")
print("======================")







########################################################################################################################
#for filename in os.listdir(pbfFolder):

    #print(filename)
    #pbfPath = os.path.join(pbfFolder, filename)
    #print(pbfPath)

    # checking if it is a file
    #if os.path.isfile(pbfPath) and not filename.startswith('.'):

        #pbfTogeoJson(pbfPath, filename)

    #    jsonOSM = gpd.read_file("/Users/kaitlyncrowley/Desktop/Fall2023/HonorsThesis/geojsons/" + filename[:-15] + ".json")
    #    roadsSubset = ['motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'residential', 'motorway_link', 'trunk_link',
    #            'primary_link', 'secondary_link', 'tertiary_link', 'living_street', 'track']

    #    roadways = jsonOSM.loc[jsonOSM['highway'].isin(roadsSubset)]
    #    roadways.to_file('/Users/kaitlyncrowley/Desktop/Fall2023/HonorsThesis/geojsons/' + filename[:-15] + 'DrivingRoads.geojson')