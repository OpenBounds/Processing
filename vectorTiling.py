
import json
import os
from urlparse import urlparse
import zipfile 

import click

import adapters
from filters import BasicFilterer
import utils
import subprocess

# To make it a command line script 
#@click.command()
#@click.argument('source', type=click.Path(exists=True), required=True)

def get_data(url, name): 
    
    # Creation of a new data folder 
    subprocess.call('mkdir ' + name +'/',shell=True)
    # Download the data 
    string = name + '/' + name + '.zip'
    command = 'curl -o ' + string + ' "' + url + '"' 
    subprocess.call(command,shell=True) 
    #Unzip the data 
    command = 'unzip ' + string + ' -d ' + string.split('/')[0]
    subprocess.call(command,shell=True) 
    #Delete the archive 
    subprocess.call('rm ' + string,shell=True)

def translate_to_geojson(shp, folder): 
    """ Function that create a geojson file 
    from a shp file and reproject to WSG84 using ogr2ogr tool 
    """
    projection = '-t_srs EPSG:4326'
    src = folder + '/' + shp + '.shp'
    dst = folder + '/' + shp.split('.')[0] + '.geojson'
    fmt = '-f GeoJSON'
    command = 'ogr2ogr ' + '-progress ' + '-overwrite ' + fmt + ' ' + projection + ' ' +  dst + ' ' + src 
    subprocess.call(command,shell=True)

def tiling(file): 
    """ Use of Tippecanoe 
    """
    command = 'tippecanoe -o ' + file + '.mbtiles ' + file + '.geojson'
    subprocess.call(command,shell=True)

def cleaning(folder): 
    """ Clean the folder except the .mbtiles 
    """
    files = [each for each in os.listdir(folder) if not each.endswith('.mbtiles')] 
    for file in files: 
        subprocess.call('rm ' + folder + '/' + file, shell=True)

def vectorTiling(source):
    """ Function that creates vector tiles
    SOURCE: either a .json file containing a "url" field that contains the geojson files 
            TODO -> a directory containing more than one of this .json file
    """
    with open(source, 'rb') as file: 
        content = json.load(file)
    url = content['url']
    folder = url.split('/')[-1].split('.')[0]

    get_data(url, folder)
    shapefile = [each for each in os.listdir(folder) if each.endswith('.shp')]
    name = shapefile[0].split('.')[0]

    translate_to_geojson(name, folder)

    tiling(folder + '/' + name)
    
    cleaning(folder)


if __name__ == '__main__':

    vectorTiling('/Users/athissen/Documents/GaiaGps/PublicLands/sources/federal/az-blm.json')
    