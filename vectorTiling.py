
import json
import os
from urlparse import urlparse
import zipfile 

import click

import adapters
from filters import BasicFilterer
import utils
import subprocess

def get_data(urls, folder): 
    # Create folder 
    command = 'mkdir ' + folder 
    subprocess.call(command,shell=True) 
    
    for url in urls: 
        name = url.split('/')[-1]
        string = folder + name
        # Download the data 
        command = 'curl -o ' + string + ' "' + url + '"' 
        subprocess.call(command,shell=True) 
        #Unzip the data 
        command = 'unzip ' + string + ' -d ' + folder 
        subprocess.call(command, shell=True) 
        #Delete the archive 
        subprocess.call('rm ' + string, shell=True)

def translate_to_geojson(shps, folder): 
    """ Function that create a geojson file 
    from a shp file and reproject to WSG84 using ogr2ogr tool 
    """
    projection = '-t_srs EPSG:4326'
    fmt = '-f GeoJSON'
    output = []
    for shp in shps:   
        src = folder + shp
        dst = folder + shp.split('.')[0] + '.geojson'    
        command = 'ogr2ogr ' + '-progress ' + '-overwrite ' + fmt + ' ' + projection + ' ' +  dst + ' ' + src 
        subprocess.call(command,shell=True)
        output.append(dst)
    return output 

def tiling(files, folder): 
    """ Use of Tippecanoe 
    """
    string = '' 
    for file in files: 
        string += file + ' '
    command = 'tippecanoe -f -o ' + folder + 'result' + '.mbtiles ' + string  #Default zoom 0 to 14 
    subprocess.call(command,shell=True)

def cleaning(folder): 
    """ Clean the folder except the .mbtiles 
    """
    files = [each for each in os.listdir(folder) if not each.endswith('.mbtiles')] 
    for file in files: 
        subprocess.call('rm ' + folder + '/' + file, shell=True)

# To make it a command line script 
#@click.command()
#@click.argument('path', type=click.Path(exists=True), required=True)
def vectorTiling(path):
    """ Function that creates vector tiles
    PATH: either a .json file containing a "url" field that contains the geojson files 
            TODO -> a directory containing more than one of this .json file
    """
    contents = []
    urls = []
    # Input is a .json file 
    if path.endswith('.json'): 
        with open(path, 'rb') as file: 
            content = json.load(file)
            urls.append(content['url'])
            contents.append(content)
    # Input is a directory    
    else: 
        for (path, dirs, files) in os.walk(path): 
            for file in files: 
                if file.endswith('.json'): 
                    uri = path + file 
                    with open(uri,'rb') as f: 
                        content = json.load(f)
                        urls.append(content['url'])
                        contents.append(content)


    # Where all the data will be downloaded - unzipped - processed 
    folder = 'VectorData/'
    urls = list(set(urls)) # A set can't contain duplicate 
    print urls 
    # Get all the data from the url(s)
    get_data(urls, folder)
    # From .shp to .geojson 
    shapefiles = [each for each in os.listdir(folder) if each.endswith('.shp')]
    geojson_files = translate_to_geojson(shapefiles, folder)
    # Tiling the .geojson 
    tiling(geojson_files, folder)
    # Cleaning the directory 
    cleaning(folder)


if __name__ == '__main__':
    example = '/Users/athissen/Documents/GaiaGps/PublicLands/sources/federal/'
    vectorTiling(example)
    