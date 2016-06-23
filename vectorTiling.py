
import json
import click
import subprocess
import utils 
import os 
import logging
 
@click.command()
@click.argument('sources', type=click.Path(exists=True), required=True)
@click.argument('output', type=click.Path(exists=True), required=True)
@click.argument('min_zoom', default=5)
@click.argument('max_zoom', default=14)
def vectorTiling(sources, output, min_zoom, max_zoom):
    """ Function that creates vector tiles
    PARAMS: 
    	- sources : directory where the geojson file(s) are
    	- output : directory for the generated data 
    """
    files = []
    for file in utils.get_files(sources):
    	if not os.path.isdir(file): 
    		if file.split('.')[1] == 'geojson': 
    			files.append(file)

	logging.info("{} geojson found".format(len(files)))
	paths_string = ''

    for file in files: 
    	with open(file, 'rb') as f: 
        	geojson = json.load(f)
    	features = geojson['features']
    	for item in features: 
        	paths_string += item['properties']['path'] + ' '

    command = 'tippecanoe -f -o ' + output + '/result.mbtiles ' + paths_string + ' -z {} -Z {}'.format(max_zoom, min_zoom)
    subprocess.call(command,shell=True)

if __name__ == '__main__':

    vectorTiling()
    