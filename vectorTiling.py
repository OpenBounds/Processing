
import json
import click
import subprocess
import utils 
import logging
 
@click.command()
@click.argument('sources', type=click.Path(exists=True), required=True)
@click.argument('output', required=True)
@click.argument('min_zoom', default=5)
@click.argument('max_zoom', default=14)
def vectorTiling(sources, output, min_zoom, max_zoom):
    """ Function that creates vector tiles
    PARAMS: 
    	- sources : directory where the geojson file(s) are
    	- output : file.mbtiles for the generated data 
    """

    files = []
    for f in utils.get_files(sources):
    	if utils.get_path_parts(f)[-1].split('.')[1] == 'geojson':
            files.append(f)

	logging.info("{} geojson found".format(len(files)))
	paths_string = ''

    for item in files: 
    	with open(item, 'rb') as f: 
        	geojson = json.load(f)
    	features = geojson['features']
    	for item in features: 
        	paths_string += item['properties']['path'] + ' '

    command = 'tippecanoe -f -o ' + output + ' ' + paths_string + ' -z {} -Z {}'.format(max_zoom, min_zoom)
    subprocess.call(command,shell=True)

if __name__ == '__main__':

    vectorTiling()
    