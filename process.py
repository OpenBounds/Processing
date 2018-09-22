#!/usr/bin/env python3

import json
import os
from urllib.parse import urlparse
import sys
import zipfile 
import logging
import requests

import click

import adapters
from filters import BasicFilterer
import utils
import geoutils

@click.command()
@click.argument('sources', type=click.Path(exists=True), required=True)
@click.argument('output', type=click.Path(exists=True), required=True)
@click.option('--force', is_flag=True)
def process(sources, output, force):
    """Download sources and process the file to the output directory.

    \b
    SOURCES: Source JSON file or directory of files. Required.
    OUTPUT: Destination directory for generated data. Required.
    """
    logging.basicConfig(level=logging.INFO,
        format='%(asctime)s [%(levelname)s] - %(message)s', datefmt="%H:%M:%S")

    logging.getLogger('shapely.geos').setLevel(logging.WARNING)
    logging.getLogger('Fiona').setLevel(logging.WARNING)
    logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARNING)
    requests.packages.urllib3.disable_warnings()
    # logging.getLogger('processing').setLevel(logging.DEBUG)

    catalog_features = []
    failures = []
    path_parts_to_skip = len(utils.get_path_parts(output))
    success = True
    for path in utils.get_files(sources):
        try:
            logging.info("Processing " + path)
            pathparts = utils.get_path_parts(path)
            pathparts[0] = output.strip(os.sep)
            pathparts[-1] = pathparts[-1].replace('.json', '.geojson')
    
            outdir = os.sep.join(pathparts[:-1])
            outfile = os.sep.join(pathparts)
    
            source = utils.read_json(path)
            urlfile = urlparse(source['url']).path.split('/')[-1]
    
            if not hasattr(adapters, source['filetype']):
                logging.error('Unknown filetype ' + source['filetype'])
                failures.append(path)
                continue
    
            if os.path.isfile(outfile) and \
                os.path.getmtime(outfile) > os.path.getmtime(path) and not force:
                logging.warning('Skipping ' + path + ' since generated file exists. Use --force to regenerate.')
                with open(outfile, "rb") as f:
                    geojson = json.load(f)
                properties = geojson['properties']
            else:
                logging.info('Downloading ' + source['url'])
    
                try:
                    fp = utils.download(source['url'])
                except IOError:
                    logging.error('Failed to download ' + source['url'])
                    failures.append(path)
                    continue
    
                logging.info('Reading ' + urlfile)
    
                if 'filter' in source:
                    filterer = BasicFilterer(source['filter'], source.get('filterOperator', 'and'))
                else:
                    filterer = None
    
                try:
                    geojson = getattr(adapters, source['filetype'])\
                        .read(fp, source['properties'],
                            filterer=filterer,
                            layer_name=source.get("layerName", None),
                            source_filename=source.get("filenameInZip", None))
                except IOError as e:
                    logging.error('Failed to read ' + urlfile + " " + str(e))
                    failures.append(path)
                    continue
                except zipfile.BadZipfile as e:
                    logging.error('Unable to open zip file ' + source['url'])
                    failures.append(path)
                    continue
                finally:
                    os.remove(fp.name)
                if(len(geojson['features'])) == 0:
                    logging.error("Result contained no features for " + path)
                    continue
                excluded_keys = ['filetype', 'url', 'properties', 'filter', 'filenameInZip']
                properties = {k:v for k,v in list(source.items()) if k not in excluded_keys}
                properties['source_url'] = source['url']
                properties['feature_count'] = len(geojson['features'])
                logging.info("Generating demo point")
                properties['demo'] = geoutils.get_demo_point(geojson)
                
                geojson['properties'] = properties
    
                utils.make_sure_path_exists(outdir)

                filename_to_match, ext = os.path.splitext(pathparts[-1])
                for name in os.listdir(os.sep.join(pathparts[:-1])):
                    base = name.split(".")[0]
                    if base == filename_to_match:
                        to_remove = list(pathparts[:-1])
                        to_remove.append(name)
                        to_remove = os.sep.join(to_remove)
                        logging.info("Removing generated file " + to_remove)
                        os.remove(to_remove)

                utils.write_json(outfile, geojson)

                logging.info("Generating label points")
                label_geojson = geoutils.get_label_points(geojson)
                label_pathparts = list(pathparts)
                label_pathparts[-1] = label_pathparts[-1].replace('.geojson', '.labels.geojson')
                label_path = os.sep.join(label_pathparts)
                utils.write_json(label_path, label_geojson)

                logging.info('Done. Processed to ' + outfile)
    
            if not "demo" in properties:
                properties['demo'] = geoutils.get_demo_point(geojson)

            properties['path'] = "/".join(pathparts[path_parts_to_skip:])
            catalog_entry = {
                'type': 'Feature',
                'properties': properties,
                'geometry': geoutils.get_union(geojson)
            }
            catalog_features.append(catalog_entry)
        except Exception as e:
            logging.error(str(e))
            logging.exception("Error processing file " + path)
            failures.append(path)
            success = False

    catalog = {
        'type': 'FeatureCollection',
        'features': catalog_features
    }
    utils.write_json(os.path.join(output,'catalog.geojson'), catalog)

    if not success:
        logging.error("Failed sources: " + ", ".join(failures))
        sys.exit(-1)

if __name__ == '__main__':
    process()
