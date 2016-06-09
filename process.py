
import os
from urlparse import urlparse
import zipfile 
import json

import click

import adapters
import utils
from filters import BasicFilterer

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
    catalog_features = []
    base_output_path_parts = utils.get_path_parts(output)
    for path in utils.get_files(sources):
        pathparts = utils.get_path_parts(path)
        pathparts[0] = output.strip(os.sep)
        pathparts[-1] = pathparts[-1].replace('.json', '.geojson')

        outdir = os.sep.join(pathparts[:-1])
        outfile = os.sep.join(pathparts)

        source = utils.read_json(path)
        urlfile = urlparse(source['url']).path.split('/')[-1]

        if not hasattr(adapters, source['filetype']):
            utils.error('Unknown filetype', source['filetype'], '\n')
            continue

        if os.path.isfile(outfile) and not force:
            utils.error('Skipping', path, 'since generated file exists.',
                        'Use --force to regenerate.', '\n')
            with open(outfile, "rb") as f:
                geojson = json.load(f)
            properties = geojson['properties']
        else:
            utils.info('Downloading', source['url'])

            try:
                fp = utils.download(source['url'])
            except IOError:
                utils.error('Failed to download', source['url'], '\n')
                continue

            utils.info('Reading', urlfile)

            if 'filter' in source:
                filterer = BasicFilterer(source['filter'], source.get('filter_operator', 'and'))
            else:
                filterer = None

            try:
                geojson = getattr(adapters, source['filetype'])\
                    .read(fp, source['properties'],
                        filterer=filterer,
                        source_filename=source.get("filenameInZip", None))
            except IOError:
                utils.error('Failed to read', urlfile)
                continue
            except zipfile.BadZipfile, e:
                utils.error('Unable to open zip file', source['url'])
                continue
            finally:
                os.remove(fp.name)

            excluded_keys = ['filetype', 'url', 'properties', 'filter', 'filenameInZip']
            properties = {k:v for k,v in source.iteritems() if k not in excluded_keys}
            properties['source_url'] = source['url']

            geojson['properties'] = properties

            utils.make_sure_path_exists(outdir)
            utils.write_json(outfile, geojson)
    
            utils.success('Done. Processed to', outfile, '\n')

        properties['path'] = "/".join(pathparts[len(base_output_path_parts):])
        catalog_entry = {
            'type': 'Feature',
            'properties': properties,
            'geometry': {
                'type': 'Polygon',
                'coordinates': utils.polygon_from_bbox(geojson['bbox'])
            }
        }
        catalog_features.append(catalog_entry)


    catalog = {
        'type': 'FeatureCollection',
        'features': catalog_features
    }
    utils.write_json(os.path.join(output,'catalog.geojson'), catalog)

if __name__ == '__main__':
    process()
