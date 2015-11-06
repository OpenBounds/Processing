
import os
from urlparse import urlparse

import click

import adapters
import utils


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
            continue

        print 'Downloading', source['url']

        try:
            fp = utils.download(source['url'])
        except IOError:
            utils.error('Failed to download', source['url'], '\n')
            continue

        print 'Reading', urlfile

        try:
            geojson = getattr(adapters, source['filetype']).read(fp)
        except IOError:
            utils.error('Failed to read', urlfile)
            os.remove(fp.name)
            continue

        os.remove(fp.name)

        utils.make_sure_path_exists(outdir)
        utils.write_json(outfile, geojson)

        utils.success('Done. Processed to', outfile, '\n')


if __name__ == '__main__':
    process()
