
import os
import json

import click

import adapters
import utils


@click.command()
@click.argument('source', type=click.Path(exists=True))
@click.argument('output', type=click.Path(exists=True))
def process(source, output):
    for path in utils.get_files(source):
        with open(path, 'r') as jsonfile:
            source = json.loads(jsonfile.read())

        if hasattr(adapters, source['filetype']):
            geojson = getattr(adapters, source['filetype']).read(source['url'])
        else:
            print 'Unknown filetype', source['filetype']
            continue

        out = path.replace('.json', '.geojson').replace('sources', 'generated')
        outdir, _ = os.path.split(out)

        utils.make_sure_path_exists(outdir)

        with open(out, 'w') as outfile:
            dump = json.dumps(geojson, indent=4, separators=(',', ': '))
            outfile.write(dump)


if __name__ == '__main__':
    process()
