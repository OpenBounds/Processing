
from functools import partial

import fiona
from fiona.transform import transform_geom


def _transformer(crs, feat):
    tg = partial(transform_geom, crs, 'EPSG:4326',
                 antimeridian_cutting=True, precision=6)
    feat['geometry'] = tg(feat['geometry'])
    return feat


def read(fp):
    """Read shapefile.

    :param fp: file-like object
    """
    layers = fiona.listlayers('/', vfs='zip://' + fp.name)

    if not layers:
        raise IOError

    filename = '/' + layers[0] + '.shp'

    with fiona.open(filename, vfs='zip://' + fp.name) as source:
        meta = source.meta
        meta['fields'] = dict(source.schema['properties'].items())

        collection = {
            'type': 'FeatureCollection',
            'fiona:schema': meta['schema'],
            'fiona:crs': meta['crs'],
            'features': [_transformer(source.crs, rec) for rec in source]
        }

    return collection


def write():
    pass
