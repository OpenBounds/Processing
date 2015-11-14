
from functools import partial

import fiona
from fiona.transform import transform_geom


def _explode(coords):
    """Explode a GeoJSON geometry's coordinates object and
    yield coordinate tuples. As long as the input is conforming,
    the type of the geometry doesn't matter.

    From @sgillies answer: http://gis.stackexchange.com/a/90554/27367
    """
    for e in coords:
        if isinstance(e, (float, int, long)):
            yield coords
            break
        else:
            for f in _explode(e):
                yield f


def _bbox(feat):
    x, y = zip(*list(_explode(feat['geometry']['coordinates'])))
    return min(x), min(y), max(x), max(y)


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
        collection = {
            'type': 'FeatureCollection',
            'features': [],
            'bbox': [float('inf'), float('inf'), float('-inf'), float('-inf')]
        }

        for rec in source:
            transformed = _transformer(source.crs, rec)
            collection['bbox'] = [
                comparator(values)
                for comparator, values in zip(
                    [min, min, max, max],
                    zip(collection['bbox'], _bbox(transformed))
                )
            ]
            collection['features'].append(transformed)

    return collection


def write():
    pass
