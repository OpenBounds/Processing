from functools import partial
import zipfile
import os
import tempfile
import shutil

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


def read(fp, prop_map, source_filename=None):
    """Read shapefile.

    :param fp: file-like object
    :param prop_map: dictionary mapping source properties to output properties
    :param source_filename: Filename to read, only applicable if fp is a zip file
    """
    #search for a shapefile in the zip file, unzip if found
    unzip_dir = tempfile.mkdtemp()
    shp_name = source_filename
    with zipfile.ZipFile(fp.name, 'r') as zipped_file:
        if shp_name is None:
            for name in zipped_file.namelist():
                base, ext = os.path.splitext(name)
                if ext == ".shp":
                    if shp_name is not None:
                        raise Exception("Found multiple shapefiles in zipfile")
                    shp_name = name

            if shp_name is None:
                raise Exception("Found 0 shapefiles in zipfile")

        zipped_file.extractall(unzip_dir)

    #Open the shapefile
    with fiona.open(os.path.join(unzip_dir, shp_name)) as source:
        collection = {
            'type': 'FeatureCollection',
            'features': [],
            'bbox': [float('inf'), float('inf'), float('-inf'), float('-inf')]
        }

        for rec in source:
            transformed = _transformer(source.crs, rec)
            transformed['properties'] = {
                key: str(transformed['properties'][value])
                for key, value in prop_map.iteritems()
            }
            collection['bbox'] = [
                comparator(values)
                for comparator, values in zip(
                    [min, min, max, max],
                    zip(collection['bbox'], _bbox(transformed))
                )
            ]
            collection['features'].append(transformed)

    shutil.rmtree(unzip_dir)

    return collection
