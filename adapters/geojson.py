import os
import shutil
import tempfile

import fiona
from utils import get_compressed_file_wrapper

from . import fiona_dataset


def read(fp, prop_map, filterer=None, source_filename=None, layer_name=None):
    """Read geojson file.

    :param fp: file-like object
    :param prop_map: dictionary mapping source properties to output properties
    :param source_filename: Filename to read, only applicable if fp is a zip file
    """
    filename = os.path.basename(fp.name)
    root, ext = os.path.splitext(filename)

    unzip_dir = tempfile.mkdtemp()

    if ext == ".geojson" or ext == ".json":
        file_to_process = fp.name
    else:
        # search for a geojson file in the zip file, unzip if found
        shp_name = source_filename
        zipped_file = get_compressed_file_wrapper(fp.name)

        if shp_name is None:
            for name in zipped_file.infolist():
                base, ext = os.path.splitext(name.filename)
                if ext == ".geojson":
                    if shp_name is not None:
                        raise Exception("Found multiple shapefiles in zipfile")
                    shp_name = name.filename

            if shp_name is None:
                raise Exception("Found 0 shapefiles in zipfile")

        zipped_file.extractall(unzip_dir)
        zipped_file.close()
        file_to_process = os.path.join(unzip_dir, shp_name)

    # Open the shapefile
    with fiona.open(file_to_process) as source:
        collection = fiona_dataset.read_fiona(source, prop_map, filterer)

    shutil.rmtree(unzip_dir)

    return collection
