import zipfile
import os
import tempfile
import shutil

from property_transformation import get_transformed_properties

import fiona
import fiona_dataset


def read(fp, prop_map, filterer=None, source_filename=None, layer_name=None):
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
        collection = fiona_dataset.read_fiona(source, prop_map, filterer)

    shutil.rmtree(unzip_dir)

    return collection
