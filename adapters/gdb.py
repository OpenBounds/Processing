import zipfile
import os
import tempfile
import shutil


import fiona
import fiona_dataset

def read(fp, prop_map, filterer=None, source_filename=None, layer_name=None):
    """Read FileGeoDatabase.

    :param fp: file-like object
    :param prop_map: dictionary mapping source properties to output properties
    :param source_filename: Filename to read, only applicable if fp is a zip file
    """
    #search for a shapefile in the zip file, unzip if found
    unzip_dir = tempfile.mkdtemp(suffix=".gdb")
    with zipfile.ZipFile(fp.name, 'r') as zipped_file:
        zipped_file.extractall(unzip_dir)

    #Open the shapefile
    with fiona.open(os.path.join(unzip_dir, source_filename), layer=layer_name) as source:
        collection = fiona_dataset.read_fiona(source, prop_map, filterer)

    shutil.rmtree(unzip_dir)

    return collection
