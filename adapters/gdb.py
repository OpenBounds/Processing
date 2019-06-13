import os
import shutil
import tempfile
import zipfile

import fiona

from . import fiona_dataset
from utils import get_compressed_file_wrapper


def read(
    fp, prop_map, filterer=None, source_filename=None, layer_name=None, merge_on=None
):
    """Read FileGeoDatabase.

    :param fp: file-like object
    :param prop_map: dictionary mapping source properties to output properties
    :param source_filename: Filename to read, only applicable if fp is a zip file
    """
    # search for a shapefile in the zip file, unzip if found
    unzip_dir = tempfile.mkdtemp(suffix=".gdb")
    gdb_name = source_filename
    zipped_file = get_compressed_file_wrapper(fp.name)

    if gdb_name is None:
        for name in zipped_file.infolist():
            dirname = os.path.dirname(name.filename)
            base, ext = os.path.splitext(dirname)
            if ext == ".gdb":
                if gdb_name is not None and gdb_name != dirname:
                    raise Exception("Found multiple .gdb entries in zipfile")
                gdb_name = dirname

        if gdb_name is None:
            raise Exception(
                "Unabled to find .gdb directory in zipfile, and filenameInZip not set"
            )

    zipped_file.extractall(unzip_dir)
    zipped_file.close()

    # Open the shapefile
    with fiona.open(os.path.join(unzip_dir, gdb_name), layer=layer_name) as source:
        collection = fiona_dataset.read_fiona(
            source, prop_map, filterer, merge_on=merge_on
        )

    shutil.rmtree(unzip_dir)

    return collection
