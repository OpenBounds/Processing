
import os
import ujson
import logging
import tempfile
import sys
from urlparse import urlparse
import shutil
import urllib2
from contextlib import closing
import zipfile
import tarfile

import hashlib

import click
import requests

CHUNK_SIZE = 1024

logging.basicConfig(level=logging.INFO)


def get_files(path):
    """Returns an iterable containing the full path of all files in the
    specified path.

    :param path: string
    :yields: string
    """
    if os.path.isdir(path):
        for (dirpath, dirnames, filenames) in os.walk(path):
            for filename in filenames:
                if not filename[0] == '.':
                    yield os.path.join(dirpath, filename)
    else:
        yield path


def read_json(path):
    """Returns JSON dict from file.

    :param path: string
    :returns: dict
    """
    with open(path, 'r') as jsonfile:
        return ujson.loads(jsonfile.read())


def write_json(path, data):
    with open(path, 'w') as jsonfile:
        jsonfile.write(ujson.dumps(data, double_precision=5))


def make_sure_path_exists(path):
    """Make directories in path if they do not exist.

    Modified from http://stackoverflow.com/a/5032238/1377021

    :param path: string
    """
    try:
        os.makedirs(path)
    except:
        pass


def get_path_parts(path):
    """Splits a path into parent directories and file.

    :param path: string
    """
    return path.split(os.sep)


def download(url):
    """Downloads a file and returns a file pointer to a temporary file.

    :param url: string
    """
    parsed_url = urlparse(url)

    urlfile = parsed_url.path.split('/')[-1]
    _, extension = os.path.split(urlfile)

    fp = tempfile.NamedTemporaryFile('wb', suffix=extension, delete=False)

    download_cache = os.getenv("DOWNLOAD_CACHE")
    cache_path = None
    if download_cache is not None:
        cache_path = os.path.join(download_cache,
            hashlib.sha224(url).hexdigest())
        if os.path.exists(cache_path):
            info("Returning %s from cache" % url)
            fp.close()
            shutil.copy(cache_path, fp.name)
            return fp

    if parsed_url.scheme == "http" or parsed_url.scheme == "https":
        res = requests.get(url, stream=True, verify=False)

        if not res.ok:
            raise IOError

        for chunk in res.iter_content(CHUNK_SIZE):
            fp.write(chunk)
    elif parsed_url.scheme == "ftp":
        with closing(urllib2.urlopen(url)) as r:
            shutil.copyfileobj(r, fp)

    fp.close()

    if cache_path:
        if not os.path.exists(download_cache):
            os.makedirs(download_cache)
        shutil.copy(fp.name, cache_path)

    return fp

def polygon_from_bbox(bbox):
    """ Generate a polygon geometry from a ESWN bouding box

    :param bbox: a 4 float bounding box
    :returns: a polygon geometry
    """
    return [[
        [bbox[0], bbox[1]],
        [bbox[2], bbox[1]],
        [bbox[2], bbox[3]],
        [bbox[0], bbox[3]],
        [bbox[0], bbox[1]]
    ]]


def info(*strings):
    if sys.stdout.isatty():
        click.echo(' '.join(strings))
    else:
        logging.info(' '.join(strings))


def error(*strings):
    if sys.stdout.isatty():
        click.secho(' '.join(strings), fg='red')
    else:
        logging.error(' '.join(strings))


def success(*strings):
    if sys.stdout.isatty():
        click.secho(' '.join(strings), fg='green')
    else:
        logging.info(' '.join(strings))


class ZipCompatibleTarFile(tarfile.TarFile):
    """Wrapper around TarFile to make it more compatible with ZipFile"""
    def infolist(self):
        members = self.getmembers()
        for m in members:
            m.filename = m.name
        return members

    def namelist(self):
        return self.getnames()

ARCHIVE_FORMAT_ZIP = "zip"
ARCHIVE_FORMAT_TAR_GZ = "tar.gz"
ARCHIVE_FORMAT_TAR_BZ2 = "tar.bz2"

def get_compressed_file_wrapper(path):
    archive_format = None

    if path.endswith(".zip"):
        archive_format = ARCHIVE_FORMAT_ZIP
    elif path.endswith(".tar.gz") or path.endswith(".tgz"):
        archive_format = ARCHIVE_FORMAT_TAR_GZ
    elif path.endswith(".tar.bz2"):
        archive_format = ARCHIVE_FORMAT_TAR_BZ2
    else:
        try:
            with zipfile.ZipFile(path, "r") as f:
                archive_format = ARCHIVE_FORMAT_ZIP
        except:
            try:
                f = tarfile.TarFile.open(path, "r")
                f.close()
                archive_format = ARCHIVE_FORMAT_ZIP
            except:
                pass

    if archive_format is None:
        raise Exception("Unable to determine archive format")

    if archive_format == ARCHIVE_FORMAT_ZIP:
        return zipfile.ZipFile(path, 'r')
    elif archive_format == ARCHIVE_FORMAT_TAR_GZ:
        return ZipCompatibleTarFile.open(path, 'r:gz')
    elif archive_format == ARCHIVE_FORMAT_TAR_BZ2:
        return ZipCompatibleTarFile.open(path, 'r:bz2')
