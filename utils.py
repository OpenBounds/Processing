
import os
import ujson
import logging
import tempfile
import sys
from urllib.parse import urlparse
import shutil
import urllib.request, urllib.error, urllib.parse
from contextlib import closing
import zipfile
import tarfile
from boto.s3.connection import S3Connection
from urllib.parse import quote_plus

import hashlib

import click
import requests

CHUNK_SIZE = 1024

logging.basicConfig(level=logging.INFO,
    format='%(asctime)s [%(levelname)s] - %(message)s', datefmt="%H:%M:%S")


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
            info("Returning %s from local cache at %s" % (url, cache_path))
            fp.close()
            shutil.copy(cache_path, fp.name)
            return fp

    s3_cache_bucket = os.getenv("S3_CACHE_BUCKET")
    s3_cache_key = None
    if s3_cache_bucket is not None:
        s3_cache_key = os.getenv("S3_CACHE_PREFIX", "") + quote_plus(url)
        conn = S3Connection(calling_format='boto.s3.connection.OrdinaryCallingFormat')
        if conn is None:
            raise Exception("Error connecting to s3")
        bucket = conn.get_bucket(s3_cache_bucket, validate=False)
        if bucket is None:
            raise Exception("Error getting s3 bucket")

        key = bucket.get_key(s3_cache_key)
        if key is not None:
            info("Found %s in s3 cache at s3://%s/%s" % 
                (url, s3_cache_bucket, s3_cache_key))
            key.get_contents_to_file(fp)
            fp.close()
            return fp

    if parsed_url.scheme == "http" or parsed_url.scheme == "https":
        res = requests.get(url, stream=True, verify=False)

        if not res.ok:
            raise IOError

        for chunk in res.iter_content(CHUNK_SIZE):
            fp.write(chunk)
    elif parsed_url.scheme == "ftp":
        download = urllib.request.urlopen(url)
    
        file_size_dl = 0
        block_sz = 8192
        while True:
            buffer = download.read(block_sz)
            if not buffer:
                break
    
            file_size_dl += len(buffer)
            fp.write(buffer)

    fp.close()

    if cache_path:
        if not os.path.exists(download_cache):
            os.makedirs(download_cache)
        shutil.copy(fp.name, cache_path)

    if s3_cache_key:
        info("Putting %s to s3 cache at s3://%s/%s" % 
                (url, s3_cache_bucket, s3_cache_key))
        key = bucket.new_key(s3_cache_key)
        key.set_contents_from_filename(fp.name)

    return fp

forceLogging = True
def info(*strings):
    if not forceLogging and sys.stdout.isatty():
        click.echo(' '.join(strings))
    else:
        logging.info(' '.join(strings))


def error(*strings):
    if not forceLogging and sys.stdout.isatty():
        click.secho(' '.join(strings), fg='red')
    else:
        logging.error(' '.join(strings))


def success(*strings):
    if not forceLogging and sys.stdout.isatty():
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
