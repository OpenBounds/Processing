
import os
import json
import logging
import tempfile
import sys
from urlparse import urlparse

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
        return json.loads(jsonfile.read())


def write_json(path, data):
    with open(path, 'w') as jsonfile:
        jsonfile.write(json.dumps(data, indent=4, separators=(',', ': ')))


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
    res = requests.get(url, stream=True, verify=False)
    urlfile = urlparse(url).path.split('/')[-1]
    _, extension = os.path.split(urlfile)

    if not res.ok:
        raise IOError

    fp = tempfile.NamedTemporaryFile('wb', suffix=extension, delete=False)

    for chunk in res.iter_content(CHUNK_SIZE):
        fp.write(chunk)

    fp.close()

    return fp


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
