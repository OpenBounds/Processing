from __future__ import print_function

import os
from urlparse import urlparse
import sqlite3
from multiprocessing.pool import ThreadPool
from functools import partial
import cStringIO

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import S3ResponseError
import boto
import click

import utils

tile_count = 0
upload_count = 0 

class MBTilesGenerator(object):
    """Generator that returns tiles from an mbtiles file"""
    def __init__(self, mbtiles):
        super(MBTilesGenerator, self).__init__()
        self.db = sqlite3.connect(mbtiles)
        self.cursor = self.db.cursor()
        self.cursor.execute("SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles")

    def len(self):
        c = self.db.cursor()
        c.execute("SELECT count(1) from tiles")
        return c.fetchone()[0]

    def __iter__(self):
        return self

    # Python 3 compatibility
    def __next__(self):
        return self.next()

    def next(self):
        row = self.cursor.fetchone()
        if row == None:
            raise StopIteration()
        zoom, x, y, tile = row
        y  = ((1 << zoom) - y) - 1;
        return zoom, x, y, tile


def upload_tile(bucket, key_template, tile_stuff, progress=True, retries=0):
    try:
        zoom, x, y, tile = tile_stuff
        k = Key(bucket)
        k.key = key_template.format(z=zoom, x=x, y=y)
        k.set_metadata("Content-Encoding", "gzip")
        k.set_contents_from_string(str(tile))
        global upload_count
        upload_count += 1
        if progress and upload_count % 10 == 0:
            print("%i/%i" % (upload_count, tile_count))
    except Exception, e:
        utils.error(str(e))
        if retries < 2:
            upload_tile(bucket, key_template, tile_stuff, progress=progress, retries=retries + 1)
        else:
            raise Exception("Too Many upload failures")


@click.command()
@click.argument('mbtiles', type=click.Path(exists=True), required=True)
@click.argument('s3_url', required=True)
@click.option('--threads', default=10,
    help="Number of simultaneous uploads")
@click.option('--extension', default=".pbf",
    help="File extension for tiles")
def upload(mbtiles, s3_url, threads, extension):
    """Upload tiles from an MBTiles file to S3.

    \b
    PARAMS:
        mbtiles: Path to an MBTiles file
        s3_url: url to an s3 bucket to upload tiles to
    """
    base_url = urlparse(s3_url)
    conn = S3Connection(calling_format=boto.s3.connection.OrdinaryCallingFormat())
    bucket = conn.get_bucket(base_url.netloc)
    key_prefix = base_url.path.lstrip("/")

    tiles = MBTilesGenerator(mbtiles)
    global tile_count
    tile_count = tiles.len()

    key_template = key_prefix + "{z}/{x}/{y}" + extension
    pool = ThreadPool(threads)
    func = partial(upload_tile, bucket, key_template)
    pool.map(func, tiles)


if __name__ == '__main__':
    upload()
