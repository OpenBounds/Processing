from __future__ import print_function

import os
from urlparse import urlparse
import sqlite3
from multiprocessing.pool import ThreadPool
from functools import partial

import boto3
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


def upload_tile(s3, bucket, key_template, headers, tile_stuff, progress=True, retries=0):
    try:
        zoom, x, y, tile = tile_stuff
        s3.put_object(Body=str(tile), 
            Bucket=bucket,
            Key=key_template.format(z=zoom, x=x, y=y), 
            ContentType=headers.get("Content-Type", ''),
            ContentEncoding=headers.get("Content-Encoding", ''),
            CacheControl=headers.get("Cache-Control", '')
        )
        global upload_count
        upload_count += 1
        if progress and upload_count % 10 == 0:
            print("%i/%i" % (upload_count, tile_count))
    except Exception, e:
        utils.error(str(e))
        if retries < 2:
            upload_tile(s3, bucket, key_template, headers, tile_stuff, progress=progress, retries=retries + 1)
        else:
            raise Exception("Too Many upload failures")


@click.command()
@click.argument('mbtiles', type=click.Path(exists=True), required=True)
@click.argument('s3_url', required=True)
@click.option('--threads', default=10,
    help="Number of simultaneous uploads")
@click.option('--extension', default=".pbf",
    help="File extension for tiles")
@click.option('--header', '-h', multiple=True,
    help="Additional headers")
def upload(mbtiles, s3_url, threads, extension, header):
    """Upload tiles from an MBTiles file to S3.

    \b
    PARAMS:
        mbtiles: Path to an MBTiles file
        s3_url: url to an s3 bucket to upload tiles to
    """
    base_url = urlparse(s3_url)

    s3 = boto3.client('s3')
    bucket = base_url.netloc
    key_prefix = base_url.path.lstrip("/")

    headers = {}
    if header is not None:
        for h in header:
            k,v = h.split(":")
            if k not in ("Cache-Control", "Content-Type", "Content-Encoding"):
                raise Exception("Unsupported header")
            headers[k] = v

    if extension == ".pbf":
        headers.update({
            "Content-Encoding":"gzip",
            "Content-Type": "application/vnd.mapbox-vector-tile"
        })
    elif extension == ".webp":
        headers.update({
            "Content-Type":"image/webp"
        })
    elif extension == ".png":
        headers.update({
            "Content-Type":"image/png"
        })
    elif extension == ".jpg" or extension == ".jpeg":
        headers.update({
            "Content-Type":"image/jpeg"
        })

    tiles = MBTilesGenerator(mbtiles)
    global tile_count
    tile_count = tiles.len()

    key_template = key_prefix + "{z}/{x}/{y}" + extension
    print("uploading tiles from %s to s3://%s/%s" % (mbtiles, bucket, key_template))
    pool = ThreadPool(threads)
    func = partial(upload_tile, s3, bucket, key_template, headers)
    pool.map(func, tiles)


if __name__ == '__main__':
    upload()
