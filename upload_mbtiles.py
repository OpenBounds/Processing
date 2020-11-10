#!/usr/bin/env python3
import json
import logging
import os
import sqlite3
from functools import partial
from multiprocessing.pool import ThreadPool
from urllib.parse import urlparse

import boto3
import click

import utils

upload_progress_interval = 100
tile_count = 0
upload_count = 0


class MBTilesGenerator(object):
    """Generator that returns tiles from an mbtiles file"""

    def __init__(self, mbtiles):
        super(MBTilesGenerator, self).__init__()
        self.db = sqlite3.connect(mbtiles)
        self.cursor = self.db.cursor()
        self.cursor.execute(
            "SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles"
        )

    def len(self):
        c = self.db.cursor()
        c.execute("SELECT count(1) from tiles")
        return c.fetchone()[0]

    def __iter__(self):
        return self

    def __next__(self):
        row = self.cursor.fetchone()
        if row == None:
            raise StopIteration()
        zoom, x, y, tile = row
        y = ((1 << zoom) - y) - 1
        return zoom, x, y, tile


def get_tile_json(mbtiles, bucket, key_template):
    db = sqlite3.connect(mbtiles)
    cursor = db.cursor()
    cursor.execute("SELECT name, value FROM metadata")
    tilejson = {
        "tilejson": "2.2.0",
        "scheme": "xyz",
        "tiles": ["https://s3.amazonaws.com/{}/{}".format(bucket, key_template)],
    }
    for key, value in cursor.fetchall():
        if key == "json":
            data = json.loads(value)
            tilejson.update(data)
        else:
            if key in ("center", "bounds"):
                value = [float(s) for s in value.split(",")]
            elif key in ("minzoom", "maxzoom"):
                value = int(value)
            tilejson[key] = value
    return tilejson


def upload_tile(
    s3, bucket, key_template, headers, tile_stuff, progress=True, retries=0
):
    try:
        zoom, x, y, tile = tile_stuff
        s3.put_object(
            Body=tile,
            Bucket=bucket,
            Key=key_template.format(z=zoom, x=x, y=y),
            ContentType=headers.get("Content-Type", ""),
            ContentEncoding=headers.get("Content-Encoding", ""),
            CacheControl=headers.get("Cache-Control", ""),
        )
        global upload_count
        upload_count += 1
        if progress and upload_count % upload_progress_interval == 0:
            print("%i/%i" % (upload_count, tile_count))
    except Exception as e:
        logging.error(str(e))
        if retries < 2:
            upload_tile(
                s3,
                bucket,
                key_template,
                headers,
                tile_stuff,
                progress=progress,
                retries=retries + 1,
            )
        else:
            raise Exception("Too Many upload failures")


@click.command()
@click.argument("mbtiles", type=click.Path(exists=True), required=True)
@click.argument("s3_url", required=True)
@click.option("--threads", default=10, help="Number of simultaneous uploads")
@click.option("--extension", default=".pbf", help="File extension for tiles")
@click.option("--header", "-h", multiple=True, help="Additional headers")
@click.option(
    "--progress", "-p", default=False, is_flag=True, help="Show upload progress"
)
@click.option("--debug", "-d", default=False, help="Debug level logging", is_flag=True)
def upload(mbtiles, s3_url, threads, extension, header, progress, debug):
    """Upload tiles from an MBTiles file to S3.

    \b
    PARAMS:
        mbtiles: Path to an MBTiles file
        s3_url: url to an s3 bucket to upload tiles to
    """
    logging.basicConfig(level=logging.DEBUG if debug else logging.INFO)
    logging.getLogger("botocore.credentials").setLevel(logging.getLevelName("ERROR"))
    logging.getLogger(
        "botocore.vendored.requests.packages.urllib3.connectionpool"
    ).setLevel(logging.getLevelName("ERROR"))
    logging.getLogger("urllib3.connectionpool").setLevel(logging.getLevelName("ERROR"))

    base_url = urlparse(s3_url)

    s3 = boto3.client("s3")
    bucket = base_url.netloc
    key_prefix = base_url.path.lstrip("/")

    headers = {}
    if header is not None:
        for h in header:
            k, v = h.split(":")
            if k not in ("Cache-Control", "Content-Type", "Content-Encoding"):
                raise Exception("Unsupported header")
            headers[k] = v

    if extension == ".pbf":
        headers.update(
            {
                "Content-Encoding": "gzip",
                "Content-Type": "application/vnd.mapbox-vector-tile",
            }
        )
    elif extension == ".webp":
        headers.update({"Content-Type": "image/webp"})
    elif extension == ".png":
        headers.update({"Content-Type": "image/png"})
    elif extension == ".jpg" or extension == ".jpeg":
        headers.update({"Content-Type": "image/jpeg"})

    tiles = MBTilesGenerator(mbtiles)
    global tile_count
    tile_count = tiles.len()

    key_template = key_prefix + "{z}/{x}/{y}" + extension
    logging.info(f"uploading tiles from {mbtiles} to s3://{bucket}/{key_template}")
    pool = ThreadPool(threads)
    func = partial(upload_tile, s3, bucket, key_template, headers, progress=progress)
    pool.map(func, tiles)

    tilejson_key = "{}/index.json".format(key_prefix.strip("/"))
    logging.info(f"uploading tilejson to s3://{bucket}/{tilejson_key}")
    tilejson_data = get_tile_json(mbtiles, bucket, key_template)
    tilejson_json = json.dumps(tilejson_data, indent=4, sort_keys=True)
    s3.put_object(
        Body=json.dumps(tilejson_data),
        Bucket=bucket,
        Key=tilejson_key,
        ContentType="application/json",
    )


if __name__ == "__main__":
    upload()
