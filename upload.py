
import os

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import S3ResponseError
import click

import utils

AWS_BUCKET = os.environ.get('AWS_BUCKET')


def get_or_create_bucket(conn, bucket_name):
    """Get or create an S3 bucket.

    :param conn: boto.s3.connection.S3Connection
    :param bucket_name: string
    :returns: boto.s3.bucket.Bucket
    """
    try:
        bucket = conn.get_bucket(bucket_name)
    except S3ResponseError:
        bucket = conn.create_bucket(bucket_name)

    return bucket


def sizeof_fmt(num):
    """Human readable file size.

    Modified from http://stackoverflow.com/a/1094933/1377021

    :param num: float
    :returns: string
    """
    for unit in ['', 'k', 'm', 'g', 't', 'p', 'e', 'z']:
        if abs(num) < 1024.0:
            return "%.0f%s%s" % (num, unit, 'b')
        num /= 1024.0

    return "%.f%s%s" % (num, 'y', 'b')


@click.command()
@click.argument('directory', type=click.Path(exists=True), required=True)
def upload(directory):
    """Upload a directory to S3.

    DIRECTORY: Directory to upload. Required.
    """
    if not AWS_BUCKET:
        utils.error('AWS_BUCKET environment variable not set. Exiting.')
        return

    conn = S3Connection()
    bucket = get_or_create_bucket(conn, AWS_BUCKET)

    files = list(utils.get_files(directory))
    total_size = 0

    utils.info('Found', len(files), 'files to upload to s3://' + AWS_BUCKET)

    for path in files:
        filesize = os.path.getsize(path)
        total_size += filesize

        utils.info('Uploading', path, '-', sizeof_fmt(filesize))

        k = Key(bucket)
        k.key = path
        k.set_contents_from_filename(path)

    utils.success('Done. Uploaded', sizeof_fmt(total_size))


if __name__ == '__main__':
    upload()
