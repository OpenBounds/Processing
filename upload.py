
import os

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import S3ResponseError
import click

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
@click.argument('directory', type=click.Path(exists=True))
def upload(directory):
    """Upload a directory to S3.

    DIRECTORY: Directory to upload. Required.
    """
    if not AWS_BUCKET:
        print 'AWS_BUCKET environment variable not set. Exiting.'
        return

    conn = S3Connection()
    bucket = get_or_create_bucket(conn, AWS_BUCKET)

    files = []
    total_size = 0

    for (dirpath, dirnames, filenames) in os.walk(directory):
        files.extend([f for f in filenames if not f[0] == '.'])

    print 'Found', len(files), 'files to upload to s3://' + AWS_BUCKET

    for path in files:
        full_path = os.path.join(directory, path)
        filesize = os.path.getsize(full_path)
        total_size += filesize

        print 'Uploading', path, '-', sizeof_fmt(filesize)

        k = Key(bucket)
        k.key = path
        k.set_contents_from_filename(full_path)

    print 'Done. Uploaded', sizeof_fmt(total_size)


if __name__ == '__main__':
    upload()
