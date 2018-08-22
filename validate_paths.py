#!/usr/bin/env python3

import json
import re

import click
import jsonschema

import utils


@click.command()
@click.argument('schema', type=click.File('r'), required=True)
@click.argument('jsonfiles', type=click.Path(exists=True), required=True)
def validate_path(schema, jsonfiles):
    schema = json.loads(schema.read())

    for path in utils.get_files(jsonfiles):
        path_components = utils.get_path_parts(path)

        regex = schema[path_components[0]]
        if not re.compile(regex).match(path):
            raise AssertionError('Path "%s" does not match spec "%s"' %
                (path, regex))

if __name__ == '__main__':
    validate_path()
