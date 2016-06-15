
import json
import re

import click
import jsonschema

import utils


@click.command()
@click.argument('schema', type=click.File('r'), required=True)
@click.argument('jsonfiles', type=click.Path(exists=True), required=True)
def validate(schema, jsonfiles):
    """Validate a JSON files against a JSON schema.

    \b
    SCHEMA: JSON schema to validate against. Required.
    JSONFILE: JSON files to validate. Required.
    """
    schema = json.loads(schema.read())

    for path in utils.get_files(jsonfiles):
        with open(path) as f:
            try:
                jsonfile = json.loads(f.read())
            except ValueError:
                utils.error("Error loading json file " + path)
                raise Exception("Invalid json file")
        jsonschema.validate(jsonfile, schema)


if __name__ == '__main__':
    validate()
