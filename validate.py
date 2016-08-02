
import json
import re
import sys

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
    success = True
    for path in utils.get_files(jsonfiles):
        with open(path) as f:
            try:
                jsonfile = json.loads(f.read())
            except ValueError:
                utils.error("Error loading json file " + path)
                raise Exception("Invalid json file")
        try:
            jsonschema.validate(jsonfile, schema)
        except Exception, e:
            success = False
            utils.error("Error validating file " + path)
            utils.error(str(e))

    if not success:
        sys.exit(-1)

if __name__ == '__main__':
    validate()
