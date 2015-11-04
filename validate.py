
import json

import click
import jsonschema


@click.command()
@click.argument('schema', type=click.File('r'), required=True)
@click.argument('jsonfile', type=click.File('r'), required=True)
def validate(schema, jsonfile):
    """Validate a JSON file against a JSON schema.

    \b
    SCHEMA: JSON schema to validate against. Required.
    JSONFILE: JSON file to validate. Required.
    """

    schema = json.loads(schema.read())
    jsonfile = json.loads(jsonfile.read())

    jsonschema.validate(jsonfile, schema)


if __name__ == '__main__':
    validate()
