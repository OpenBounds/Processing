# Scripts

### Python Environment

Create a virtual environment and install the requirements:

```
$ virtualenv env
$ source env/bin/activate
$ pip install -r requirements.txt
```

### validate.py

Validate a JSON file against a JSON schema:

```
$ python scripts/validate.py schemas/source.json sources/US/MT/antelope.json
```

Usage:

```
validate.py [OPTIONS] SCHEMA JSONFILE

  SCHEMA: JSON schema to validate against. Required.
  JSONFILE: JSON file to validate. Required.

Options:
  --help  Show this message and exit.
```

### upload.py

Upload a directory to S3. Requires environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to be set.

```
$ python scripts/upload.py generated/
```

Usage:

```
Usage: upload.py [OPTIONS] DIRECTORY

  DIRECTORY: Directory to upload. Required.

Options:
  --help  Show this message and exit.
```