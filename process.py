#!/usr/bin/env python3
import datetime
import json
import logging
import os
import sys
import zipfile
from shutil import rmtree
from urllib.parse import urlparse

import click
import requests

import adapters
import geoutils
import utils
from filters import BasicFilterer
from merge import merge_features


@click.command()
@click.argument("sources", type=click.Path(exists=True), required=True)
@click.argument("output", type=click.Path(exists=True), required=True)
@click.option("--force", is_flag=True)
@click.option("--force-summary", is_flag=True)
def process(sources, output, force, force_summary):
    """Download sources and process the file to the output directory.

    \b
    SOURCES: Source JSON file or directory of files. Required.
    OUTPUT: Destination directory for generated data. Required.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] - %(message)s",
        datefmt="%H:%M:%S",
    )

    logging.getLogger("shapely.geos").setLevel(logging.WARNING)
    logging.getLogger("Fiona").setLevel(logging.WARNING)
    logging.getLogger("requests.packages.urllib3.connectionpool").setLevel(
        logging.WARNING
    )
    requests.packages.urllib3.disable_warnings()
    # logging.getLogger('processing').setLevel(logging.DEBUG)

    catalog_features = []
    failures = []
    path_parts_to_skip = utils.get_path_parts(sources).index("sources") + 1
    success = True
    for path in utils.get_files(sources):
        try:
            logging.info("Processing " + path)
            pathparts = utils.get_path_parts(path)[path_parts_to_skip:]
            pathparts[-1] = pathparts[-1].replace(".json", ".geojson")

            outdir = os.path.join(
                output, *pathparts[:-1], pathparts[-1].replace(".geojson", "")
            )
            outfile = os.path.join(output, *pathparts)

            source = utils.read_json(path)
            urlfile = urlparse(source["url"]).path.split("/")[-1]

            if not hasattr(adapters, source["filetype"]):
                logging.error("Unknown filetype " + source["filetype"])
                failures.append(path)
                continue

            read_existing = False
            if os.path.isfile(outfile):
                logging.info("Output file exists")
                if os.path.getmtime(outfile) > os.path.getmtime(path):
                    logging.info("Output file is up to date")
                    if not force:
                        read_existing = True
                        logging.warning(
                            "Skipping "
                            + path
                            + " since generated file exists. Use --force to regenerate."
                        )
                else:
                    logging.info(
                        "Output is outdated, {} < {}".format(
                            datetime.datetime.fromtimestamp(os.path.getmtime(outfile)),
                            datetime.datetime.fromtimestamp(os.path.getmtime(path)),
                        )
                    )

            if read_existing:
                with open(outfile, "rb") as f:
                    geojson = json.load(f)
                properties = geojson["properties"]
            else:
                logging.info("Downloading " + source["url"])

                try:
                    fp = utils.download(source["url"])
                except IOError:
                    logging.error("Failed to download " + source["url"])
                    failures.append(path)
                    continue

                logging.info("Reading " + urlfile)

                if "filter" in source:
                    filterer = BasicFilterer(
                        source["filter"], source.get("filterOperator", "and")
                    )
                else:
                    filterer = None

                try:
                    geojson = getattr(adapters, source["filetype"]).read(
                        fp,
                        source["properties"],
                        filterer=filterer,
                        layer_name=source.get("layerName", None),
                        source_filename=source.get("filenameInZip", None),
                    )
                except IOError as e:
                    logging.error("Failed to read " + urlfile + " " + str(e))
                    failures.append(path)
                    continue
                except zipfile.BadZipfile as e:
                    logging.error("Unable to open zip file " + source["url"])
                    failures.append(path)
                    continue
                finally:
                    os.remove(fp.name)

                if (len(geojson["features"])) == 0:
                    logging.error("Result contained no features for " + path)
                    continue

                if "mergeOn" in source:
                    geojson = merge_features(geojson, source["mergeOn"])

                # generate properties
                excluded_keys = [
                    "filetype",
                    "url",
                    "properties",
                    "filter",
                    "filenameInZip",
                ]
                properties = {
                    k: v for k, v in list(source.items()) if k not in excluded_keys
                }
                properties["source_url"] = source["url"]
                properties["feature_count"] = len(geojson["features"])
                properties["demo"] = geoutils.get_demo_point(geojson)
                geojson["properties"] = properties
                if "bbox" not in geojson:
                    geojson["bbox"] = geoutils.get_bbox_from_geojson(geojson)

                utils.make_sure_path_exists(os.path.dirname(outfile))

                # cleanup existing generated files
                if os.path.exists(outdir):
                    rmtree(outdir)
                filename_to_match, ext = os.path.splitext(pathparts[-1])
                output_file_dir = os.sep.join(utils.get_path_parts(outfile)[:-1])
                logging.info(
                    "looking for generated files to delete in " + output_file_dir
                )
                for name in os.listdir(output_file_dir):
                    base, ext = os.path.splitext(name)
                    if base == filename_to_match:
                        to_remove = os.path.join(output_file_dir, name)
                        logging.info("Removing generated file " + to_remove)
                        os.remove(to_remove)

                utils.write_json(outfile, geojson)

                logging.info("Generating label points")
                label_geojson = geoutils.get_label_points(geojson)
                label_path = outfile.replace(".geojson", ".labels.geojson")
                utils.write_json(label_path, label_geojson)

                logging.info("Done. Processed to " + outfile)

            if not "demo" in properties:
                properties["demo"] = geoutils.get_demo_point(geojson)

            properties["path"] = "/".join(pathparts)
            catalog_entry = {
                "type": "Feature",
                "properties": properties,
                "geometry": geoutils.get_union(geojson),
                "bbox": geoutils.get_bbox_from_geojson(geojson),
            }
            catalog_features.append(catalog_entry)

            if (
                force_summary
                or not read_existing
                or not os.path.exists(outdir)
                or not os.path.exists(os.path.join(outdir, "units.json"))
                or not os.path.exists(os.path.join(outdir, "source.json"))
            ):
                logging.info("Generated exploded GeoJSON to " + outdir)
                if not os.path.exists(outdir):
                    os.makedirs(outdir)
                units = []
                for feature in geojson["features"]:
                    if not "bbox" in feature:
                        feature["bbox"] = geoutils.get_bbox_from_geojson_geometry(
                            feature["geometry"]
                        )
                    feature_id = str(feature["properties"]["id"])
                    feature_id = feature_id.replace("/", "")
                    feature_filename = os.path.join(outdir, feature_id + ".geojson")
                    utils.write_json(feature_filename, feature)
                    units.append(feature["properties"])
                # source.json is just the catalog entry
                # units.json is the properties dicts from all of the units in an array
                # .json instead of .geojson, incase there is a unit named "source"
                utils.write_json(os.path.join(outdir, "source.json"), catalog_entry)
                utils.write_json(os.path.join(outdir, "units.json"), units)
            else:
                logging.debug("exploded GeoJSON already exists, not generating")

        except Exception as e:
            logging.error(str(e))
            logging.exception("Error processing file " + path)
            failures.append(path)
            success = False

    catalog = {"type": "FeatureCollection", "features": catalog_features}
    utils.write_json(os.path.join(output, "catalog.geojson"), catalog)

    if not success:
        logging.error("Failed sources: " + ", ".join(failures))
        sys.exit(-1)


if __name__ == "__main__":
    process()
