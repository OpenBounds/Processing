from shapely.geometry import mapping
from shapely.geometry import shape
from shapely.ops import cascaded_union

import geoutils


def merge_features(geojson, merge_field):
    """ Merge features based on matching properties

    :param geojson: A GeoJSON feature collection containing Polygons or MultiPolygons
    :returns: A new GeoJSON Feature collection containing Polygons or MultiPolygons
    """
    output_features = []

    input_features = geojson["features"]
    while len(input_features) > 0:
        feature = input_features.pop()
        shapes = []
        shapes.append(shape(feature["geometry"]))

        to_merge = [
            f
            for f in input_features
            if f["properties"][merge_field] == feature["properties"][merge_field]
        ]
        if len(to_merge) == 0:
            output_features.append(feature)
            continue

        largest = None
        for f in to_merge:
            input_features.remove(f)
            shapes.append(shape(f["geometry"]))
            if largest is None or f.area > largest.area:
                largest = f

        # Fix invalid geometries
        shapes = [s.buffer(0.0) if not s.is_valid else s for s in shapes]

        try:
            result = cascaded_union(shapes)
        except Exception as e:
            # workaround for geos bug with cacscaded_union sometimes failing
            logger.error("cascaded_union failed, falling back to union")
            result = shapes.pop()
            for s in shapes:
                result = result.union(s)

        result_feature = {
            "type": "Feature",
            "properties": largest["properties"],
            "geometry": mapping(result),
        }
        result_feature["bbox"] = geoutils.get_bbox_from_geojson_feature(result_feature)
        result_feature["properties"]["acres"] = geoutils.get_area_acres(
            feature["geometry"]
        )
        if "id" in result_feature["properties"]:
            result_feature["id"] = result_feature["properties"]["id"]

        output_features.append(result_feature)

    output = {"type": "FeatureCollection", "features": output_features}
    if "properties" in geojson:
        output["properties"] = geojson["properties"]
    return output
