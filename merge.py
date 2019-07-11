from shapely.geometry import mapping
from shapely.geometry import shape
from shapely.ops import cascaded_union

import geoutils


def merge_features(geojson, merge_field, properties_key="properties"):
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
            if f[properties_key][merge_field] == feature[properties_key][merge_field]
        ]
        if len(to_merge) == 0:
            output_features.append(feature)
            continue

        largest = None
        for f in to_merge:
            input_features.remove(f)
            f_geom = shape(f["geometry"])
            shapes.append(f_geom)
            if largest is None or f_geom.area > largest["geometry"].area:
                largest = {"geometry": f_geom, "properties": f["properties"]}

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
        output_features.append(result_feature)

    output = {"type": "FeatureCollection", "features": output_features}
    if "properties" in geojson:
        output["properties"] = geojson["properties"]
    return output
