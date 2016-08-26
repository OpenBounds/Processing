from shapely.geometry import mapping, shape, Polygon, MultiPolygon
from shapely.ops import cascaded_union
import utils

def get_union(geojson):
    """ Returns a geojson geometry that is the union of all features in a geojson feature collection """
    shapes = []
    for feature in geojson['features']:
        if feature['geometry']['type'] not in ['Polygon', 'MultiPolygon']:
            continue

        s = shape(feature['geometry'])
        if s and s.is_valid:
            #simplify so calculating union doesnt take forever
            simplified = s.simplify(0.01, preserve_topology=True)
            if simplified.is_valid:
                shapes.append(simplified)
            else:
                shapes.append(s)

    result = cascaded_union(shapes)
    #get rid of holes
    if type(result) == MultiPolygon:
        hulls = [Polygon(r.exterior) for r in result.geoms]
        hull = MultiPolygon(hulls)
    else:
        hull = Polygon(result.exterior)
    
    return mapping(hull)


def polygon_from_bbox(bbox):
    """ Generate a polygon geometry from a ESWN bouding box

    :param bbox: a 4 float bounding box
    :returns: a polygon geometry
    """
    return [[
        [bbox[0], bbox[1]],
        [bbox[2], bbox[1]],
        [bbox[2], bbox[3]],
        [bbox[0], bbox[3]],
        [bbox[0], bbox[1]]
    ]]


def add_label_points(geojson):
    """ Add a point feature to a geojson feature collection to be used a label point for the feature """
    try:
        from shapely.algorithms.polylabel import polylabel
    except:
        utils.error("Polylabel not available, using centroid for label points")
        polylabel = None

    label_features = []
    for feature in geojson['features']:
        if feature['geometry']['type'] not in ['Polygon', 'MultiPolygon']:
            continue

        feature_geometry = shape(feature['geometry'])

        if type(feature_geometry) == MultiPolygon:
            geometries = feature_geometry.geoms
        else:
            geometries = [feature_geometry]

        for geometry in geometries:
            if polylabel and geometry.is_valid: #polylabel doesnt work on invalid geometries, centroid does
                label_geometry = polylabel(geometry)
            else:
                label_geometry = geometry.centroid

            if label_geometry:
                f = {
                    'type': 'Feature',
                    'geometry': mapping(label_geometry),
                    'properties': feature['properties']
                }
                label_features.append(f)

    geojson['features'].extend(label_features)
    return geojson