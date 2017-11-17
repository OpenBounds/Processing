from shapely.geometry import mapping, shape, Polygon, MultiPolygon, GeometryCollection
from shapely.ops import cascaded_union, transform
import utils
import logging
from functools import partial
import pyproj

def get_union(geojson):
    """ Returns a geojson geometry that is the union of all features in a geojson feature collection """
    shapes = []
    for feature in geojson['features']:
        if feature['geometry']['type'] not in ['Polygon', 'MultiPolygon']:
            continue

        s = shape(feature['geometry'])
        if s and not s.is_valid:
            s = s.buffer(0.0)
            if not s.is_valid:
                logging.error("Invalid geometry in get_union, failed to fix")
            else: 
                logging.error("Invalid geometry in get_union. Fixed.")
        if s and s.is_valid:
            #get rid of holes
            if type(s) in (MultiPolygon, GeometryCollection):
                hulls = [Polygon(r.exterior) for r in s.geoms]
                hull = MultiPolygon(hulls)
            else:
                hull = Polygon(s.exterior)

            #simplify so calculating union doesnt take forever
            simplified = hull.simplify(0.01, preserve_topology=True)
            if simplified.is_valid:
                shapes.append(simplified)
            else:
                shapes.append(hull)

    try:
        result = cascaded_union(shapes)
    except Exception, e:
        #workaround for geos bug with cacscaded_union sometimes failing
        logging.error("cascaded_union failed, falling back to union")
        result = shapes.pop()
        for s in shapes:
            result = result.union(s)

    #get rid of holes
    if type(result) in (MultiPolygon, GeometryCollection):
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


def get_label_points(geojson, use_polylabel=True):
    """ Generate label points for polygon features 

    :param geojson: A GeoJSON feature collection contain Polygons or MultiPolygons
    :returns: A new GeoJSON Feature collection containing Point features
    """
    if use_polylabel:
        try:
            from shapely.algorithms.polylabel import polylabel
        except:
            utils.error("Polylabel not available, using centroid for label points")
            polylabel = None
    else:
        utils.error("using centroid for label points, Polylabel disabled")
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
                project = partial(
                    pyproj.transform,
                    pyproj.Proj(init='epsg:4326'),
                    pyproj.Proj(init='epsg:3857'))
                geometry_3857 = transform(project, geometry)
                label_geometry_3857 = polylabel(geometry_3857)
                project = partial(
                    pyproj.transform,
                    pyproj.Proj(init='epsg:3857'),
                    pyproj.Proj(init='epsg:4326'))
                label_geometry = transform(project, label_geometry_3857)  # apply projection
            else:
                label_geometry = geometry.centroid

            if label_geometry:
                f = {
                    'type': 'Feature',
                    'geometry': mapping(label_geometry),
                    'properties': feature['properties']
                }
                label_features.append(f)

    return {
        "type": "FeatureCollection",
        "features": label_features
    }
