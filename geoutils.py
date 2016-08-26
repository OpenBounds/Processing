from shapely.geometry import mapping, shape, Polygon, MultiPolygon
from shapely.ops import cascaded_union

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
