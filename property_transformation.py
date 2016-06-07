from types import UnicodeType, StringType

class PropertyMappingFailedException(Exception):
    pass

def get_transformed_properties(source_properties, prop_map):
    results = {}
    for key, value in prop_map.iteritems():
        if type(value) in (StringType, UnicodeType):
            if value in source_properties:
                results[key] = source_properties[value]
            else:
                raise PropertyMappingFailedException("property %s not found in source feature" % 
                    (value))
        elif type(value) == dict:
            if "static" in value:
                results[key] = value["static"]
        else:
            raise PropertyMappingFailedException("Unhandled mapping for key:%s value type:%s" % 
                (key, type(value)))
    return results
