class PropertyMappingFailedException(Exception):
    pass


def get_transformed_properties(source_properties, prop_map):
    results = {}
    for key, value in prop_map.items():
        if isinstance(value, str):
            if value in source_properties:
                results[key] = source_properties[value]
            else:
                raise PropertyMappingFailedException(
                    "property %s not found in source feature" % (value)
                )
        elif isinstance(value, dict):
            if "static" in value:
                results[key] = value["static"]
            elif "mapping" in value:
                if not "key" in value:
                    raise PropertyMappingFailedException(
                        "Found mapping, but no key specified to map"
                    )
                source_value = source_properties[value["key"]]
                if source_value is None:
                    source_value = "null"
                if source_value in value["mapping"]:
                    results[key] = value["mapping"][source_value]
                else:
                    raise PropertyMappingFailedException(
                        "value:%s not found in mapping for key:%s" % (source_value, key)
                    )
            else:
                raise PropertyMappingFailedException(
                    "Failed to find key for mapping in dict for field:%s" % (key,)
                )
        else:
            raise PropertyMappingFailedException(
                "Unhandled mapping for key:%s value type:%s" % (key, type(value))
            )
    return results
