from types import UnicodeType, StringType

class FilteringFailedException(Exception):
    pass

class BasicFilterer(object):
    """Filter that reads in a dictionary and filters based on feature properties"""
    def __init__(self, filter_def):
        super(BasicFilterer, self).__init__()
        self.filter_def = filter_def
        self.operator = filter_def.get('operator', 'and')
        if 'operator' in filter_def:
            del filter_def['operator']

    def keep(self, feature):
        for key, value in self.filter_def.iteritems():
            if type(value) in (StringType, UnicodeType):
                if value == "not null":
                    test_success = key in feature['properties'] and \
                        feature['properties'][key] is not None
                else:
                    raise FilteringFailedException("Unhandled filtering operator key:%s value:%s" % 
                        (key, value))
            else:
                raise FilteringFailedException("Unhandled filtering for key:%s value type:%s" % 
                    (key, type(value)))

            if self.operator == 'and' and not test_success:
                return False
            elif self.operator == 'or' and test_success:
                return True
        return True
