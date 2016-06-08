from types import UnicodeType, StringType
import re

class FilteringFailedException(Exception):
    pass

class BasicFilterer(object):
    """Filter that reads in a dictionary and filters based on feature properties"""
    def __init__(self, filter_def, filter_operator):
        super(BasicFilterer, self).__init__()
        self.filter_def = filter_def
        self.operator = filter_operator
        if not self.operator in ['and', 'or']:
            raise FilteringFailedException("Unknown filter operator:%s" % 
                filter_operator)

    def keep(self, feature):
        for item in self.filter_def:
            if not 'expression' in item:
                raise FilteringFailedException("Expression missing from filter definition" % 
                        (key, value))
            if item['expression'] == "not null":
                test_success = item['key'] in feature['properties'] and \
                    feature['properties'][item['key']] is not None
            elif item['expression'] == "=":
                test_success = item['key'] in feature['properties'] and \
                    feature['properties'][item['key']] == item['value']
            elif item['expression'] == "!=":
                test_success = item['key'] not in feature['properties'] or \
                    feature['properties'][item['key']] != item['value']
            elif item['expression'] == "match":
                test_success = item['key'] in feature['properties'] and \
                    re.match(item['value'], feature['properties'][item['key']])
            else:
                raise FilteringFailedException("Unhandled filtering expression:%s" % 
                    (item['expression']))

            if self.operator == 'and' and not test_success:
                return False
            elif self.operator == 'or' and test_success:
                return True
        
        if self.operator == 'or':
            return False
        else:
            return True
