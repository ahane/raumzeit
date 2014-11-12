

# Or we simply use namedtuples:

from collections import namedtuple

# Location = namedtuple('Location', ['name', 'lat', 'lon', 'details', 'dbinfo'])
# Happening = namedtuple('Happening', ['name', 'start', 'end', 'details', 'dbinfo'])
# SubHappening = namedtuple('SubHappening', ['name', 'start', 'end', 'details', 'dbinfo'])



def in_timespan(happening, start, end):
    """Check if a happening overlaps with a timespan."""

    return timespans_overlap((happening.start, happening.end), (start, end))


def timespans_overlap(start_end, other_start_end):
    """Check if two timespans overlap."""

    s1, e1 = start_end
    s2, e2 = other_start_end

    try:
        assert s1 < e1
        assert s2 < e2
    except AssertionError:
        raise ValueError('Timespans must begin before they end.')

    return s1 < e2 and s2 < e1



class Location(object):
    '''
    Represents a physical point on earth.

    Methods:
      >>> location.add_happening(h)
    adds the happening and sets the location on the happening

      >>> location.add_link(relation, url)
    adds a hyperlink. relation is the name of the link.

      >>> location.add_tag(tag)
    add a tag string describing the location

    '''
    def __init__(self, name, lat_lon, props, db_info, 
                    crs='WGS 84', links=None, happenings=None, tags=None):
    """ Create a geographic location.

        :param name: name or title representative of the location
        :param lat_lon: float tuple of latitude and longitude coordinates
        :param props: dict of strings further describing the location.
            Example: {'address': 'Somestree. 1', 'copy': 'Some text'
        :param db_info: dict of strings to locate this in database
        :param crs: coordinate reference system. WSG 84 is the one used by GPS
        :param links: dict of {'rel': 'http://example.com'} hyperlinks
        :param happenings: list of happenings this location is hosting
        :param tags: set of strings that act as search and explorability tags

    """

        
        self.name = name
        self.lat_lon = lat_lon
        self.lat = self.lat_lon[0]
        self.lon = self.lat_lon[1]

        self.props = props
        self.db_info = db_info
        
        self.crs = crs

        self.links = links or []
        self.happenings = happenings or []

        def add_property(self, key, value):




class Happening(object):
    '''
    Represents a happening or event taking place at a Location.
    '''

    def __init__(self, name, start, end, props, db_info,
                     links=None, location=None, tags=None):

        self.name = name
        self.start = start
        self.end = end
        self.props = props
        self.db_info = db_info            

        self.links = links or []
        self.location = location or []

    def set_location(self, location):
        self.location = location

    def location(self):
        return self._location

class SubHappening(object):
    '''
    Represents a happening or event taking place at a Location.
    '''

    def __init__(self, name, start, end, props, db_info,
                     links=[], location=None):

        self.name = name
        self.start = start
        self.end = end
        self.props = props
        self.links = links
        self._location = location

    def set_location(self, location):
        self._location = location

    def location(self):
        return self._location

class Person(object):
    '''
    Represents some kind of entity in the real world that can 
    partake in an happening.
    '''

    def __init__(self, name, desc=None, links=[]):

        self.name = name
        self.desc = desc
        self.links = links


