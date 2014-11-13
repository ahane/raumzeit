from abc import import abstractproperty, abstractmethod, ABCMeta

# Or we simply use namedtuples:

#from collections import namedtuple

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

class Resource(metaclass=ABCMeta):
    """Abstract base class describing the hypermedia resource interface
    """
    @abstractproperty
    def links(self):
        """ a collection of (link-relation, title, url) tuples.
        """
        pass

    @abstractproperty
    def resources(self):
        """ a collection of (resource_name, resource_instance) tuples
        resource_name should be picked from the embedding perspective.
        ie. location on a happening, happenings on a location
        """
        pass

    @abstractproperty
    def props(self):
        """ 
        """
        pass


class SlugResource(Resource):

    @abstractproperty
    def slug(self):
        pass

    @abstractproperty
    def table(self):
        pass

class LayerResource(Resource):
    @abstract_property
    def db_url(self):
        pass

    @abstract_property
    def name(self):
        pass

class Location(SlugResource):
    '''
    Represents a physical point on earth.

    Methods:
      >>> location.add_happening(h)
    adds the happening and sets the location on the happening

      >>> location.add_tag(tag)
    add a tag string describing the location

      >>> location.set_link(relation, url)
    adds a hyperlink. relation is the name of the link.

      >>> location.set_property(key, value)
    adds a entry to the props dict used to carry textual information
    

    '''
    def __init__(self, name, lat_lon, props, slug_table, 
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
        self.crs = crs

        # SlugResource
        self.slug_table = slug_table
        self.slug = self.slug_table[0]
        self.table = self.slug_table[1]

        # Referencing of location specific properties
        self.props = props or {}
        self.props['name'] = self.name
        self.props['lat'] = self.lat
        self.props['lon'] = self.lon
        

        self.links = links or []
        self.happenings = happenings or []

    @property
    def resources(self): 
        return {
        'happenings': self.happenings,
        }

    def add_happening(self, happening):
        self.happenings.append(happening)
        happening.set_location(self)
        return self

    add_links


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
        self.db_info = db_info
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


