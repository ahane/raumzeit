

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

# class BaseResource(metaclass=ABCMeta):
#     """Abstract base class describing the hypermedia resource interface
#     """
#     @property
#     @abstractmethod
#     def links(self):
#         """ a collection of (link-relation, title, url) tuples.
#         """
#         pass
    
#     @property
#     @abstractmethod
#     def slug(self):
#         pass



class Location(object):
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
    def __init__(self, name, lat, lon, slug, db_info,
                    links=None, happenings=None):
        """ Create a geographic location.

            :param name: name or title representative of the location
            :param lat: geographic latitude
            :param lon: geographic longnitude according to WSG 84
            :param db_info: dict of strings to locate this in database
            :param links: dict of {'rel': 'http://example.com'} hyperlinks
            :param happenings: list of happenings this location is hosting
            

        """  
        # Public fields    
        self.name = name
        self.lat, self.lon = lat, lon
        self.happenings = happenings or []

        # Public fields with special meaning
        self.slug = slug
        self.links = links or {}

        # Private fields
        self._db_info = db_info

    def add_happening(self, happening):
        self.happenings.append(happening)
        happening.set_location(self)   

            
class Happening(object):
    '''
    Represents a happening or event taking place at a Location.
    '''

    def __init__(self, name, start, end, slug, db_info,
                     links=None, location=None, persons=None):

        self.name = name
        self.start = start
        self.end = end
        self.set_location(location)
        self.slug = slug
        self.links = links or {}
        self.persons = persons or []
        self._db_info = db_info


    def set_location(self, location):
        """Sets the location of the happenings.
        Should this call add_happening() on the location?
        The reverse is true
        """
        self.location = location


class Person(object):
    '''
    Represents a happening or event taking place at a Location.
    '''

    def __init__(self, name, slug, db_info,
                     links=None, happenings=None):

        self.name = name
        self.slug = slug
        self._db_info = db_info
        self.links = links or {}
        self.happenings = happenings or []
        
