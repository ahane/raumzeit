

# Or we simply use namedtuples:

from collections import namedtuple

Location = namedtuple('Location', ['name', 'lat', 'lon', 'details', 'dbinfo'])
Happening = namedtuple('Happening', ['name', 'start', 'end', 'details', 'dbinfo'])
SubHappening = namedtuple('SubHappening', ['name', 'start', 'end', 'details', 'dbinfo'])



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



# class Location(object):
#     '''
#     Represents a physical location on earth.
#     '''
#     def __init__(self, name, lat, lon, address=None, desc=None, 
#                     crs=None, links=[], happenings=[]):
        
#         self.name = name
#         self._lat = lat
#         self._lon = lon
#         self.crs = crs
#         self.address = address
#         self.desc = desc

#     def lat(self):
#         return self._lat

#     def lon(self):
#         return self._lon


# class Happening(object):
#     '''
#     Represents a happening or event taking place at a Location.
#     '''

#     def __init__(self, name, start, end, desc=None, links=[], location=None):

#         self.name = name
#         self.start = start
#         self.end = end
#         self.desc = desc
#         self.links = links
#         self._location = location

#     def set_location(self, location):
#         self._location = location

#     def location(self):
#         return self._location

# class Person(object):
#     '''
#     Represents some kind of entity in the real world that can 
#     partake in an happening.
#     '''

#     def __init__(self, name, desc=None, links=[]):

#         self.name = name
#         self.desc = desc
#         self.links = links


