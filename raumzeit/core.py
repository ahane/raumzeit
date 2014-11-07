

class Location(object):
    '''
    Represents a physical location on earth.
    '''
    def __init__(self, name, lat, lon, address=None, desc=None, 
                    crs=None, links=[], happenings=[]):
        
        self.name = name
        self._lat = lat
        self._lon = lon
        self.crs = crs
        self.address = address
        self.desc = desc

    def lat(self):
        return self._lat

    def lon(self):
        return self._lon


class Happening(object):
    '''
    Represents a happening or event taking place at a Location.
    '''

    def __init__(self, name, start, end, desc=None, links=[], location=None):

        self.name = name
        self.start = start
        self.end = end
        self.desc = desc
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


