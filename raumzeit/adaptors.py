from .hinterteil import Hinterteil
from .core import Location, Happening

def AbstractAdaptor(object):

    def get_locations(self):
        pass

    def get_happenings(self, location, after, before):
        pass



class HinterteilAdaptor(object):

    def __init__(self, url):
        self.db = Hinterteil(url)
        self.locations = []

    def get_locations(self):
        locations_df = self.db.get_df('venue')
        
        for i, row in locations_df.T.iteritems():
            print()
            print(row.index)
            print()
            name = row['name']
            lat, lon = row['lat'], row['lon']
            address = row['adress_string'] #TODO fix this typo in db schema
            #address = row['address_string']
            desc = row['description']
            yield Location(name, lat, lon, address, desc)