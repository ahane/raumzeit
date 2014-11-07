import hinterteil
from .core import Location, Happening

def AbstractAdaptor(object):

    def get_locations(self):
        pass

    def get_happenings(self, location, after, before):
        pass



def HinterteilAdaptor(object):

    def __init__(self, url):
        self.db = hinterteil.Hinterteil(url)

    def get_locations(self):
        locations_df = self.db.get_df('venue')
        for i, row in locations_df.iteritems():
            name = row['name']
            yield 