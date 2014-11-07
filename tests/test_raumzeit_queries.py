from nose2.tools import *
from raumzeit.core import Location, Happening, Person
from raumzeit.collections import Happenings, Locations

class FakeQuerier(object):
        def get(self, location, after, before):
            h = {'A': [Happening('H1', None, None, None,), Happening('H2', None, None, None),
                        Happening('H3', None, None, None), Happening('H3', None, None, None)],
                 'B': []}
            return h[location]

def test_locations_is_active():                
    querier = FakeQuerier()
    happenings = Happenings(querier)
    locations = Locations(['A', 'B'])
    locations.set_happenings(happenings)
    assert locations.is_active('A', None, None) == True
    assert locations.is_active('B', None, None) == False
    
def test_locations_all_active():                
    querier = FakeQuerier()
    happenings = Happenings(querier)
    locations = Locations(['A', 'B'])
    locations.set_happenings(happenings)

    active = list(locations.all_active(None, None))
    assert active == ['A']
    
