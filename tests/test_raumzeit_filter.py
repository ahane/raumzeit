from nose2.tools import *
from raumzeit.core import Location, Happening, SubHappening, in_timespan
from raumzeit.filters import TimeAwareLayer
from datetime import datetime

class FakeEntityGenerator():
    locs = [Location('a', None, None, None, None),
            Location('b', None, None, None, None),
            Location('c', None, None, None, None)]
    
    haps = {locs[0]: [Happening('jan. A', datetime(2014, 1, 1), datetime(2014, 1, 31), None, None)],

            locs[1]: [Happening('jan. B', datetime(2014, 1, 1), datetime(2014, 1, 31), None, None),
                   Happening('feb. B', datetime(2014, 2, 1), datetime(2014, 2, 28), None, None)],

            locs[2]: [Happening('feb. C', datetime(2014, 2, 1), datetime(2014, 2, 28), None, None)]}

    def all_locations(self):
        for l in self.locs:
            yield l
    
    def active_happenings(self, loc, start, end):
        for h in self.haps[loc]:
            if in_timespan(h, start, end):
                yield h


def test_timeawarelayer_init():
    entity_generator = FakeEntityGenerator()
    timelayer = TimeAwareLayer(entity_generator)
    start = datetime(2014, 1, 1)
    end = datetime(2014, 1, 20)
    timelayer.set_timespan(start, end)

    assert timelayer.start == start
    assert timelayer.end == end
    assert timelayer._entity_generator == entity_generator

    

def test_timeawarelayer_timespan_change():
    entity_generator = FakeEntityGenerator()
    timelayer = TimeAwareLayer(entity_generator)
    
    start_a = datetime(2014, 1, 1)
    end_a = datetime(2014, 1, 20)
    timelayer.set_timespan(start_a, end_a)

    all_locs = list(timelayer._all_locations())
    assert len(all_locs) == 3
    assert all_locs[0].name == 'a'
    assert all_locs[1].name == 'b'
    assert all_locs[2].name == 'c'

    active_locs = list(timelayer._active_locations())
    assert len(active_locs) == 2
    assert active_locs[0].name == 'a'
    assert active_locs[1].name == 'b'

    start_b = datetime(2014, 2, 10)
    end_b = datetime(2014, 2, 20)
    timelayer.set_timespan(start_b, end_b)

    all_locs = list(timelayer._all_locations())
    assert len(all_locs) == 3
    assert all_locs[0].name == 'a'
    assert all_locs[1].name == 'b'
    assert all_locs[2].name == 'c'

    active_locs = list(timelayer._active_locations())
    assert len(active_locs) == 2
    assert active_locs[0].name == 'b'
    assert active_locs[1].name == 'c'