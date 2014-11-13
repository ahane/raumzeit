from nose2.tools import *
from raumzeit.core import Location, Happening, SubHappening, in_timespan
from raumzeit.filters import TimeAwareLayer
from datetime import datetime

class FakeEntityGenerator():
    locs = [Location('a', (None, None), None, None),
            Location('b', (None, None), None, None),
            Location('c', (None, None), None, None)]
    
    haps = {locs[0]: [Happening('jan. A', datetime(2014, 1, 1), datetime(2014, 1, 31), None, None)],

            locs[1]: [Happening('jan. B', datetime(2014, 1, 1), datetime(2014, 1, 31), None, None),
                   Happening('feb. B', datetime(2014, 2, 1), datetime(2014, 2, 28), None, None)],

            locs[2]: [Happening('feb. C', datetime(2014, 2, 1), datetime(2014, 2, 28), None, None)]}

    subhaps = {}
    for l, happenings in haps.items():
        for h in happenings:
            subhaps[h] = []
            subhaps[h].append(SubHappening(h.name+'-a', None, None, None, None))
            subhaps[h].append(SubHappening(h.name+'-b', None, None, None, None))

            #every happening gets two subhappenings with consecutive numbers
            

    def all_locations(self):
        for l in self.locs:
            yield l
    
    def active_happenings(self, loc, start, end):
        for h in self.haps[loc]:
            if in_timespan(h, start, end):
                yield h

    def subhappenings(self, happ):
        subhaps = self.subhaps[happ]
        for subhap in subhaps:
            yield subhap


def test_timeawarelayer_init():
    entity_generators = FakeEntityGenerator()
    timelayer = TimeAwareLayer(entity_generators)
    start = datetime(2014, 1, 1)
    end = datetime(2014, 1, 20)
    timelayer.set_timespan(start, end)

    assert timelayer.start == start
    assert timelayer.end == end
    assert timelayer._entity_generators == entity_generators

    

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

# def test_happenings():

#     entity_generator = FakeEntityGenerator()
#     timelayer = TimeAwareLayer(entity_generator)
    
#     start_a = datetime(2014, 1, 1)
#     end_a = datetime(2014, 1, 20)
#     timelayer.set_timespan(start_a, end_a)

#     active_haps = list(timelayer._all_active_happenings())
#     assert len(active_haps) == 2
#     assert active_haps[0].name == 'jan. A'
#     assert active_haps[1].name == 'jan. B'

#     hap_a = active_haps[0]
#     subhaps_a = list(timelayer._subhappenings(hap_a))
#     assert len(subhaps_a) == 2
#     assert subhaps_a[0].name == 'jan. A-a'
#     assert subhaps_a[1].name == 'jan. A-b'

#     hap_b = active_haps[1]
#     subhaps_b = list(timelayer._subhappenings(hap_b))
#     assert len(subhaps_b) == 2
#     assert subhaps_b[0].name == 'jan. B-a'
#     assert subhaps_b[1].name == 'jan. B-b'

#     #Test the tupelized version of the two functions above
#     active_locations = list(timelayer._active_locations())
#     haps_subs = [list(timelayer._active_happenings_subs(l)) for l in active_locations]
#     assert len(haps_subs) == 2
#     hap_subs_a = haps_subs[0]
#     hap_subs_b = haps_subs[1]
    
#     assert type(hap_subs_a) == list
#     assert len(hap_subs_a) == 1
    
#     assert type(hap_subs_b) == list
#     assert len(hap_subs_b) == 1

#     hap_sub_a = hap_subs_a[0]
#     hap_sub_b = hap_subs_b[0]

#     assert type(hap_sub_a) == tuple
#     assert type(hap_sub_b) == tuple

#     assert hap_sub_a[0].name == 'jan. A'
#     assert hap_sub_b[0].name == 'jan. B'

#     subs_a = list(hap_sub_a[1])
#     subs_b = list(hap_sub_b[1])
#     assert len(subs_a) == 2
#     assert subs_a[0].name == 'jan. A-a'
#     assert subs_a[1].name == 'jan. A-b'

#     assert len(subs_b) == 2
#     assert subs_b[0].name == 'jan. B-a'
#     assert subs_b[1].name == 'jan. B-b'

