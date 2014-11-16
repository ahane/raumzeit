# from nose2.tools import *
# from raumzeit.core import Location, Happening, Person, in_timespan
# from raumzeit.filters import TimeAwareLayer
# from datetime import datetime

# class FakeEntityGenerator():
# """this is our db gateway"""


#     haps = [Happening('party_a', datetime(2014, 1, 1), datetime(2014, 1, 2), 'party_a', 'dbinfo'),
#             Happening('party_b', datetime(2014, 6, 21), datetime(2014, 6, 22), 'party_b', 'dbinfo'),
#             Happening('party_c', datetime(2014, 2, 1), datetime(2014, 2, 28), 'party_c', 'dbinfo'),
#             Happening('party_d', datetime(2014, 2, 6), datetime(2014, 2, 8), 'party_d', 'dbinfo')]

#     locs = [Location('a', 51.1, 3.1, 'kater', 'dbinfo'),
#             Location('b', 51.1, 3.1, 'renate', 'dbinfo'),
#             Location('c', 51.1, 3.1, 'kater2', 'dbinfo')]
#     locs_dict = {haps[0]: locs[0], 
#                  haps[1]: locs[0], 

#                  haps[2]: locs[1], 

#                  haps[3]: locs[2]}

#     persons_dict = {}
#     for h in haps:
#             persons[h] = []
#             persons[h].append(Person(h.name+'-dj1', h.name+'-dj1', 'dbinfo'))
#             persons[h].append(Person(h.name+'-dj2', h.name+'-dj2', 'dbinfo'))

#             #every happening gets two subhappenings with consecutive numbers
            
    
#     def all_active_happenings(self, start, end):
#         for h in self.haps:
#             if in_timespan(h, start, end):
#                 yield h

#     def fetch_persons(self, happening):
#         persons = self.persons_dict[happening]
#         for p in persons:
#             yield p

#     def fetch_location(self, happening):
#         return locs_dict[happening]

# def test_timeawarelayer_init():
#     entity_generators = FakeEntityGenerator()
#     timelayer = TimeAwareLayer(entity_generators)
#     start = datetime(2014, 1, 1)
#     end = datetime(2014, 1, 20)
    
#     # our input interface, implemented by timelayer
#     timelayer.add_output()  
#     timelayer.set_timespan(start, end)

#     # we add an object that implements our output interface
#     # the output interface has
    

#     assert timelayer.start == start
#     assert timelayer.end == end
#     assert timelayer._entity_generators == entity_generators

#     active_happenings = entity_generators.all_active_happenings()
# def test_timaware_fetching():



# def test_timeawarelayer_timespan_change():
#     entity_generator = FakeEntityGenerator()
#     timelayer = TimeAwareLayer(entity_generator)
    
#     start_a = datetime(2014, 1, 1)
#     end_a = datetime(2014, 1, 20)
#     timelayer.set_timespan(start_a, end_a)

    


