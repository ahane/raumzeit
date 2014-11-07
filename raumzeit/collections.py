from .core import Location, Happening, Person

# class QueryHandler(object):
#     '''
#     '''
#     def __init__(self, locations=Locations):
#         pass

class Happenings(object):

    def __init__(self, querier):
        self.querier = querier

    def filter_by_date(self, location, before, after):
        '''
        Given a location, returns all happenings for it,
        that lay in the timespan designated by the datetimes before and after.
        '''

        for h in self.querier.get(location, before, after):
            yield h

class Locations(object):

    def __init__(self, locations):
        self.locations = locations

    def set_happenings(self, happenings):
        self.happenings = happenings

    def all_active(self, before, after):
        for location in self.locations:
            if self.is_active(location, before, after):
                yield location

    def is_active(self, location, before, after):
        in_timespan = self.happenings.filter_by_date(location, before, after)
        try:
            next(in_timespan)
            return True
        except StopIteration:
            return False

