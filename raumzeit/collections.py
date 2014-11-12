from .core import Location, Happening, SubHappening

# class QueryHandler(object):
#     '''
#     '''
#     def __init__(self, locations=Locations):
#         pass

class Happenings(object):

    def __init__(self, querier):
        self.querier = querier

    def filter_by_date(self, location, after, before):
        '''
        Given a location, returns all happenings for it,
        that lay in the timespan designated by the datetimes before and after.
        '''

        for h in self.querier.get_happenings(location, before, after):
            yield h

class Locations(object):

    def __init__(self, locations):
        self.locations = locations

    def set_happenings(self, happenings):
        self.happenings = happenings

    def all_active(self, after, before):
        for location in self.locations:
            if self.is_active(location, before, after):
                yield location

    def is_active(self, location, after, before):
        in_timespan = self.happenings.filter_by_date(location, before, after)
        try:
            next(in_timespan)
            return True
        except StopIteration:
            return False

