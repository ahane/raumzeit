

class Repository(object):

    def __init__(self):
        pass

    def iter_happenings(self):
        raise NotImplementedError

    def iter_happenings_timespan(self, start_stop, *args):
        raise NotImplementedError

    def iter_locations(self):
        raise NotImplementedError

    def iter_locations_join(self, other):
        raise NotImplementedError

    def iter_persons(self):
        raise NotImplementedError

    def iter_persons_join(self, other):
        raise NotImplementedError

class AlchemyRepository(Repository):

    def __init__(self, db_url, table_names):
        self.url = url
        self.table_names = table_names

#    def _get_from_table()



    def iter_happenings(self):
        pass

