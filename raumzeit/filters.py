from .core import Location, Happening, SubHappening

class TimeAwareLayer(object):
    """Aggregates locations which have happenings in a set timespan.

    Parameters: 
    entity_generator: a object that exposes the methods: all_locations(), active_happenings().
    """
    def __init__(self, entity_generator):
        self._entity_generator = entity_generator

    def set_timespan(self, start, end):
        """Sets the current timespan and reloads the happenings."""
        self.start = start
        self.end = end
        
    def _all_locations(self):
        """Iterate over all locations in layer."""
        for location in self._entity_generator.all_locations():
            yield location

    def _active_happenings(self, location):
        for happening in self._entity_generator.active_happenings(location, self.start, self.end):
            yield happening

    def _active_locations(self):
        """Iterate over locations that have happenings in set timespan."""
        for location in self._all_locations():
            happenings = self._active_happenings(location)
            try:
                h = next(happenings)
                yield location
            except StopIteration:
                pass

