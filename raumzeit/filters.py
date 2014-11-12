from .core import Location, Happening, SubHappening

class TimeAwareLayer(object):
    """Aggregates locations which have happenings in a set timespan.

    Parameters: 
    entity_generator: a object that exposes the methods: all_locations(), active_happenings().
    """
    def __init__(self, entity_generators):
        self._entity_generators = entity_generators

    def set_timespan(self, start, end):
        """Sets the current timespan and reloads the happenings."""
        self.start = start
        self.end = end
        
    



    # Wrappers of entity_generators 
    def _all_locations(self):
        """Iterate over all locations in layer."""
        for location in self._entity_generators.all_locations():
            yield location

    def _active_happenings(self, location):
        """Iterate over happenings that match set timespan and belong to passed location."""
        active_happenings = self._entity_generators.active_happenings(location, self.start, self.end)
        for happening in active_happenings:
            yield happening

    def _subhappenings(self, happening):
        """Iterate over subhappenings of a passed happening."""
        for subhappening in self._entity_generators.subhappenings(happening):
            yield subhappening

    # Additional logic

    def active_locations_happenings(self):
        """Iterate over (location, happenings_subs) tuples.
        happenings_subs is a generator of (happening, subhappenings) tuples.
        subhappenings is a generator of subhappenings.
        """
        pass
        for location in self._active_locations():
            for happening in self._active_happenings(location):
                subhappenings = self._subhappenings(happening)
                yield (happening, subhappenings)

    def _active_happenings_subs(self, location):
        for happening in self._active_happenings(location):
            subhappenings = self._subhappenings(happening)
            yield (happening, subhappenings)
    # def _all_active_happenings_subs(self):
    #     """Iterate over (happening, subhappenings) tuples.
    #     subhappenings is a generator of subhappenings.
    #     """
    #     for happening in self._all_active_happenings():
    #         subhappenings = self._subhappenings(happening)
    #         yield (happening, subhappenings)

    def _all_active_happenings(self):
        """Iterate over happenings that match set timespan."""
        for location in self._all_locations():
            for happening in self._active_happenings(location):
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

