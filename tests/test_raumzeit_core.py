import pytest
from raumzeit.core import Location, Happening, Person, timespans_overlap, in_timespan
from datetime import datetime
from unittest.mock import patch
    
def init_location():
    name = 'SomeName'
    lat = 51.1
    lon = 13.1
    slug = 'some_name'
    db_info = {'table': 'venue'}
    l = Location(name, lat, lon, slug, db_info)
    return l
def test_location_init():  
    l = init_location()
    assert l.name == 'SomeName'
    assert l.lat == 51.1
    assert l.lon == 13.1
    assert l.links == {}
    # Test the BaseResource implementation
    assert l.slug == 'some_name'
    assert l._db_info == {'table': 'venue'}
    
def test_assingment_location():
    l = init_location()
    # we can assing any random attribute we want
    l.address = 'Some street 1'
    assert l.address == 'Some street 1'

    # we can add to the dicts
    l.links['resident-advisor'] = 'http://ra.com/somesite'
    assert l.links['resident-advisor'] == 'http://ra.com/somesite'

def test_add_happening_to_location():
    l = init_location()
    h = Happening('n', 'start', 'stop', 'slug', 'dbinfo')
    l.add_happening(h)
    
    assert len(l.happenings) == 1
    assert list(l.happenings)[0].name == 'n'





    


def test_happening_init():
    from datetime import datetime
    start = datetime(2014, 1, 1, 12)
    end = datetime(2014, 1, 1, 18)
    slug = 'some_event'
    db_info = {'table': 'event'}
    h = Happening('SomeHappName', start, end, slug, db_info)
    
    assert h.name == 'SomeHappName'
    assert h.start == start
    assert h.end == end
    assert h.slug == 'some_event'
    assert h.location == None
    assert h.links == {}
    assert h._db_info == {'table': 'event'}
    

def test_person_init():
    
    slug = 'some_artist'
    db_info = {'table': 'artist'}
    p = Person('SomePersonName', slug, db_info)

    assert p.name == 'SomePersonName'
    assert p.slug  == slug
    assert p._db_info == db_info
    assert p.happenings == []
    assert p.links == {}

def test_timepans_overlap():

    jan = (datetime(2014, 1, 1), datetime(2014, 1, 31))
    feb = (datetime(2014, 2, 1), datetime(2014, 2, 28))

    in_jan = (datetime(2014, 1, 7), datetime(2014, 1, 14))
    in_feb = (datetime(2014, 2, 7), datetime(2014, 2, 14))
    in_both = (datetime(2014, 1, 15), datetime(2014, 1, 15))
    before_both = (datetime(2013, 12, 1), datetime(2013, 12, 31))
    after_both = (datetime(2014, 3, 1), datetime(2014, 3, 31))

    assert timespans_overlap(jan, feb) == False
    assert timespans_overlap(feb, jan) == False

    assert timespans_overlap(jan, in_jan) == True
    assert timespans_overlap(in_jan, jan) == True

    assert timespans_overlap(feb, in_feb) == True
    assert timespans_overlap(in_feb, feb) == True

    assert timespans_overlap(before_both, feb) == False
    assert timespans_overlap(jan, before_both) == False

    assert timespans_overlap(after_both, feb) == False
    assert timespans_overlap(jan, after_both) == False

def test_timepans_overlap_exceptions():

    corrupt_jan = (datetime(2014, 1, 31), datetime(2014, 1, 1))
    feb = (datetime(2014, 2, 1), datetime(2014, 2, 28))

    with pytest.raises(ValueError):
        timespans_overlap(corrupt_jan, feb)

def test_in_timespan():
    a = Happening('a', datetime(2014, 1, 1), datetime(2014, 1, 7), None, None)
    b = Happening('b', datetime(2014, 1, 25), datetime(2014, 2, 13), None, None)

    jan = (datetime(2014, 1, 1), datetime(2014, 1, 31))
    feb = (datetime(2014, 2, 1), datetime(2014, 2, 28))

    assert in_timespan(a, jan[0], jan[1]) == True
    assert in_timespan(a, feb[0], feb[1]) == False
    assert in_timespan(b, jan[0], jan[1]) == True
    assert in_timespan(b, feb[0], feb[1]) == True
