import pytest
from raumzeit.core import Location, Happening, SubHappening, timespans_overlap, in_timespan
from datetime import datetime
from unittest.mock import patch
    
def test_location_init():
    props = 'Rollbergstr. 1, Berlin'
    db_info = ('venue', 42)
    l = Location('SomeLocationName', (51.1, 13.1), props, db_info)
    
    assert l.name == 'SomeLocationName'
    assert l.lat == 51.1
    assert l.lon == 13.1
    assert l.props == props
    assert l.db_info == db_info

def test_location_setters():
    props = 'Rollbergstr. 1, Berlin'
    db_info = ('venue', 42)
    l = Location('SomeLocationName', (51.1, 13.1), props, db_info)


    assert len(l.happenings) == 0
    with patch.object(Happening, 'set_location') as mock_method:
        l.add_happening(Happening('a', None, None, None, None))
    mock_method.assert_called_once_with(l)
    assert len(l.happenings) == 1
    assert l.happenings[0].name == 'a'

    assert len(l.links) == 0
    l.add_link('rel_a', 'http://ex.com')
    assert len(l.links) == 1
    assert assert l.links['rel_a'] == 'http://ex.com'
    


def test_happening_init():
    from datetime import datetime
    start = datetime(2014, 1, 1, 12)
    end = datetime(2014, 1, 1, 18)
    props = 'Some string or dict'
    db_info = ('event', 51)
    h = Happening('SomeHappName', start, end, props, db_info)
    
    assert h.name == 'SomeHappName'
    assert h.start == start
    assert h.end == end
    assert h.props == props
    assert h.db_info == db_info
    

def test_subhappening_init():
    props = 'Some Artist'
    db_info = ('artist', 80)
    start, end = None, None
    p = SubHappening('SomePersonName', start, end, props, db_info)

    assert p.name == 'SomePersonName'
    assert p.start == None
    assert p.end == None
    assert p.props  == props
    assert p.db_info == db_info

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
