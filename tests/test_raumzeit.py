from nose2.tools import *
from raumzeit.core import Location, Happening, Person


    
def test_location_init():
    address = 'Rollbergstr. 1, Berlin'
    desc = 'Fun Place!'
    l = Location('SomeLocationName', 51.1, 13.1, address, desc)
    
    assert l.name == 'SomeLocationName'
    assert l.lat() == 51.1
    assert l.lon() == 13.1
    assert l.address == address
    assert l.desc == desc


def test_happening_init():
    from datetime import datetime
    start = datetime(2014, 1, 1, 12)
    end = datetime(2014, 1, 1, 18)
    desc = 'Fun Party!'
    links = [('sc', 'http://somelink.com')]
    h = Happening('SomeHappName', start, end, desc, links)
    
    assert h.name == 'SomeHappName'
    assert h.start == start
    assert h.end == end
    assert h.desc == 'Fun Party!'
    assert links == links

def test_add_location_to_happening():
    h = Happening('SomeHappName', None, None, None, None)
    l = Location('SomeLocationName', None, None, None, None)

    h.set_location(l)

    assert h.location() == l

def test_person_init():
    desc = 'Some Artist'
    p = Person('SomePersonName', desc)

    assert p.name == 'SomePersonName'
    assert p.desc  == desc