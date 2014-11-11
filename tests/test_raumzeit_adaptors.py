from nose2.tools import *
from raumzeit.core import Location, Happening, Person
from raumzeit.adaptors import HinterteilAdaptor

def test_hinterteil_adaptor():
    db = HinterteilAdaptor('http://localhost:5000/api/')
    
    locations = db.get_locations()
    first = next(locations)
    assert isinstance(first, Location)