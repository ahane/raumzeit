from raumzeit import repo 
from .util import clear_db, is_same_graph, has_sub_graph, graph
import py2neo
import pytest
from datetime import datetime


start = datetime(2014, 1, 1, 18)
stop = datetime(2014, 1, 1, 22)


@pytest.fixture
def neorepo(graph):
	return repo.NeoRepository(graph)

def test_init_(graph, neorepo):
	repo.LocationCollection(neorepo)

def test_create(neorepo):
	locations = repo.LocationCollection(neorepo)
	props = {'name': 'Kater Holzig'}
	address = {'lat': 51.1, 'lon': 13.1, 'string': 'Somestreet. 1'}

	loc = locations.create(props, address)
	assert is_same_graph("""CREATE (n: Location {name: 'Kater Holzig', slug: 'kater-holzig'})
							-[:LOCATED_AT]->(m: Address {lat: '51.1', lon: '13.1', string: 'Somestreet. 1'})""")

	assert 'name' and 'slug' and 'address' in loc
	assert loc['_label'] == 'Location'
	assert loc['slug'] == 'kater-holzig'
	assert loc['address']['string'] == 'Somestreet. 1'