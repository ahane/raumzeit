from raumzeit import repo 
from .util import clear_db, is_same_graph, has_sub_graph, graph
import py2neo
import pytest
from datetime import datetime


loc_props = {'name': 'Kater Holzig'}
artist_props = {'name': 'DJ1'}
address = {'lat': 51.1, 'lon': 13.1, 'string': 'Somestreet. 1'}
links = [{'name': 'SomeRel', 'url': 'http://someurl.com'}, {'name': 'OtherRel', 'url': 'http://otherurl.com'}]

@pytest.fixture
def neorepo(graph):
	return repo.NeoRepository(graph)

@pytest.fixture
def locations(neorepo):
	return repo.LocationCollection(neorepo)

@pytest.fixture
def artists(neorepo):
	return repo.ArtistCollection(neorepo)

def test_init_(graph, neorepo):
	repo.LocationCollection(neorepo)

def test_create_loc(locations):

	loc = locations.create(loc_props, address, links)
	assert has_sub_graph("""CREATE (n: Location {name: 'Kater Holzig', slug: 'kater-holzig'})
							-[:LOCATED_AT]->(m: Address {lat: '51.1', lon: '13.1', string: 'Somestreet. 1'})""")

	assert has_sub_graph("""CREATE (o: URI {name: 'SomeRel', url: 'http://someurl.com'})
                        <-[:IDENTIFIED_BY]-
                        (n: Location {name: 'Kater Holzig', slug: 'kater-holzig'})
                        -[:IDENTIFIED_BY]->
                        (m: URI {name: 'OtherRel', url: 'http://otherurl.com'})""")

	assert 'name' and 'slug' and 'address' in loc
	assert loc['_label'] == 'Location'
	assert loc['slug'] == 'kater-holzig'
	assert loc['address']['string'] == 'Somestreet. 1'

def test_get_loc(locations):
    locations.create(loc_props, address, links)

    loc = locations.get('kater-holzig')

    assert 'name' and 'slug' and 'address' in loc
    assert loc['_label'] == 'Location'
    assert loc['slug'] == 'kater-holzig'
    assert loc['address']['string'] == 'Somestreet. 1'
    assert len(loc['links']) == 2
    assert loc['links'][0]['name'] == 'SomeRel' or loc['links'][0]['name'] == 'OtherRel'	

def test_get_loc_url(locations):
	locations.create(loc_props, address, links)
	loc = locations.url_get('http://otherurl.com')
	assert loc == locations.get('kater-holzig')


def test_create_artist(artists):
	artist = artists.create(artist_props, links)

	assert has_sub_graph("""CREATE (o: URI {name: 'SomeRel', url: 'http://someurl.com'})
                        <-[:IDENTIFIED_BY]-
                        (n: Artist {name: 'DJ1', slug: 'dj1'})
                        -[:IDENTIFIED_BY]->
                        (m: URI {name: 'OtherRel', url: 'http://otherurl.com'})""")

	assert 'name' and 'slug' in artist
	assert artist['_label'] == 'Artist'
	assert artist['slug'] == 'dj1'
	

def test_get_artist(artists):
    artists.create(artist_props, links)

    artist = artists.get('dj1')

    assert 'name' and 'slug' in artist
    assert artist['_label'] == 'Artist'
    assert artist['slug'] == 'dj1'
    assert len(artist['links']) == 2
    assert artist['links'][0]['name'] == 'SomeRel' or artist['links'][0]['name'] == 'OtherRel'	

def test_get_artist_url(artists):
	artists.create(artist_props, links)
	artist = artists.url_get('http://otherurl.com')
	assert artist == artists.get('dj1')
