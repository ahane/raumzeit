from raumzeit import repo 
from .util import clear_db, is_same_graph, has_sub_graph, graph
import py2neo
import pytest
from datetime import datetime


loc_props = {'name': 'Kater Holzig'}
artist_props = {'name': 'DJ1'}
work_props = {'name': 'some work'}
happ_props = {'name': 'some party'}
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

@pytest.fixture
def works(neorepo):
	return repo.WorkCollection(neorepo)

@pytest.fixture
def timeline(graph):
	return repo.Timeline(graph)

@pytest.fixture
def happenings(neorepo, timeline):
	return repo.HappeningCollection(neorepo, timeline)

@pytest.fixture
def happenings_filled(neorepo, timeline, locations, artists):

	happenings = repo.HappeningCollection(neorepo, timeline)
	artist = artists.create(make_props('artist'), [make_link('artist')])
	location = locations.create(make_props('loc'), address, [make_link('loc')])

	happenings.create(make_dt(1, 1, 13), make_dt(1, 1, 15), make_props('hap_a'), location, [artist], [make_link('happ_a')] )
	happenings.create(make_dt(1, 1, 14), make_dt(1, 1, 16), make_props('hap_b'), location, [artist], [make_link('happ_b')] )
	happenings.create(make_dt(1, 1, 18), make_dt(1, 1, 20), make_props('hap_c'), location, [artist], [make_link('happ_c')] )
	return happenings

def make_link(url, name='foo'):
	return {'name': name, 'url': url}

def make_props(name):
	return {'name': name}

def make_dt(m, d, h, mi=0):
	return datetime(2014, m, d, h, mi)

def test_validation(locations):
	collection = locations
	props = {'name': 'foo'}
	mandatory = {'props': ['name']}
	collection._validate(mandatory, props=props)
	with pytest.raises(ValueError):
		collection._validate(mandatory, props={'bad': 'props'})

	mandatory = {'props': ['name'], 'links': ['name', 'url']}
	links = [{'name': 'foo', 'url': 'bar'}, {'name': 'foo', 'url': 'bar'}]
	collection._validate(mandatory, props=props, links=links)
	with pytest.raises(ValueError):
		collection._validate(mandatory, props=props, links=[{'name': 'aaa'}])

	mandatory = {'props': ['name'], 'links': ['name', 'url'], 'entity': ['name', ('_label', 'FOO')]}
	entity = {'name': 'bert', '_label': 'FOO'}
	collection._validate(mandatory, props=props, links=links, entity=entity)
	with pytest.raises(ValueError):
		collection._validate(mandatory, props=props, links=links, entity={'name': 'bert', '_label': 'BAR'})




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
	assert loc['slug'] == locations.get('kater-holzig')['slug']


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
	assert artist['slug'] == artists.get('dj1')['slug']

def test_create_work(works, artists):
	artist = artists.create(artist_props, [{'name':'foo', 'url':'bar'}])
	work = works.create(work_props, artist, links)
	assert has_sub_graph("""CREATE (o: URI {name: 'SomeRel', url: 'http://someurl.com'})
                        <-[:IDENTIFIED_BY]-
                        (n: Work {name: 'some work', slug: 'some-work'})
                        -[:IDENTIFIED_BY]->
                        (m: URI {name: 'OtherRel', url: 'http://otherurl.com'})""")

	assert has_sub_graph("""CREATE (o: Artist {name: 'DJ1', slug: 'dj1'})
                        <-[:MADE_BY]-
                        (n: Work {name: 'some work', slug: 'some-work'})""")

	assert 'name' and 'slug' in work
	assert work['_label'] == 'Work'
	assert work['slug'] == 'some-work'
	
	with pytest.raises(ValueError) as exc:
		works.create(work_props, {'slug': 'aaa', '_label': 'wrong'}, [{'name': '', 'url':''}])
	assert 'Key _label should be Artist' in str(exc.value)


def test_get_work(works, artists):
    artist = artists.create(artist_props, [{'name':'foo', 'url':'bar'}])
    works.create(work_props, artist, links)
    
    work = works.get('some-work')
    assert 'name' and 'slug' in work
    assert work['_label'] == 'Work'
    assert work['slug'] == 'some-work'
    assert work['artist']['slug'] == 'dj1'
    assert len(work['links']) == 2
    assert work['links'][0]['name'] == 'SomeRel' or work['links'][0]['name'] == 'OtherRel'	

def test_get_work_url(works, artists):
    artist = artists.create(artist_props, [{'name':'foo', 'url':'bar'}])
    works.create(work_props, artist, links)
    work = works.url_get('http://otherurl.com')
    assert work['slug'] == works.get('some-work')['slug']


def test_create_happening(happenings, artists, locations):
	artist_a = artists.create(artist_props, [{'name':'foo', 'url':'bar'}])
	artist_b = artists.create({'name': 'DJ2'}, [{'name':'foo', 'url':'baz'}])
	loc = locations.create(loc_props, address, [{'name': 'foo', 'url': 'bar2'}])
	start = datetime(2014, 1, 1, 14, 20)
	stop = datetime(2014, 1, 1, 15, 30)
	happening = happenings.create(start, stop, happ_props, loc, [artist_a, artist_b], links)

	assert has_sub_graph("""CREATE (o: URI {name: 'SomeRel', url: 'http://someurl.com'})
                        <-[:IDENTIFIED_BY]-
                        (n: Happening {name: 'some party', slug: 'some-party'})
                        -[:IDENTIFIED_BY]->
                        (m: URI {name: 'OtherRel', url: 'http://otherurl.com'})""")

	assert has_sub_graph("""CREATE (m: Location {name: 'Kater Holzig', slug: 'kater-holzig'})
							<-[:HAPPENS_AT]-(n: Happening {name: 'some party', slug: 'some-party'})""")

	assert has_sub_graph("""CREATE (m: Artist {name: 'DJ1', slug: 'dj1'})
							<-[:HOSTS]-(n: Happening {name: 'some party', slug: 'some-party'})
							-[:HOSTS]->(o: Artist {name: 'DJ2', slug: 'dj2'})""")

	assert has_sub_graph("""CREATE (m: Artist {name: 'DJ1', slug: 'dj1'})
							<-[:HOSTS]-(n: Happening {name: 'some party', slug: 'some-party'})
							-[:HOSTS]->(o: Artist {name: 'DJ2', slug: 'dj2'})""")

	assert has_sub_graph("""CREATE (m: Timespan {start: '2014-01-01T14:20:00', stop: '2014-01-01T15:30:00'})
							<-[:ACTIVE_DURING]-(n: Happening {name: 'some party', slug: 'some-party'})
							MERGE (o: Hour {start: '2014-01-01T14:00:00'})
							<-[:OVERLAPS]-(m)-[:OVERLAPS]->
							(p: Hour {start: '2014-01-01T15:00:00'})""")


	assert 'name' and 'slug' in happening
	assert happening['_label'] == 'Happening'
	assert happening['slug'] == 'some-party'
	
	with pytest.raises(ValueError) as exc:
		happening = happenings.create(start, stop, happ_props, {'slug': 'bert', '_label':'WRONG'}, [artist_a, artist_b], links)
	assert 'Key _label should be Location' in str(exc.value)

	with pytest.raises(ValueError) as exc:
		happening = happenings.create(start, stop, happ_props, loc, [{'slug': 'hans', '_label':'WRONG'}, artist_b], links)
	assert 'Key _label should be Artist' in str(exc.value)


def test_create_multiple_happenings(happenings, artists, locations):
	artist = artists.create(make_props('artist_a'), [make_link('artist_a')])
	location = locations.create(make_props('loc'), address, [make_link('loc')])

	happenings.create(make_dt(1, 1, 13), make_dt(1, 1, 15), make_props('hap_a'), location, [artist], [make_link('happ_a')] )
	happenings.create(make_dt(1, 1, 14), make_dt(1, 1, 16), make_props('hap_b'), location, [artist], [make_link('happ_b')] )
	happenings.create(make_dt(1, 1, 18), make_dt(1, 1, 20), make_props('hap_c'), location, [artist], [make_link('happ_c')] )

	assert has_sub_graph("""CREATE (m: Timespan {start: '2014-01-01T13:00:00', stop: '2014-01-01T15:00:00'})
							<-[:ACTIVE_DURING]-(n: Happening {name: 'hap_a', slug: 'hap-a'})""")

	assert has_sub_graph("""CREATE (m: Timespan {start: '2014-01-01T14:00:00', stop: '2014-01-01T16:00:00'})
							<-[:ACTIVE_DURING]-(n: Happening {name: 'hap_b', slug: 'hap-b'})""")

	assert has_sub_graph("""CREATE (m: Timespan {start: '2014-01-01T18:00:00', stop: '2014-01-01T20:00:00'})
							<-[:ACTIVE_DURING]-(n: Happening {name: 'hap_c', slug: 'hap-c'})""")




def test_get_happening(happenings, locations, artists):
    artist_a = artists.create(artist_props, [{'name':'foo', 'url':'bar'}])
    artist_b = artists.create({'name': 'DJ2'}, [{'name':'foo', 'url':'baz'}])
    loc = locations.create(loc_props, address, [{'name': 'foo', 'url': 'bar2'}])
    start = datetime(2014, 1, 1, 14, 20)
    stop = datetime(2014, 1, 1, 15, 30)
    happenings.create(start, stop, happ_props, loc, [artist_a, artist_b], links)
  
    happ = happenings.get('some-party')
    assert 'name' and 'slug' in happ
    assert happ['_label'] == 'Happening'
    assert happ['slug'] == 'some-party'
    assert happ['time']['start'] == '2014-01-01T14:20:00'
    assert happ['time']['stop'] == '2014-01-01T15:30:00'

    artists = happ['artists']
    assert len(artists) == 2
    assert artists[0]['name'] == artist_props['name'] or artists[1]['name'] == artist_props['name']
    
    assert happ['location']['slug'] == 'kater-holzig'

    assert len(happ['links']) == 2
    assert happ['links'][0]['name'] == 'SomeRel' or happ['links'][0]['name'] == 'OtherRel'	

def test_get_happening_url(happenings, locations, artists):
    artist_a = artists.create(artist_props, [{'name':'foo', 'url':'bar'}])
    artist_b = artists.create({'name': 'DJ2'}, [{'name':'foo', 'url':'baz'}])
    loc = locations.create(loc_props, address, [{'name': 'foo', 'url': 'bar2'}])
    start = datetime(2014, 1, 1, 14, 20)
    stop = datetime(2014, 1, 1, 15, 30)
    happenings.create(start, stop, happ_props, loc, [artist_a, artist_b], links)

    happ = happenings.url_get('http://otherurl.com')
    assert happ['slug'] == happenings.get('some-party')['slug']


def test_get_happening_timespan(happenings_filled):

	happenings = happenings_filled
	# happenings.create(make_dt(1, 1, 13), make_dt(1, 1, 15), make_props('hap_a'), location, [artist], [make_link('happ_a')] )
	# happenings.create(make_dt(1, 1, 14), make_dt(1, 1, 16), make_props('hap_b'), location, [artist], [make_link('happ_b')] )
	# happenings.create(make_dt(1, 1, 18), make_dt(1, 1, 20), make_props('hap_c'), location, [artist], [make_link('happ_c')] )
	#assert len(list(happenings.iter_timeframe(make_dt(1, 1, 13), make_dt(1, 1, 15)))) == 2
	#assert len(list(happenings.iter_timeframe(make_dt(1, 1, 15), make_dt(1, 1, 17)))) == 2
	#assert len(list(happenings.iter_timeframe(make_dt(1, 1, 17), make_dt(1, 1, 22)))) == 1

	last = list(happenings.iter_timeframe(make_dt(1, 1, 17), make_dt(1, 1, 22)))[0]
	assert last['slug'] == 'hap-c'
	
	assert last['time']['start'] == '2014-01-01T18:00:00'
	assert last['time']['stop'] == '2014-01-01T20:00:00'
	assert last['location']['slug'] == 'loc'
	assert last['location']['_label'] == 'Location'

