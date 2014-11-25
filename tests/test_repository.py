from raumzeit.repo import Repository, NeoRepository
import py2neo
import pytest
import json
import requests

host = 'localhost'
neo_uri = 'http://{host}:7474/db/data/'.format(host=host)
graph = py2neo.Graph(neo_uri)

def clear_db(host=host):
    resp = requests.post('http://{host}:7474/graphaware/resttest/clear'.format(host=host))
    if resp.ok:
        return True 
    else:
        raise ValueError(resp.status_code)

def is_same_graph(cypher, host=host):
    body = json.dumps({"cypher": cypher})
    headers= {'content-type': 'application/json'}
    url = 'http://{host}:7474/graphaware/resttest/assertSameGraph'.format(host=host)
    resp = requests.post(url, data=body, headers=headers)
    if resp.ok:
        return True
    else:
        raise ValueError(resp.status_code)

def test_db_testing():
    clear_db()
    g = py2neo.Graph(neo_uri)
    create_q = "CREATE (one:Person {name:'One'})-[:FRIEND_OF]->(two:Person {name:'Two'})"
    g.cypher.execute(create_q)
    assert is_same_graph(create_q)


def test_repo_signatures():
    r = Repository()
    
    with pytest.raises(NotImplementedError):
        r.get('Location', 'kater123')

    with pytest.raises(NotImplementedError):
        r.get_one('Location', {'field': 'value'})

    with pytest.raises(NotImplementedError):
        r.iter_all('Location')

    with pytest.raises(NotImplementedError):
        r.iter_joined('Location', happenings='Happening')

    with pytest.raises(NotImplementedError):    
        r.create('Location', name='renate')
    

def test_casts():
    
    node = py2neo.Node('LableA', prop_a=1, prop_b='foo')
    node_dict = NeoRepository._node_to_dict(node)

    assert len(node_dict) == 3
    assert node_dict['_label'] == 'LableA'
    assert node_dict['prop_a'] == 1
    assert node_dict['prop_b'] == 'foo'


    rec_producer = py2neo.cypher.RecordProducer(['col_a', 'col_b'])
    record = rec_producer.produce(['bar', node])
    rec_dict = NeoRepository._record_to_dict(record)

    assert len(rec_dict) == 4
    assert rec_dict['col_a'] == 'bar'
    assert rec_dict['_label'] == 'LableA'
    assert rec_dict['prop_a'] == 1
    assert rec_dict['prop_b'] == 'foo'



def test_iter_all():
    clear_db()

    
    graph.cypher.execute("""CREATE (n: Location {name: 'renate'})""")
    graph.cypher.execute("""CREATE (n: Location {name: 'kater'})""")
    graph.cypher.execute("""CREATE (n: Location {name: 'kater2'})""")
    graph.cypher.execute("""CREATE (n: Other {name: 'foobar'})""")

    repo = NeoRepository(graph)
    locations = list(repo.iter_all('Location'))
    assert len(locations) == 3
    for each in locations:
        assert len(each.items()) == 2
        assert 'name' in each.keys()
        assert each['_label'] == 'Location'


def test_get_one():
    clear_db()

    graph.cypher.execute("""CREATE (n: Location {name: 'renate'})""")
    graph.cypher.execute("""CREATE (n: Location {name: 'kater'})""")
    graph.cypher.execute("""CREATE (n: Location {name: 'kater'})""")
    graph.cypher.execute("""CREATE (n: Location {name: 'kater2'})""")
    graph.cypher.execute("""CREATE (n: Other {name: 'foobar'})""")

    repo = NeoRepository(graph)
    one = repo.get_one('Location', 'name', 'renate')
    assert one['name'] == 'renate'
    assert one['_label'] == 'Location'

    with pytest.raises(KeyError) as exc:
        one = repo.get_one('Location', 'name', 'doesnt exist')
    assert "No node" in str(exc.value)
    #one_again = repo.get_one('Location', \)


def test_get():
    clear_db()

    graph.cypher.execute("""CREATE CONSTRAINT ON (n:Location) ASSERT n.slug IS UNIQUE""")
    repo = NeoRepository(graph)
    props = {'name': 'Kater Holzig', 'desc': 'Some Text'}
    created = repo.create('Location', props)

    fetched = repo.get('Location', 'kater-holzig')

    assert created == fetched

def test_url_get():
    clear_db()

    graph.cypher.execute("""CREATE CONSTRAINT ON (n:URI) ASSERT n.url is UNIQUE""")
    graph.cypher.execute("""CREATE (n: Location {name: 'renate'})-[:IDENTIFIED_BY]->(m: URI {url:'http://someurl.com'})""")
    graph.cypher.execute("""CREATE (n: Location {name: 'kater'})-[:IDENTIFIED_BY]->(m: URI {url:'http://someurl2.com'})""")
    repo = NeoRepository(graph)

    fetched = repo.url_get('http://someurl2.com')

    assert fetched['name'] == 'kater'

    with pytest.raises(KeyError) as exc:
        repo.url_get('http://not_existing')
    assert "No node" in str(exc.value)



def test_generate_slug():
    names = ['Kater Holzig', 'Möp']
    slugs = [NeoRepository.slugify(n) for n in names]
    assert slugs == ['kater-holzig', 'mop']

    props = {'name': 'Kater Holzig', 'desc': 'Some Text'}
    slug = Repository.slugify('Kater Holzig', props)
    assert slug == 'kater-holzig-9b04172633'

def test_create():
    clear_db()
    graph.cypher.execute("""CREATE CONSTRAINT ON (n:Location) ASSERT n.slug IS UNIQUE""")
    repo = NeoRepository(graph)
    props = {'name': 'Kater Holzig', 'desc': 'Some Text'}
    created_a = repo.create('Location', props)

    assert len(created_a) == 4
    assert created_a['slug'] == 'kater-holzig'
    assert created_a['name'] == 'Kater Holzig'
    assert created_a['desc'] == 'Some Text'
    assert created_a['_label'] == 'Location'

    # check slug handling for duplicate names
    created_b = repo.create('Location', props)
    assert created_b['slug'] == 'kater-holzig-9b04172633'

def test_create_connection():
    clear_db()
    repo = NeoRepository(graph)

    graph.cypher.execute("""CREATE CONSTRAINT ON (n:Location) ASSERT n.slug IS UNIQUE""")
    graph.cypher.execute("""CREATE CONSTRAINT ON (n:Happening) ASSERT n.slug IS UNIQUE""")
    
    loc_props = {'name': 'Kater Holzig'}
    repo.create('Location', loc_props)

    event_props = {'name': 'foo party', }
    repo.create('Happening', event_props)

    repo._create_connection('Happening', 'foo-party', 'HAPPENS_AT', 'Location', 'kater-holzig')

    should_state = """CREATE 
                       (a:Happening {name:'foo party', slug: 'foo-party'})
                      -[:HAPPENS_AT]->
                      (b:Location {name:'Kater Holzig', slug: 'kater-holzig'})"""
    
    assert is_same_graph(should_state)

def test_connection_happening_location():
    clear_db()
    repo = NeoRepository(graph)

    loc_props = {'name': 'Kater Holzig'}
    loc = repo.create('Location', loc_props)

    event_props = {'name': 'foo party'}
    event = repo.create('Happening', event_props)


    with_child = repo.create_connection(event, loc)

    should_state_loc = """CREATE 
                       (a:Happening {name:'foo party', slug: 'foo-party'})
                      -[:HAPPENS_AT]->
                      (b:Location {name:'Kater Holzig', slug: 'kater-holzig'})"""
    
    assert is_same_graph(should_state_loc)
    assert with_child['slug'] == 'foo-party'
    assert with_child['location'][0]['slug'] == 'kater-holzig'

def test_connection_happening_artists():
    clear_db()
    repo = NeoRepository(graph)
    event_props = {'name': 'foo party', }
    event = repo.create('Happening', event_props)
    artist_props = {'name': 'DJ1'}
    artist = repo.create('Artist', artist_props)

    repo.create_connection(event, artist)
    should_state_artist = """CREATE 
                       (a:Happening {name:'foo party', slug: 'foo-party'})
                       -[:HOSTS]->
                       (b:Artist {name:'DJ1', slug: 'dj1'})"""
    assert is_same_graph(should_state_artist)

def test_get_joined():
    clear_db()

    repo = NeoRepository(graph)
    loc_props = {'name': 'Kater Holzig', }
    repo.create('Location', loc_props)

    event_props = {'name': 'foo party', }
    #event = repo.create('Happening', event_props, location=loc)
    repo.create('Happening', event_props)

    repo.create('Artist', {'name': 'DJ1'})
    repo.create('Artist', {'name': 'DJ2'})

    repo._create_connection('Happening', 'foo-party', 'HAPPENS_AT', 'Location', 'kater-holzig')
    repo._create_connection('Happening', 'foo-party', 'HOSTS', 'Artist', 'dj1')
    repo._create_connection('Happening', 'foo-party', 'HOSTS', 'Artist', 'dj2')
    

    joined_loc = repo.get_joined('Happening', 'foo-party', 'location')
    assert joined_loc['slug'] =='foo-party'
    assert len(joined_loc['location']) == 1
    assert joined_loc['location'][0]['slug'] == 'kater-holzig'

    joined_art = repo.get_joined('Happening', 'foo-party', 'artists')
    assert joined_loc['slug'] =='foo-party'
    assert len(joined_art['artists'])  == 2
    artist_slugs = set([a['slug'] for a in joined_art['artists']])
    assert 'dj1' in artist_slugs
    assert 'dj2' in artist_slugs

    # Next up!
    #joined_both = repo.get_joined('Happening', 'foo-party', 'artists', 'location')
    
    
    