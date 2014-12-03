from raumzeit.repo import Repository, NeoRepository, HappeningCollection
from .util import clear_db, is_same_graph, graph
import py2neo
import pytest

def test_db_testing(graph):
    clear_db()
    create_q = "CREATE (one:Person {name:'One'})-[:FRIEND_OF]->(two:Person {name:'Two'})"
    graph.cypher.execute(create_q)
    assert is_same_graph(create_q)


def test_repo_signatures(graph):
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
    

def test_casts(graph):
    
    node = py2neo.Node('LableA', prop_a=1, prop_b='foo')
    node_dict = NeoRepository._node_to_dict(node)

    assert len(node_dict) == 3
    assert node_dict['_label'] == 'LableA'
    assert node_dict['prop_a'] == 1
    assert node_dict['prop_b'] == 'foo'


    rec_producer = py2neo.cypher.RecordProducer(['col_a', 'col_b', 'sub_'])
    record = rec_producer.produce(['bar', node, node])
    rec_dict = NeoRepository._record_to_dict(record)

    assert len(rec_dict) == 5
    assert rec_dict['col_a'] == 'bar'
    assert rec_dict['_label'] == 'LableA'
    assert rec_dict['prop_a'] == 1
    assert rec_dict['prop_b'] == 'foo'
    assert rec_dict['sub']['prop_a'] == 1



def test_subgraph_to_collection(graph):
    clear_db()
    graph.cypher.execute("""""")

def test_iter_all(graph):
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


def test_get_one(graph):
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


def test_get(graph):
    clear_db()

    graph.cypher.execute("""CREATE CONSTRAINT ON (n:Location) ASSERT n.slug IS UNIQUE""")
    repo = NeoRepository(graph)
    props = {'name': 'Kater Holzig', 'desc': 'Some Text'}
    created = repo.create('Location', props)

    fetched = repo.get('Location', 'kater-holzig')

    assert created == fetched

def test_url_get(graph):
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



def test_generate_slug(graph):
    names = ['Kater Holzig', 'Möp']
    slugs = [NeoRepository.slugify(n) for n in names]
    assert slugs == ['kater-holzig', 'mop']

    props = {'name': 'Kater Holzig', 'desc': 'Some Text'}
    slug = Repository.slugify('Kater Holzig', props)
    assert slug == 'kater-holzig-9b04172633'

def test_create_node(graph):
    clear_db()
    repo = NeoRepository(graph)
    props = {'desc': 'Some Text', 'foo': 'bar'}
    created_node = repo._create_node('Foobar', props)
    assert created_node.properties == props
    assert len(created_node.labels) == 1
    assert list(created_node.labels)[0] == 'Foobar'
    assert created_node.bound

def test_create_entity(graph):
    clear_db()
    graph.cypher.execute("""CREATE CONSTRAINT ON (n:Location) ASSERT n.slug IS UNIQUE""")
    repo = NeoRepository(graph)
    props = {'name': 'Kater Holzig', 'desc': 'Some Text'}
    created_entity = repo._create_entity('Location', props)

    assert created_entity.properties['slug'] == 'kater-holzig'
    assert created_entity.properties['name'] == 'Kater Holzig'
    assert created_entity.properties['desc'] == 'Some Text'
    labels = list(created_entity.labels)
    assert len(labels) == 1
    assert labels[0] == 'Location'

    should_state = """CREATE (a:Location {name: 'Kater Holzig', desc: 'Some Text', slug: 'kater-holzig'})"""
    assert is_same_graph(should_state)
    # check slug handling for duplicate names
    created_entity_b = repo._create_entity('Location', props)
    assert created_entity_b['slug'] == 'kater-holzig-9b04172633'

def test_create_entity_with_links(graph):
    clear_db()
    repo = NeoRepository(graph)
    props = {'name': 'Kater Holzig'}
    links = {'SomeRel': 'http://someurl.com', 'OtherRel': 'http://otherurl.com'}
    repo._create_entity('Location', props, links)


    should_state = """CREATE (o: URI {name: 'SomeRel', url: 'http://someurl.com'})
                        <-[:IDENTIFIED_BY]-
                        (n: Location {name: 'Kater Holzig', slug: 'kater-holzig'})
                        -[:IDENTIFIED_BY]->
                        (m: URI {name: 'OtherRel', url: 'http://otherurl.com'})"""
    assert is_same_graph(should_state)

def test_create_connection(graph):
    clear_db()
    repo = NeoRepository(graph)

    #graph.cypher.execute("""CREATE CONSTRAINT ON (n:Location) ASSERT n.slug IS UNIQUE""")
    #graph.cypher.execute("""CREATE CONSTRAINT ON (n:Happening) ASSERT n.slug IS UNIQUE""")
    
    loc_props = {'name': 'Kater Holzig'}
    loc = repo._create_entity('Location', loc_props)

    event_props = {'name': 'foo party', }
    event = repo._create_entity('Happening', event_props)

    rel = repo._create_connection(event, 'HAPPENS_AT', loc)
    print(rel.start_node)
    print(rel.end_node)
    should_state = """CREATE 
                       (a:Happening {name:'foo party', slug: 'foo-party'})
                      -[:HAPPENS_AT]->
                      (b:Location {name:'Kater Holzig', slug: 'kater-holzig'})"""
    
    assert is_same_graph(should_state)

def test_connection_happening_location(graph):
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

def test_connection_happening_artists(graph):
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

def test_get_joined(graph):
    clear_db()

    repo = NeoRepository(graph)
    graph.cypher.execute("""CREATE 
                       (a:Happening {name:'foo party', slug: 'foo-party'})
                       -[:HAPPENS_AT]->
                       (b:Location {name:'Kater Holzig', slug: 'kater-holzig'})""")

    graph.cypher.execute("""MATCH 
                       (a:Happening {slug: 'foo-party'})
                       MERGE
                       (a)-[:HOSTS]->
                       (b:Artist {name:'DJ1', slug: 'dj1'})""")

    graph.cypher.execute("""MATCH
                       (a:Happening {slug: 'foo-party'})
                       MERGE
                       (a)-[:HOSTS]->
                       (b:Artist {name:'DJ2', slug: 'dj2'})""")


    joined_loc = repo.get_joined('Happening', 'foo-party', 'HAPPENS_AT', 'location')
    assert joined_loc['slug'] =='foo-party'
    assert len(joined_loc['location']) == 1
    assert joined_loc['location'][0]['slug'] == 'kater-holzig'

    joined_art = repo.get_joined('Happening', 'foo-party', 'HOSTS', 'artists')
    assert joined_loc['slug'] =='foo-party'
    assert len(joined_art['artists'])  == 2
    artist_slugs = set([a['slug'] for a in joined_art['artists']])
    assert 'dj1' in artist_slugs
    assert 'dj2' in artist_slugs

    # Next up!
    #joined_both = repo.get_joined('Happening', 'foo-party', 'artists', 'location')
    
def test_return_joined(graph):
    parent_node = py2neo.Node('LableA', prop_a=1, prop_b='foo')
    child_node_a = py2neo.Node('LableB', prop_a=2, prop_b='bar')
    child_node_b = py2neo.Node('LableB', prop_a=3, prop_b='baz')

    repo = NeoRepository(graph)
    dct = repo.return_joined(parent_node, child_node_a, 'rel', single_child=True)
    assert dct['prop_a'] == 1
    assert dct['prop_b'] == 'foo'
    assert dct['rel']['prop_a'] == 2
    assert dct['rel']['prop_b'] == 'bar'

    dct = repo.return_joined(parent_node, [child_node_b, child_node_a], 'rel')
    assert dct['prop_a'] == 1
    assert dct['prop_b'] == 'foo'
    children = dct['rel']
    assert len(children) == 2
    assert children[0]['prop_a'] == 2 and children[1]['prop_a'] == 3 or children[1]['prop_a'] == 2 and children[0]['prop_a'] == 3
# def test_happenings():
#     clear_db()
#     happs = HappeningCollection(graph)
#     happs.create('a', 'b', {'name': 'foo party'})

#     should_state = """CREATE (a:Happening {name:'foo party', slug: 'foo-party'})
#                              -[:ACTIVE_TURING]->(b:Timespan {start: '2014-11-25T18:30:00', stop: '2014-11-25T22:30:00'})
#                              -[:STARTS_IN]->(c: Hour {start: '2014-11-25T18:00:00'})"""
#     assert is_same_graph(should_state)
