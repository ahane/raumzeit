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
        raise ValueError(resp.status)

def is_same_graph(cypher, host=host):
    body = json.dumps({"cypher": cypher})
    headers= {'content-type': 'application/json'}
    url = 'http://{host}:7474/graphaware/resttest/assertSameGraph'.format(host=host)
    resp = requests.post(url, data=body, headers=headers)
    if resp.ok:
        return True
    else:
        raise ValueError(resp.status)

def test_db_testing():
    clear_db()
    g = py2neo.Graph(neo_uri)
    create_q = "CREATE (one:Person {name:'One'})-[:FRIEND_OF]->(two:Person {name:'Two'})"
    g.cypher.execute(create_q)
    assert is_same_graph(create_q)


def test_repo_signatures():
    r = Repository()
    
    with pytest.raises(NotImplementedError):
        r.get('location', 'kater123')

    with pytest.raises(NotImplementedError):
        r.get_one('location', 'some query')

    with pytest.raises(NotImplementedError):
        r.iter_all('location')

    with pytest.raises(NotImplementedError):
        happenings = None
        r.iter_joined('location', happenings)

    with pytest.raises(NotImplementedError):    
        r.create('location', name='renate')
    

def test_casts():
    
    node = py2neo.Node('LableA', 'LableB', prop_a=1, prop_b='foo')
    node_dict = NeoRepository._node_to_dict(node)

    assert len(node_dict) == 3
    assert node_dict['labels'] == {'LableA', 'LableB'}
    assert node_dict['prop_a'] == 1
    assert node_dict['prop_b'] == 'foo'


    rec_producer = py2neo.cypher.RecordProducer(['col_a', 'col_b'])
    record = rec_producer.produce(['bar', node])
    rec_dict = NeoRepository._record_to_dict(record)

    assert len(rec_dict) == 4
    assert rec_dict['col_a'] == 'bar'
    assert rec_dict['labels'] == {'LableA', 'LableB'}
    assert rec_dict['prop_a'] == 1
    assert rec_dict['prop_b'] == 'foo'

def test_create():
    clear_db()
    repo = NeoRepository(graph)
    
    #repo.create('Location', {'name': 'renate'})

    #assert is_same_graph("""CREATE (l:Location {name: 'renate'})""")

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
        assert each['labels'] == {'Location'}


def test_compile_query():
    d = {'a': 123, 'b': 'foo'}
    s = NeoRepository._dict_to_param_q(d)

    # our dict is accessed randomly so we test both orderings
    assert (s == """{a: {a}, b: {b}}""" or s == """{b: {b}, a: {a}}""")

def test_get_one():
    clear_db()

    graph.cypher.execute("""CREATE (n: Location {name: 'renate'})""")
    graph.cypher.execute("""CREATE (n: Location {name: 'kater'})""")
    graph.cypher.execute("""CREATE (n: Location {name: 'kater'})""")
    graph.cypher.execute("""CREATE (n: Location {name: 'kater2'})""")
    graph.cypher.execute("""CREATE (n: Other {name: 'foobar'})""")

    repo = NeoRepository(graph)
    one = repo.get_one('Location', {'name': 'renate'})
    assert one['name'] == 'renate'
    assert one['labels'] == {'Location'}


    with pytest.raises(KeyError) as e:
        two = repo.get_one('Location', {'name': 'kater'})
    assert 'Not unique' in str(e.value)

    with pytest.raises(KeyError) as e:
        two = repo.get_one('Location', {'name': 'notexisting'})
    assert 'Not found' in str(e.value)

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
    assert created_a['labels'] == {'Location'}

    # check slug handling for duplicate names
    created_b = repo.create('Location', props)
    assert created_b['slug'] == 'kater-holzig-9b04172633'


def test_get():
    clear_db()

    graph.cypher.execute("""CREATE CONSTRAINT ON (n:Location) ASSERT n.slug IS UNIQUE""")
    repo = NeoRepository(graph)
    props = {'name': 'Kater Holzig', 'desc': 'Some Text'}
    created = repo.create('Location', props)

    fetched = repo.get('Location', 'kater-holzig')

    assert created == fetched
