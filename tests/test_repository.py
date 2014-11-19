from raumzeit.repo import Repository, NeoRepository
import py2neo
import pytest
import json
import requests

host = 'localhost'
neo_uri = 'http://{host}:7474/db/data/'.format(host=host)

def clear_db(host=host):
    resp = requests.post('http://{host}:7474/graphaware/resttest/clear'.format(host=host))
    if resp.ok:
        return True 
    else:
        raise ValueError(resp.status)

def assert_same_graph_state(cypher, host=host):
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
    assert assert_same_graph_state(create_q)


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
    

def test_casts_():
    
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

# def test_create():
#     g = py2neo.Graph(neo_uri)
#     assert g.bound

#     repo = NeoRepository(g)
#     repo.create('Location', {'name': 'renate'})



