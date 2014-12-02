import py2neo
import requests
import json
import pytest
HOST = 'localhost'
neo_uri = 'http://{host}:7474/db/data/'.format(host=HOST)
#graph = py2neo.Graph(neo_uri)
@pytest.fixture
def graph():
    clear_db()
    return py2neo.Graph(neo_uri)

def clear_db(host=HOST):
    resp = requests.post('http://{host}:7474/graphaware/resttest/clear'.format(host=host))
    if resp.ok:
        return True 
    else:
        raise ValueError(resp.status_code)

def is_same_graph(cypher, host=HOST):
    body = json.dumps({"cypher": cypher})
    headers= {'content-type': 'application/json'}
    url = 'http://{host}:7474/graphaware/resttest/assertSameGraph'.format(host=host)
    resp = requests.post(url, data=body, headers=headers)
    if resp.ok:
        return True
    else:
        raise ValueError(resp.status_code)

def has_sub_graph(cypher, host=HOST):
    body = json.dumps({"cypher": cypher})
    headers= {'content-type': 'application/json'}
    url = 'http://{host}:7474/graphaware/resttest/assertSubgraph'.format(host=host)
    resp = requests.post(url, data=body, headers=headers)
    if resp.ok:
        return True
    else:
        raise ValueError(resp.status_code)