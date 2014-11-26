from raumzeit.repo import Repository, NeoRepository, Timeline
from .util import clear_db, is_same_graph, has_sub_graph, graph
import py2neo
import pytest
from datetime import datetime


def test_init():
	clear_db()
	tl = Timeline(graph)

	graph.cypher.execute("""CREATE (n: Hour {start: '2014-11-25T18:00:00'}) """)
	with pytest.raises(py2neo.error.GraphError) as exc:
		graph.cypher.execute("""CREATE (n: Hour {start: '2014-11-25T18:00:00'}) """)
	assert "already exists" in str(exc.value)

	assert len(tl.index.labels) == 1
	assert list(tl.index.labels)[0] == 'HourIndex'
	assert tl.index.bound

def test_create_hour():
	clear_db()
	
	tl = Timeline(graph)
	assert is_same_graph("""CREATE (n: HourIndex) """)
	hour = tl._create_hour(datetime(2014, 1, 1, 12))
	print(hour)
	assert is_same_graph("""CREATE (n: HourIndex)-[:LATEST]->(m: Hour {start: '2014-01-01T12:00:00'}) """)
	assert hour.bound 
	labels = list(hour.labels)
	assert len(labels) == 1
	assert labels[0] == 'Hour'

def test_find_latest():
	clear_db()
	tl = Timeline(graph)
	graph.cypher.execute("""MATCH (n: HourIndex) 
							MERGE (n)-[:LATEST]->(m: Hour {start: '2014-01-01T12:00:00'}) """)
	latest = tl.latest
	assert latest.properties['start'] == '2014-01-01T12:00:00'

def test_create_multiple_hours():
	clear_db()

	tl = Timeline(graph)
	tl._create_hour(datetime(2014, 1, 1, 12))
	tl._append_hours(datetime(2014, 1, 1, 14))
	
	 # MERGE (n: Hour {start: '2014-01-01T12:00:00'})
		# 	 -[:NEXT]->(m: Hour {start: '2014-01-01T13:00:00'})
		#      -[:NEXT]->(o: Hour {start: '2014-01-01T14:00:00'})<-[:LATEST]-(i)"""

	target = """CREATE (i: HourIndex)
			  MERGE (n: Hour {start: '2014-01-01T12:00:00'})
			 -[:NEXT]->(m: Hour {start: '2014-01-01T13:00:00'})
		     -[:NEXT]->(o: Hour {start: '2014-01-01T14:00:00'})<-[:LATEST]-(i)"""
	assert is_same_graph(target)

def test_set_latest():
	clear_db()
	tl = Timeline(graph)
	graph.cypher.execute("""MATCH (n: HourIndex)
							MERGE (n)-[:LATEST]->(m: Hour {start: '2014-01-01T12:00:00'}) """)
	new_latest, = graph.create(py2neo.Node('Hour', start='2014-01-01T13:00:00'))
	
	tl._set_latest(new_latest)

	assert tl.latest == new_latest
	#assert has_sub_graph("""CREATE (n: HourIndex)-[:LATEST]->(m: Hour {start: '2014-01-01T13:00:00'}) """)



def test_hour_range():

	target_range = [datetime(2014, 1, 1, 12), datetime(2014, 1, 1, 13), datetime(2014, 1, 1, 14)]
	range_a = Timeline._hour_range(datetime(2014, 1, 1, 12), datetime(2014, 1, 1, 14))
	assert range_a == target_range

	range_b = Timeline._hour_range(datetime(2014, 1, 1, 12), len_range=3)
	assert range_b == target_range

def test_format_datetime():
	tl = Timeline(graph)
	dt1 = datetime(2014, 1, 1, 12)
	dt2 = datetime(2014, 1, 1, 12, 30)
	dt3 = datetime(2014, 1, 1, 12, 0, 11)

	dstring = '2014-01-01T12:00:00'

	assert tl._date_to_string(dt1) == dstring
	assert tl._date_to_string(dt2) == dstring
	assert tl._date_to_string(dt3) == dstring


	assert tl._string_to_date(dstring) == dt1