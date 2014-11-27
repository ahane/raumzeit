from raumzeit.repo import Repository, NeoRepository, Timeline
from .util import clear_db, is_same_graph, has_sub_graph, graph
import py2neo
import pytest
from datetime import datetime


def test_cold_init():
	clear_db()

	# Cold Init
	tl = Timeline(graph)
	assert is_same_graph("""CREATE (n: HourIndex) """)
	assert len(tl.index.labels) == 1
	assert list(tl.index.labels)[0] == 'HourIndex'
	assert tl.index.bound
	assert tl.earliest is None
	assert tl.latest is None

	
def test_warm_init():
	clear_db()
	graph.cypher.execute("""CREATE (i: HourIndex)
		                    MERGE (i)-[:EARLIEST]->(n: Hour {start: '2014-11-25T18:00:00'})
		                   -[:NEXT]->(m: Hour {start: '2014-11-25T19:00:00'})<-[:LATEST]-(i) """)
	tl = Timeline(graph)
	#Test Unique Constraint
	# with pytest.raises(py2neo.error.GraphError) as exc:
	# 	graph.cypher.execute("""CREATE (n: Hour {start: '2014-11-25T18:00:00'}) """)
	# assert "already exists" in str(exc.value)

	assert len(tl.index.labels) == 1
	assert list(tl.index.labels)[0] == 'HourIndex'
	assert tl.index.bound
	print(tl.latest)
	assert tl.earliest.properties['start'] == '2014-11-25T18:00:00'
	assert tl.latest.properties['start'] == '2014-11-25T19:00:00'

def test_init_hour():
	clear_db()
	
	tl = Timeline(graph)
	assert is_same_graph("""CREATE (n: HourIndex) """)
	hour = tl._init_hour(datetime(2014, 1, 1, 12, 30))
	assert is_same_graph("""CREATE (n: HourIndex)-[:LATEST]->(m: Hour {start: '2014-01-01T12:00:00'})<-[:EARLIEST]-(n) """)
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

def test_set_latest():
	clear_db()
	tl = Timeline(graph)
	graph.cypher.execute("""MATCH (n: HourIndex)
							MERGE (n)-[:LATEST]->(m: Hour {start: '2014-01-01T12:00:00'})<-[:EARLIEST]-(n) """)
	new_latest, = graph.create(py2neo.Node('Hour', start='2014-01-01T13:00:00'))
	
	tl._set_latest(new_latest)

	assert tl.latest == new_latest
	assert has_sub_graph("""CREATE (n: HourIndex)-[:LATEST]->(m: Hour {start: '2014-01-01T13:00:00'}) """)

def test_set_earliest():
	clear_db()
	tl = Timeline(graph)
	graph.cypher.execute("""MATCH (n: HourIndex)
							MERGE (n)-[:LATEST]->(m: Hour {start: '2014-01-01T12:00:00'})<-[:EARLIEST]-(n) """)
	new_earliest, = graph.create(py2neo.Node('Hour', start='2014-01-01T11:00:00'))
	
	tl._set_earliest(new_earliest)

	assert tl.earliest == new_earliest
	assert has_sub_graph("""CREATE (n: HourIndex)-[:EARLIEST]->(m: Hour {start: '2014-01-01T11:00:00'}) """)

def test_append_hours():
	clear_db()

	tl = Timeline(graph)
	tl._init_hour(datetime(2014, 1, 1, 12))
	tl._append_hours(datetime(2014, 1, 1, 14))
	
	target = """CREATE (i: HourIndex)
			  MERGE (i)-[:EARLIEST]->(n: Hour {start: '2014-01-01T12:00:00'})
			 -[:NEXT]->(m: Hour {start: '2014-01-01T13:00:00'})
		     -[:NEXT]->(o: Hour {start: '2014-01-01T14:00:00'})<-[:LATEST]-(i)"""
	assert is_same_graph(target)

def test_prepend_hours():
	clear_db()

	tl = Timeline(graph)
	tl._init_hour(datetime(2014, 1, 1, 12))
	tl._prepend_hours(datetime(2014, 1, 1, 10))
	
	target = """CREATE (i: HourIndex)
			  MERGE (i)-[:EARLIEST]->(n: Hour {start: '2014-01-01T10:00:00'})
			 -[:NEXT]->(m: Hour {start: '2014-01-01T11:00:00'})
		     -[:NEXT]->(o: Hour {start: '2014-01-01T12:00:00'})<-[:LATEST]-(i)"""
	assert is_same_graph(target)





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

	assert tl._dt_to_str(dt1) == dstring
	assert tl._floor_dt(dt2) == dt1
	assert tl._floor_dt(dt3) == dt1


	assert tl._str_to_dt(dstring) == dt1


def test_create_timespan():
	clear_db()
	tl = Timeline(graph)
	start = datetime(2014, 1, 1, 18, 30)
	stop = datetime(2014, 1, 1, 20, 10)
	timespan_node = tl.create_timespan(start, stop)

	assert timespan_node.bound 
	assert timespan_node.properties['start'] == '2014-01-01T18:30:00'
	assert timespan_node.properties['stop'] == '2014-01-01T20:10:00'
	target_tl = """CREATE (i: HourIndex) MERGE (n: Hour {start: '2014-01-01T18:00:00'})
						 -[:NEXT]->(m: Hour {start: '2014-01-01T19:00:00'})
	 				     -[:NEXT]->(o: Hour {start: '2014-01-01T20:00:00'})<-[:LATEST]-(i)"""
	assert has_sub_graph(target_tl)
	target_ts_a = """CREATE (t: Timespan {start: '2014-01-01T18:30:00', stop: '2014-01-01T20:10:00'})
			  -[:OVERLAPS]-> (n: Hour {start: '2014-01-01T18:00:00'})"""
	target_ts_b = """CREATE (t: Timespan {start: '2014-01-01T18:30:00', stop: '2014-01-01T20:10:00'})
				  -[:OVERLAPS]-> (n: Hour {start: '2014-01-01T19:00:00'})"""
	target_ts_c = """CREATE (t: Timespan {start: '2014-01-01T18:30:00', stop: '2014-01-01T20:10:00'})
				  -[:OVERLAPS]-> (n: Hour {start: '2014-01-01T20:00:00'})"""

	assert has_sub_graph(target_ts_a)
	assert has_sub_graph(target_ts_b)
	assert has_sub_graph(target_ts_c)