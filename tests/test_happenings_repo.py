from raumzeit.repo import Timeline, HappeningCollection
from .util import clear_db, is_same_graph, has_sub_graph, graph
import py2neo
import pytest
from datetime import datetime

def test_init():
	clear_db()
	tl = Timeline(graph)
	happenings = HappeningCollection(graph, tl)
	assert is_same_graph("""CREATE (n: HourIndex) """)


#def test_create_happening():