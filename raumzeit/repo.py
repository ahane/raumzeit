import py2neo
from py2neo import Node, Relationship
from py2neo.error import GraphError
import re
import hashlib
from unicodedata import normalize as uni_normalize
from datetime import datetime, timedelta

_punct_re = re.compile(r"""[\t !"#$%&\'()*\-/<=>?@\[\\\]^_`{|},.]+""")



class Repository(object):

    def __init__(self):
        pass

    def get(self, collection, slug):
        raise NotImplementedError

    def get_one(self, collection, query):
        raise NotImplementedError

    def iter_all(self, collection):
        raise NotImplementedError

    def iter_joined(self, collection, **other_collection):
        raise NotImplementedError

    def create(self, collection, **other_entities):
        raise NotImplementedError

    @staticmethod
    def slugify(text, hash_dct=False, delim='-', hash_chars=10, punct_re=_punct_re):
        def calc_hash(dct):
            dct_str = str(sorted(dct.items()))
            bytes = uni_normalize('NFKD', dct_str).encode('ascii', 'ignore')
            hash_key = hashlib.sha224(bytes).hexdigest()
            return hash_key

        result = []
        for word in punct_re.split(text.lower()):
            word = uni_normalize('NFKD', word).encode('ascii', 'ignore')
            if word:
                result.append(word.decode('utf-8'))

        if hash_dct:
            result.append(calc_hash(hash_dct)[:hash_chars])

        return delim.join(result)

class NeoRepository(Repository):

    LABELS_TO_REL = {('Happening', 'Location'): 'HAPPENS_AT',
                     ('Happening', 'Artist'): 'HOSTS'}
    REL_TO_ATTR = {'HAPPENS_AT': 'location', 'HOSTS': 'artists'}
    ATTR_TO_REL = {v: k for k, v in REL_TO_ATTR.items()}

    def __init__(self, graph):
        self._graph = graph

    def iter_all_transaction(self, label):
        """ Deprecated"""
        """ Iterate over all entities for a given label """
        match_label_q = """MATCH (n:{l}) RETURN n""".format(l=label)
        record_stream = self._graph.cypher.stream(match_label_q)
        for record in record_stream:
            yield self._record_to_dict(record)

    def iter_all(self, label):
        for node in self._iter_all(label):
            yield self._node_to_dict(node)

    def _iter_all(self, label):
        response = self._graph.find(label)
        for node in response:
            yield node

    def get_one(self, label, prop_key, prop_value):
        """ Get a unique enity that matches the query dictionary """
        node = self._get_one(label, prop_key, prop_value)
        return self._node_to_dict(node)

    def _get_one(self, label, prop_key, prop_value):
        """ Get a unique enity that matches the query dictionary """

        found_node = self._graph.find_one(label, prop_key, prop_value)
        if found_node is None:
            raise KeyError('No node with label {l} and {pk}={pv}'.format(l=label, pk=prop_key, pv=prop_value))
        return found_node

    def get(self, label, slug):
        return self.get_one(label, 'slug', slug)

    def _get(self, label, slug):
        return self._get_one(label, 'slug', slug)

    def url_get(self, url):
        return self._node_to_dict(self._url_get(url))

    def _url_get(self, url):
        url_node = self._get_one('URI', 'url', url)
        rel = self._graph.match_one(None, 'IDENTIFIED_BY', url_node)
        return rel.start_node

    def get_joined(self, label, slug, rel_attr):
        """ Returns an entity dict with another entity embedded in it """
        
        rel_label = self.ATTR_TO_REL[rel_attr]
        start_node = self._get(label, slug)
        nodes = self._get_joined(start_node, rel_label)

        parent_node = nodes[0][0]
        child_nodes = [child for par, child in nodes]
        
        return self._return_joined(parent_node, child_nodes, rel_label)

    def _return_joined(self, parent_node, child_nodes, rel_label):

        if len(child_nodes) == 0:
            raise KeyError("No relationship found with label {rel}".format(rel=rel_label))

        else:
            child_dict = [self._node_to_dict(node) for node in child_nodes]

        parent_dict = self._node_to_dict(parent_node)
        rel_attr = self.REL_TO_ATTR[rel_label]
        parent_dict[rel_attr] = child_dict
        return parent_dict

    def _get_joined(self, start_node, rel_label):
        
        rels = list(self._graph.match(start_node, rel_label, bidirectional=True))
        
        #handle bidirectionality
        if start_node == rels[0].start_node:
            return [(start_node, rel.end_node) for rel in rels]
        else:
            return [(start_node, rel.start_node) for rel in rels]

#    def iter_joined(self, label, slug, *rel_attrs):
#        rel_labels = [self.ATTR_TO_REL[a] for a in rel_attrs]



    def create(self, label, props, **kwargs):
        """ Create a new sluggable entity """
        return self._node_to_dict(self._create_entity(label, props))
        
    def _create_entity(self, label, props):
        """ Create a new sluggable entity. Must have a 'name' property. """
        entity_dct = self._with_slug(props)
        try:           
            node = self._create_node(label, entity_dct)

        except GraphError as exc:
            if "already exists" in str(exc.args):
                entity_dct = self._with_slug(props, with_hash=True)
                node = self._create_node(label, entity_dct)
            else:
                raise exc

        return node

    def _create_node(self, label, props):
        """ Create a new node. """
        node = Node(label, **props)
        self._graph.create(node)
        return node


    def create_connection(self, from_dct, to_dct, props=None):

        from_label, to_label = from_dct['_label'], to_dct['_label']
        from_slug, to_slug = from_dct['slug'], to_dct['slug']
        from_node = self._get(from_label, from_slug)
        to_node = self._get(to_label, to_slug)
        rel_label = self.LABELS_TO_REL[(from_label, to_label)]

        created_rel = self._create_connection(from_node, rel_label, to_node, props)
        parent_node = created_rel.start_node
        child_nodes = [created_rel.end_node]

        created_dct = self._return_joined(parent_node, child_nodes, rel_label)
        return created_dct

    def _create_connection(self, from_node, rel_label, to_node, props=None):
        """ Creates an edge between two existing nodes `frm` and `to`. """
        props = {} if props is None else props
        rel = Relationship(from_node, rel_label, to_node, **props)
        created_rel, = self._graph.create(rel)
        return created_rel

    @classmethod
    def _with_slug(cls, props, with_hash=False):
        """ Insert a slug into the props dict """
        props = props.copy()
        if 'name' not in props.keys():
            raise ValueError("Entity needs 'name' attribute")
        if not with_hash:
            props['slug'] = cls.slugify(props['name'])
        else:
            props['slug'] = cls.slugify(props['name'], props)
        return props



    def get_or_create(self, label, props):
        """ Get or create a value node. """
        pass
        

    @classmethod
    def _record_to_dict(cls, record):
        """ Casts a cypher record into a dict.
        Any nodes in the record will be flattened by adding their
        properties and their lable to the dict.
        Node properties overwrite record properties.
        """
        record_dict = {}
        for key, value in record.__dict__.items():
            if isinstance(value, py2neo.Node):
                node_dict = cls._node_to_dict(value)
                record_dict.update(node_dict)

            else:
                record_dict[key] = value
        return record_dict


    @classmethod
    def _node_to_dict(cls, node):
        node_dict = node.properties.copy()
        assert len(node.labels) == 1
        node_dict['_label'] = list(node.labels)[0]
        return node_dict
        

class HappeningCollection(object):
    
    def __init__(self, graph, repo, timeline):
        self._graph = graph
        self._timeline = timeline
        self._repo = repo

    def iter_all():
        pass

    def get():
        pass

    def iter_timespan():
        pass

    def create(self, start, stop, props):
        happ_node = self._create_entity('Happening', props)
        return self._node_to_dict(happ_node)


class ArtistCollection(object):
    
    def iter_all():
        pass

    def get():
        pass

    def iter_timespan():
        pass

    def create():
        pass


class WorkCollection(object):

    def iter_all():
        pass

    def get():
        pass

    def create():
        pass


class LocationCollection(object):
    
    def iter_all():
        pass

    def get():
        pass

    def create():
        pass
    
class Timeline(object):

    def __init__(self, graph):
        
        self._graph = graph
        self._graph.cypher.execute("""CREATE CONSTRAINT ON (n:Hour) ASSERT n.start is UNIQUE""")
    
        index_nodes =  list(self._graph.find("HourIndex"))
        num_index = len(index_nodes)
        print(num_index)
        if num_index > 1:
            raise ValueError('More than one index found')
        elif num_index == 1:
            print('found')
            self.index = index_nodes[0]
        else:
            print('create')
            index_node = Node('HourIndex')
            self._graph.create(index_node)
            self.index = index_node

    @property 
    def latest(self):
        latest_rel = self._graph.match_one(self.index, 'LATEST')
        if latest_rel is None:
            return None
        else:
            return latest_rel.end_node

    @property 
    def earliest(self):
        earliest_rel = self._graph.match_one(self.index, 'EARLIEST')
        if earliest_rel is None:
            return None
        else:
            return earliest_rel.end_node
    

    def _init_hour(self, hour_datetime):
        
        floored = self._floor_dt(hour_datetime)
        start = self._dt_to_str(floored)
        hour_node = Node("Hour", start=start)
        self._graph.create(hour_node)
        self._set_latest(hour_node)
        self._set_earliest(hour_node)
        return hour_node

    def create_timespan(self, start, stop):
        """ Create a timespan node and connect it to the hours on the timeline it overlaps. """
        
        if self.latest is None or self.earliest is None:
            self._init_hour(start)

        curr_latest_dt = self._start_h_from_node(self.latest)
        if stop > curr_latest_dt:
            self._append_hours(stop)

        curr_earliest_dt = self._start_h_from_node(self.earliest)
        if start < curr_earliest_dt:
            self._prepend_hours(start)

        start_string, stop_string = self._dt_to_str(start), self._dt_to_str(stop)
        timespan_node = Node('Timespan', start=start_string, stop=stop_string)
        
        hour_dts = self._hour_range(start, stop)
        hour_strings = [self._dt_to_str(dt) for dt in hour_dts]
        hour_nodes = [self._graph.find_one('Hour', 'start', start) for start in hour_strings] 
        rels = [Relationship(timespan_node, 'OVERLAPS', hour_node) for hour_node in hour_nodes]

        new_entries = [timespan_node] + rels
        self._graph.create(*new_entries)

        return timespan_node

    def _append_hours(self, new_latest_dt):

        old_latest_node = self.latest
        old_latest_dt = self._start_h_from_node(old_latest_node)
        
        datetime_range = self._hour_range(old_latest_dt, new_latest_dt)
        new_datetimes = datetime_range[1:]
        strings = [self._dt_to_str(h) for h in new_datetimes]
        new_nodes = [Node("Hour", start=h) for h in strings]

        all_nodes = [old_latest_node] + new_nodes
        neighbours = zip(all_nodes[:-1], all_nodes[1:])
        rels = [Relationship(left, 'NEXT', right) for left, right in neighbours]
        
        new_entries = new_nodes + rels
        self._graph.create(*new_entries)
        self._set_latest(new_nodes[-1])

    def _prepend_hours(self, new_earliest_dt):

        old_earliest_node = self.earliest
        old_earliest_dt = self._start_h_from_node(old_earliest_node)
        
        datetime_range = self._hour_range(new_earliest_dt, old_earliest_dt)
        new_datetimes = datetime_range[:-1]
        strings = [self._dt_to_str(h) for h in new_datetimes]
        new_nodes = [Node("Hour", start=h) for h in strings]

        all_nodes = new_nodes + [old_earliest_node]
        neighbours = zip(all_nodes[:-1], all_nodes[1:])
        rels = [Relationship(left, 'NEXT', right) for left, right in neighbours]
        
        new_entries = new_nodes + rels
        self._graph.create(*new_entries)
        self._set_earliest(new_nodes[0])


    def _set_latest(self, latest_node):
        old_rel = self._graph.match_one(self.index, 'LATEST')
        self._graph.delete(old_rel)
        rel = Relationship(self.index, 'LATEST', latest_node)
        self._graph.create(rel)
        return latest_node

    def _set_earliest(self, earliest_node):
        old_rel = self._graph.match_one(self.index, 'EARLIEST')
        self._graph.delete(old_rel)
        rel = Relationship(self.index, 'EARLIEST', earliest_node)
        self._graph.create(rel)
        return earliest_node

    @classmethod
    def _hour_range(self, start_hour, end_hour=None, len_range=None):
        """ Creates a range of datetime objects of hours"""
        
        start_hour = self._floor_dt(start_hour)
        if end_hour is not None:
            end_hour = self._floor_dt(end_hour)
            delta = end_hour - start_hour
            num_hours = int((delta.days*24) + (delta.seconds/60/60))

        elif len_range is not None:
            num_hours = len_range - 1

        else:
            raise ValueError('Need either end_hour or len_range to compute hour range')

        one_hour = timedelta(0, 60*60)        
        hours = [start_hour]
        for i in range(num_hours):
            last_hour = hours[-1]
            next_hour = last_hour + one_hour
            hours.append(next_hour)
        return hours

    @classmethod
    def _start_h_from_node(self, node_with_start):
        date_string = node_with_start.properties['start']
        return self._str_to_dt(date_string)
    @classmethod
    def _dt_to_str(self, dt):
        return dt.isoformat()
    @classmethod
    def _str_to_dt(self, date_string):
        return datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S')
    @classmethod
    def _floor_dt(self, dt):
        if dt.minute != 0 or dt.second != 0:
            dt = datetime(dt.year, dt.month, dt.day, dt.hour)
        return dt
    


