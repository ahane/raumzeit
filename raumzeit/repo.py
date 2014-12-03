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
    REL_TO_ATTR = {'HAPPENS_AT': 'location', 'HOSTS': 'artists', 'LOCATED_AT': 'address'}
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

    def get_joined(self, label, slug, rel_label, rel_attr, single_child=False):
        """ Returns an entity dict with another entity embedded in it """
        
        #old style get_joined fix tests
        #rel_label = self.ATTR_TO_REL[rel_attr]
        start_node = self._get(label, slug)
        nodes = self._get_joined(start_node, rel_label)

        parent_node = nodes[0][0]
        child_nodes = [child for par, child in nodes]
        
        return self.return_joined(parent_node, child_nodes, rel_attr, single_child)

    def _get_joined(self, start_node, rel_label):
        """Moved"""
        
        rels = list(self._graph.match(start_node, rel_label, bidirectional=True))
        if len(rels) == 0:
            raise KeyError('Node {s} doesnt have rel {rel}'.format(s=start_node, rel=rel_label))
        #handle bidirectionality
        if start_node == rels[0].start_node:
            return [(start_node, rel.end_node) for rel in rels]
        else:
            return [(start_node, rel.start_node) for rel in rels]

    def _return_joined(self, parent_node, child_nodes, rel_label):

        if len(child_nodes) == 0:
            raise KeyError("No relationship found with label {rel}".format(rel=rel_label))

        else:
            child_dict = [self._node_to_dict(node) for node in child_nodes]

        parent_dict = self._node_to_dict(parent_node)
        rel_attr = self.REL_TO_ATTR[rel_label]
        parent_dict[rel_attr] = child_dict
        return parent_dict

    def return_joined(self, parent_node, child_nodes, rel_attr, single_child=False):
        
        if single_child:
            if hasattr(child_nodes, '__iter__'):
                child_dict = self._node_to_dict(child_nodes[0])
            else:
                child_dict = self._node_to_dict(child_nodes)
        else:
            if not hasattr(child_nodes, '__iter__'):
                raise ValueError('Need an iterable when single_child not true')
            child_dict = [self._node_to_dict(node) for node in child_nodes]

        parent_dict = self._node_to_dict(parent_node)
        parent_dict[rel_attr] = child_dict
        return parent_dict

    
    def create(self, label, props, **kwargs):
        """ Create a new sluggable entity """
        return self._node_to_dict(self._create_entity(label, props))
        
    def _create_entity(self, label, props, links=None):
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

        finally:
            if links is not None:
                self._create_uris(node, links)
        return node

    def _create_uris(self, entity_node, url_dicts):
        """ Adds URI nodes to an entity node.
            url_dict should resolve from names to urls """
        new_entries = []
        for dct in url_dicts:
            uri_node = Node('URI', name=dct['name'], url=dct['url'])
            rel = Relationship(entity_node, 'IDENTIFIED_BY', uri_node)
            new_entries.extend([uri_node, rel])

        self._graph.create(*new_entries)



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

    @classmethod
    def _record_to_dict(cls, record):
        """ Casts a cypher record into a dict.
        Any nodes in the record will be flattened by adding their
        properties and their lable to the dict.
        Node properties overwrite record properties.
        """
        record_dict = {}
        for key, value in record.__dict__.items():
            is_node = isinstance(value, py2neo.Node)
            trailing_underscore = (key[-1] == '_')
            
            if is_node and not trailing_underscore:
                node_dict = cls._node_to_dict(value)
                record_dict.update(node_dict)

            elif is_node and trailing_underscore:
                sub_node_dict = cls._node_to_dict(value)
                sub_key = key[:-1]
                record_dict[sub_key] = sub_node_dict
            else:
                record_dict[key] = value
        return record_dict


    @classmethod
    def _node_to_dict(cls, node):
        node_dict = node.properties.copy()
        assert len(node.labels) == 1
        node_dict['_label'] = list(node.labels)[0]
        return node_dict

class EntityCollection(object):

    # resolves the field names to graph details
    # first entry in tuple it relationship label name
    # second entry in tuple is wether to expect one (True) or multiple (False)        
    RELS = {'links': ('IDENTIFIED_BY', False)}

    def __init__(self, neorepo, label):
        self.label = label
        self._repo = neorepo
        self._graph = neorepo._graph

    def get(self, slug, joins):
        dct = {}
        for attr in joins:
            rel_label = self.RELS[attr][0]
            single_child = self.RELS[attr][1]
            joined_dct = self._repo.get_joined(self.label, slug, rel_label, attr, single_child)
            dct.update(joined_dct)
        return dct

    def url_get(self, url):
        return self._repo.url_get(url)

    def _init_entity(self, props, links=None):
        node = self._repo._create_entity(self.label, props, links)
        return node

    @classmethod
    def _validate(cls, mandatory, **kwargs):

        def validate_dict(mandatory, dct):
            for item in mandatory:
                if isinstance(item, tuple):
                    key = item[0]
                    value = item[1]
                else:
                    key = item
                    value = None
                if key not in dct:
                    raise ValueError("Couldn't validate: No key: {k}".format(k=key))
                if value is not None:
                    if dct[key] != value:
                        raise ValueError("Couldn't validate: Key {k} should be {v}".format(k=key, v=value))

        def validate_group(mandatory, group):
            if isinstance(group, list):
                dicts = group
            else:
                dicts = [group]
            for dct in dicts:
                validate_dict(mandatory, dct)
        
        for group_name, group in kwargs.items():
            if group_name in mandatory:
                validate_group(mandatory[group_name], group)
            else:
                raise ValueError("Couldn't validate: No group {g}".format(g=group))


class LocationCollection(EntityCollection):

    def __init__(self, neorepo):
        super().__init__(neorepo, 'Location')

        self.RELS.update({'address': ('LOCATED_AT', True)})

    def get(self, slug, joins=['address', 'links']):
        return super().get(slug, joins)

    def create(self, props, address, links):
    
        try:
            self.validate(props=props, address=address, links=links)        
            loc_node = self._init_entity(props, links)
            addr_node = self._repo._create_node('Address', address)
            rel_label = self.RELS['address'][0]
            rel = self._repo._create_connection(loc_node, rel_label, addr_node)
            return self._repo.return_joined(loc_node, addr_node, 'address', True)
        
        except ValueError:
            raise

    @classmethod
    def validate(cls, **kwargs):
        mandatory = {'props': ['name'],
                     'address': ['lat', 'lon', 'string'],
                     'links': ['name', 'url']}
        cls._validate(mandatory, **kwargs)

class ArtistCollection(EntityCollection):
    def __init__(self, neorepo):
        super().__init__(neorepo, 'Artist')

    def get(self, slug, joins=['links']):
        return super().get(slug, joins)

    def create(self, props, links):
        try:
            self.validate(props=props, links=links)
            artist_node = self._init_entity(props, links)
            return self._repo._node_to_dict(artist_node)
        except:
            raise

    @classmethod
    def validate(cls, **kwargs):
        mandatory = {'props': ['name'],
                     'links': ['name', 'url']}
        cls._validate(mandatory, **kwargs)

class WorkCollection(EntityCollection):

    def __init__(self, neorepo):
        super().__init__(neorepo, 'Work')
        self.RELS.update({'artist': ('MADE_BY', True)})

    def get(self, slug, joins=['links', 'artist']):
        return super().get(slug, joins)

    def create(self, props, artist, links):
        try:
            self.validate(props=props, artist=artist, links=links)
            work_node = self._init_entity(props, links)
            artist_node = self._repo._get('Artist', artist['slug'])
            rel_label = self.RELS['artist'][0]
            rel = self._repo._create_connection(work_node, rel_label, artist_node)
            return self._repo.return_joined(work_node, artist_node, 'artist', self.RELS['artist'][1])
        except:
            raise

    @classmethod
    def validate(cls, **kwargs):
        mandatory = {'props': ['name'],
                     'links': ['name', 'url'],
                     'artist': ['slug', ('_label', 'Artist')]}
        cls._validate(mandatory, **kwargs)

class HappeningCollection(EntityCollection):
    
    def __init__(self, neorepo, timeline):
        super().__init__(neorepo, 'Happening')
        self._timeline = timeline
        self.RELS.update({'location': ('HAPPENS_AT', True)})
        self.RELS.update({'artists': ('HOSTS', False)})
        self.RELS.update({'time': ('ACTIVE_DURING', True)})

    def get(self, slug, joins=['links', 'artists', 'location', 'time']):
        return super().get(slug, joins)

    def create(self, start, stop, props, location, artists, links):
        try:
            self.validate(props=props, artists=artists, location=location, links=links)
            
            happ_node = self._init_entity(props, links)

            artist_nodes = [self._repo._get('Artist', a['slug']) for a in artists]
            artist_rel_label = self.RELS['artists'][0]
            artist_rels = [Relationship(happ_node, artist_rel_label, a_n) for a_n in artist_nodes]

            location_node = self._repo._get('Location', location['slug'])
            location_rel_label = self.RELS['location'][0]
            location_rel = Relationship(happ_node, location_rel_label, location_node)

            timespan_node = self._timeline.create_timespan(start, stop)
            timespan_rel_label = self.RELS['time'][0]
            timespan_rel = Relationship(happ_node, timespan_rel_label, timespan_node)

            new_entries = artist_rels + [location_rel] + [timespan_rel]
            self._graph.create(*new_entries)

            return self.get(happ_node.properties['slug'])

        except:
            raise


    def iter_timeframe(self, start, stop):
        params = self._timeline.compile_timeframe_params(start, stop)
        happenings_iter = self._graph.cypher.stream(
            """MATCH
                (h1: Hour {start: {start}}),
                (h2: Hour {start: {stop}}),
                active = (h1)-[:NEXT*0..]->(h: Hour)-[:NEXT*0..]->h2,
                happs = (h)<-[:OVERLAPS]-(t: Timespan)<-[:ACTIVE_DURING]-(happ: Happening),
                happs_ext = (happ)-[:HAPPENS_AT]->(loc)
                RETURN DISTINCT happ, t as time_, loc as location_
            """, parameters = params)
        for record in happenings_iter:
            yield self._repo._record_to_dict(record)

    @classmethod
    def validate(cls, **kwargs):
        mandatory = {'props': ['name'],
                     'links': ['name', 'url'],
                     'location': ['slug', ('_label', 'Location')],
                     'artists': ['slug', ('_label', 'Artist')]}
        cls._validate(mandatory, **kwargs)

    
class Timeline(object):

    def __init__(self, graph):
        
        self._graph = graph
        self._graph.cypher.execute("""CREATE CONSTRAINT ON (n:Hour) ASSERT n.start is UNIQUE""")
    
        index_nodes =  list(self._graph.find("HourIndex"))
        num_index = len(index_nodes)
        
        if num_index > 1:
            raise ValueError('More than one index found')
        
        elif num_index == 1:
            self.index = index_nodes[0]
        
        else:
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
    
    def compile_timeframe_params(self, start, stop):
        """ Compiles a dictionary used in a start-stop timeframe query.
            Makes sure the params are within range of the timeline. """
        start = self._floor_dt(start)
        stop = self._floor_dt(stop)
        
        earliest = self._str_to_dt(self.earliest.properties['start'])
        latest = self._str_to_dt(self.latest.properties['start'])

        if start < earliest:
            start = earliest
        if stop > latest:
            stop = latest

        start_str, stop_str = start.isoformat(), stop.isoformat()
        return {'start': start_str, 'stop': stop_str}


    def _init_hour(self, hour_datetime):
        
        floored = self._floor_dt(hour_datetime)
        start = self._dt_to_str(floored)
        hour_node = Node("Hour", start=start)
        self._graph.create(hour_node)
        self._set_latest(hour_node)
        self._set_earliest(hour_node)
        return hour_node

    def iter_timeframe(self, start, stop, node_label):
        pass

    def create_timespan(self, start, stop):
        """ Create a timespan node and connect it to the hours on the timeline it overlaps. """
        
        self._extend_timeline(start, stop)
        start_string, stop_string = self._dt_to_str(start), self._dt_to_str(stop)
        timespan_node = Node('Timespan', start=start_string, stop=stop_string)
        self._graph.create(timespan_node)
        self._connect_timespan(timespan_node, start, stop)
        self._graph.create(timespan_node)

        return timespan_node

    def _connect_timespan(self, timespan_node, start, stop):
        """ Connects a timespan node with all the hours of the timeline it overlaps. """
        hour_dts = self._hour_range(start, stop)
        hour_strings = [self._dt_to_str(dt) for dt in hour_dts]
        hour_nodes = [self._graph.find_one('Hour', 'start', start) for start in hour_strings] 
        rels = [Relationship(timespan_node, 'OVERLAPS', hour_node) for hour_node in hour_nodes]
        
        self._graph.create(*rels)

    def _extend_timeline(self, start, stop):
        """Adds the timespan between start, stop to the timeline. Fills up gaps with hour nodes. """
        if self.latest is None or self.earliest is None:
            self._init_hour(start)

        curr_latest_dt = self._start_h_from_node(self.latest)
        if stop > curr_latest_dt:
            self._append_hours(stop)

        curr_earliest_dt = self._start_h_from_node(self.earliest)
        if start < curr_earliest_dt:
            self._prepend_hours(start)

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
    


