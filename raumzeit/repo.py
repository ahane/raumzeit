import py2neo
from py2neo import Node, Relationship
from py2neo.error import GraphError
import re
import hashlib
from unicodedata import normalize as uni_normalize

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
        with_slug = self._with_slug

        entity_dct = with_slug(props)
        node = Node(label, **entity_dct)

        try:           
            created_node, = self._graph.create(node)

        except GraphError as exc:
            if "already exists" in str(exc.args):
                entity_dct = with_slug(props, with_hash=True)
                node_hash = Node(label, **entity_dct)
                created_node, = self._graph.create(node_hash)

            else:
                raise exc

        return created_node

    def create_connection(self, from_dct, to_dct, props=None):
        from_label = from_dct['_label']
        from_slug = from_dct['slug']
        to_label = to_dct['_label']
        to_slug = to_dct['slug']
        rel_label = self.LABELS_TO_REL[(from_label, to_label)]

        created_rel = self._create_connection(from_label, from_slug, rel_label, to_label, to_slug, props)
        parent_node = created_rel.start_node
        child_nodes = [created_rel.end_node]

        created_dct = self._return_joined(parent_node, child_nodes, rel_label)
        return created_dct


    def _create_connection(self, from_label, from_slug, rel_label, to_label, to_slug, props=None):
        """ Creates an edge between two existing nodes `frm` and `to`. """
        
        props = {} if props is None else props
        
        from_node = self._get(from_label, from_slug)
        to_node = self._get(to_label, to_slug)
        rel = Relationship(from_node, rel_label, to_node, **props)
        
        created_rel, = self._graph.create(rel)

        return created_rel

    @classmethod
    def _with_slug(cls, props, with_hash=False):
        """ Insert a slug into the props dict """
        props = props.copy()
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
        
    