import py2neo
from py2neo import Node, Relationship
from py2neo.cypher.error.schema import ConstraintViolation
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

    def _create_connection(self, from_label, from_slug, rel_label, to_label, to_slug, props=None):
        """ Creates an edge between two existing nodes `frm` and `to`. """
        """MATCH (n:Other {name: 'a'}), (m:Other {name: 'b'}) MERGE (n)-[:AA]->(b)"""
        props = {} if props is None else props
        from_node = self._get(from_label, from_slug)
        to_node = self._get(to_label, to_slug)
        print(type(from_node))
        print(type(to_node))
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
        
    @classmethod
    def _compile_props_pattern(cls, dct, ident=None):
        """ Compiles the properties part of a parameterized cypher query.
        Example:
        >>> dct = {'slug': 'foo', 'name': 'Foo'}
        >>> param_cypher = _compile_props_pattern(dct)
        >>> assert param_cypher == '{slug: {slug}, name: {name}}'
        """
        prefix = '' if ident is None else ident + '_'
        p = '{'
        for key, value in dct.items():
            if key[0] != '_':
                p += key + ': ' + '{' + prefix + key + '}' + ', '
        p = p[:-2] #remove last ', '
        p += '}'
        return p

    @classmethod
    def _compile_node_pattern(cls, dct, label, ident=None):
        """ Compiles the node descriptor part of a cypher query.
        Example:
        >>> dct = {'slug': 'foo', 'name': 'Foo', '_label': 'Happening'}
        >>> node_desc = _dict_to_node_cypher(dct, 'foo')
        >>> assert node_desc == '(foo: Happening {slug: {slug}, name: {name}})'
        """
        
        props_pattern = cls._compile_props_pattern(dct, ident)
        identifier = 'n' if ident is None else ident
        pattern = '(' + identifier + ':' + label + ' '
        pattern += props_pattern + ')'
        return pattern, identifier

    @classmethod
    def _prepend_identifier(cls, dct, ident):
        """ Adds an identifier to the keys of a dict so it can be used in a parameterized query.
        Example:
        >>> dct = {'slug', 'foo', 'name': 'Foo'}
        >>> dct_ident = _prepend_identifier(dct, 'n')
        >>> assert dct_ident == {'n_slug': 'foo', 'n_name': 'Foo'}
        """
        return {ident + '_' + k: v for k, v  in dct.items()}