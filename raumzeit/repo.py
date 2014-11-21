import py2neo
from py2neo.cypher.error.schema import ConstraintViolation
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

    def iter_all(self, label):
        """ Iterate over all entities for a given label """
        match_label_q = """MATCH (n:{l}) RETURN n""".format(l=label)
        record_stream = self._graph.cypher.stream(match_label_q)
        for record in record_stream:
            yield self._record_to_dict(record)

    def get_one(self, label, query):
        """ Get a unique enity that matches the query dictionary """
        param_str = self._dict_to_param_properties_pattern(query)
        match_one_q = """MATCH (n:{l} {q}) RETURN n""".format(l=label, q=param_str)
        rec_list = self._graph.cypher.execute(match_one_q, parameters=query)
        num_results = len(rec_list)
        if num_results == 0:
            raise KeyError('Not found')
        elif num_results > 1:
            raise KeyError('Not unique')
        else:
            rec = rec_list[0]
            return self._record_to_dict(rec)

    def get(self, label, slug):
        return self.get_one(label, {'slug': slug})

    def create(self, label, props, **kwargs):
        """ Create a new sluggable entity """
        return self._create_entity(label, props)
        

    def _create_entity(self, label, props):

        props_slug = props.copy()
        props_slug['slug'] = self.slugify(props['name'])
        param_q = self._dict_to_param_properties_pattern(props_slug)
        create_q = """CREATE (n:{l} {p}) return n""".format(l=label, p=param_q)

        try:
            rec_list = self._graph.cypher.execute(create_q, parameters=props_slug)

        except ConstraintViolation:
            props_slug['slug'] = self.slugify(props['name'], props)
            rec_list = self._graph.cypher.execute(create_q, parameters=props_slug)

        return self._record_to_dict(rec_list[0])

    # def _create_connection(self, from, label, to, props=None):
    #     """ Creates an edge between two existing nodes `from` and `to`. """
    #     """MATCH (n:Other {name: 'a'}), (m:Other {name: 'b'}) MERGE (n)-[:AA]->(b)"""
    #     cypher.execute()

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
    def _dict_to_param_properties_pattern(cls, dct):
        """ Compiles the properties part of a parameterized cypher query.
        Example:
        >>> dct = {'slug': 'foo', 'name': 'Foo'}
        >>> param_cypher = _dict_to_param_properties_pattern(dct)
        >>> assert param_cypher == '{slug: {slug}, name: {name}}'
        """
        p = '{'
        for key, value in dct.items():
            if key[0] != '_':
                p += key + ': ' + '{' + key + '}' + ', '
        p = p[:-2] #remove last ', '
        p += '}'
        return p

    @classmethod
    def _dict_to_param_node_pattern(cls, dct, identifier):
        """ Compiles the node descriptor part of a cypher query.
        Example:
        >>> dct = {'slug': 'foo', 'name': 'Foo', '_label': 'Happening'}
        >>> node_desc = _dict_to_node_cypher(dct, 'foo')
        >>> assert node_desc == '(foo: Happening {slug: {slug}, name: {name}})'
        """
        p = '(' + str(identifier) + ':' + dct['_label'] + ' '
        p += cls._dict_to_param_properties_pattern(dct) + ')'
        return p


