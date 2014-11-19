import py2neo


#
class Repository(object):

    def __init__(self):
        pass

    def get(self, collection, slug):
        raise NotImplementedError

    def get_one(self, collection, query):
        raise NotImplementedError

    def iter_all(self, collection):
        raise NotImplementedError

    def iter_joined(self, collection, *others):
        raise NotImplementedError

    def create(self, collection, **kwargs):
        raise NotImplementedError

class NeoRepository(Repository):

    def __init__(self, graph):
        self._graph = graph

    def iter_all(self, label):
        match_label_q = """MATCH (n:{l}) RETURN n""".format(l=label)
        record_stream = self._graph.cypher.stream(match_label_q)
        for record in record_stream:
            yield _record_to_dict(record)
        

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
        node_dict['labels'] = node.labels
        return node_dict
        

#    def get(self, label, slug):
