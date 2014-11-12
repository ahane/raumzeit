import requests
from requests.utils import quote
from requests import RequestException
import json
from pandas import DataFrame


class Hinterteil(object):

    def __init__(self, url):
        self.url = url


    def get_by_primary(self, table_name, primary_key):

        '''
            Returns a dict representation of a table row by primary key:
            >>> get_by_primary('event', 1)['id']
            1
            >>> get_by_primary('event', 1)['name']
            u'EventA'
            >>> get_by_primary('event', 1)['start_datetime']
            u'2014-10-11T12:00:00'
        '''
        url = self.url
        
        #key = quote(str(primary_key)
        key = primary_key

        r = requests.get(url + table_name + '/' + key )
        if r.ok:
            return r.json()
        else:
            raise IOError(r.status_code)

    def get_single(self, table_name, value, field_name='primary'):
        
        '''
            Returns a single row of a table by looking for a match of `value` 
            in columns `field_name`. 

            Raises an exception if no or more than one
            results found.

            >>> get_single('third_party', 'thirdPartyA', 'name')['name']
            u'thirdPartyA'

            Also wraps get_by_primary():
            >>> get_single('event', 1)['name']
            u'EventA'
        '''
        url = self.url
        
        #value = quote(str(value))



        if field_name == 'primary':
            return self.get_by_primary(table_name, value)
        
        else:
            query = {'filters': [{'name': field_name, 'op': '==', 'val': value}], 'single': True}
            params= {'q':  json.dumps(query)}
            
            r = requests.get(url + table_name, params=params)
        
            if r.ok:
                return r.json()
            else:
                raise IOError(r.status_code)


    # def insert_dict_a(self, table_name, payload):
    #     '''
    #         Inserts a dict representation of a table row.
    #         Returns the inserted dict if successful.
    #         Raises RequestException otherwise.

    #         >>> tp = insert_dict('third_party', {'name': 'thirdPartyD', 'url': 'http://tpD.com'})
    #         >>> a = get_single('artist', 1)
    #         >>> ap = insert_dict('artist_page', {'url': 'http://abc.com', 'artist': {'id': a['id']}, 'third_party': {'id': tp['id']}})
    #         u'thirdPartyC'
    #     '''
    #     url = self.url

    #     json_payload = json.dumps(payload)
    #     headers = {'content-type': 'application/json'}
    #     r = requests.post(url + table_name, data=json_payload, headers=headers)
        
    #     try:
    #         response_dict = r.json()    
    #     except:
    #         pass
        
    #     if r.ok:
    #         return response_dict
    #     else:
    #         if response_dict and 'message' in response_dict.keys():
    #             res_str = '/' + response_dict['message']
    #         else:
    #             res_str = ''
    #         raise IOError(str(r.status_code) + res_str) 

    def append_child(self, table_name, item, field_name, child_payload):
        
        '''
        Adds a new item to a child table that has a 1:n relation to table_name.
        Returns the parent item on success. Child payload must reference its
        parent object.
        
        >>> e = get_single('event', 2)
        >>> len(e['performances'])
        0
        >>> a = get_single('artist', 1)
        >>> k = get_single('performance_kind', 'PerformanceKindA')
        >>> perf = {'name': 'perfC', 'artist': a, 'kind': k, 'event': e}
        >>> e = append_child('event', e, 'performances', perf)
        >>> len(e['performances'])
        1
        '''
        url = self.url

        payload = {field_name: {'add': [child_payload]}}
        json_payload = json.dumps(payload)
        headers = {'content-type': 'application/json'}
        request_url = url + table_name + '/' + str(item['id'])
        r = requests.put(request_url, data=json_payload, headers=headers)
        
        if r.ok:
            response_dict = r.json()
            return response_dict
        else:
            raise IOError(r.status_code)


    def insert_dict(self, table_name, payload):
        '''
            Inserts a dict representation of a table row.
            Returns the inserted dict if successful.
            Raises RequestException otherwise.

            >>> tp = insert_dict('third_party', {'name': 'thirdPartyD', 'url': 'http://tpD.com'})
            >>> a = get_single('artist', 1)
            >>> ap = insert_dict('artist_page', {'url': 'http://abc.com', 'artist': {'id': a['id']}, 'third_party': {'id': tp['id']}})
            u'thirdPartyC'
        '''
        url = self.url

        json_payload = json.dumps(payload)
        headers = {'content-type': 'application/json'}
        r = requests.post(url + table_name, data=json_payload, headers=headers)
        
        status = str(r.status_code)
        if r.ok:
            try:
                response_dict = r.json()  
            except:
                raise RequestException(status)
        
        else:
            try:
                response_dict = r.json()
                msg = response_dict['message']
                ex_msg = status + '/' + msg
                raise RequestException(ex_msg)
            except:
                raise RequestException(status)
    
        return response_dict


    

    def get_df(self, table_name, item_id=None, query=None):
        ''' 
        Requests a table and returns it as a pandas ``DataFrame``.
        Can be combined with a ``query`` argument that has to 
        follow the Flask-restless notation.
        
        If an ``item_id`` is passed, it requests the specific item
        and returns it as a DataFrame with a single row.

        The returned DataFrame contains all fields of the response,
        plus a column ``remote_url`` which is compiled out of the 
        request url and the ``id`` field of the response.
        '''

        def _response_to_dataframe(response, request_url):
            ''' Converts a ``requests`` json response to a ``pandas`` DataFrame '''

            resp_dict = response.json()

            # Table request
            if 'objects' in resp_dict:
                rows = resp_dict['objects']

            # single item request
            else: 
                rows = [resp_dict]

            df = DataFrame(rows)
            
            # We look one Colunn deep for further dataframes
            for col in df.columns:
                df[col] = df[col].apply(_collection_to_df)
                    
            if len(df) > 0:
                df = _add_uri(df, request_url)
            
            return df

        def _collection_to_df(collection):
            ''' Converts lists and dicts to DataFrame.
                If neither a list nor a dict is found it
                returns the unchanged argument
            '''
            if isinstance(collection, dict):
                collection = [collection]
            
            if isinstance(collection, list):
                try:
                    df = DataFrame(collection)
                except:
                    raise
                
                return df
            
            else:
                return collection


        def _add_uri(df, url, id_col='id', url_col='remote_url'):
            
            if id_col in df:
                url = url.rstrip('/') + '/'
                urls = df[id_col].apply(lambda id: url + str(id))
                df[url_col] = urls
            
            elif len(df.columns) == 0:
                pass
            
            else:
                raise KeyError('DataFrame doesnt have id column')
                
            return df


        url = self.url

        table_name = table_name.rstrip('/')
        request_url = url + table_name

        # Single row case
        if item_id:
            response = requests.get(request_url + '/' + str(item_id))

        # Normal collection-like case
        else:
            if query:
                q = json.dumps(query)
                params = {'q': q}
            else:
                params = None
            response = requests.get(request_url, params=params)
            
        if response.ok:
            df = _response_to_dataframe(response, request_url)
            
        else:
            raise IOError(response.status_code)
        
        return df