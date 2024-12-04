"""dlx.marc"""

import time, re, json, threading, copy, typing
from collections import Counter
from datetime import datetime, timezone
from warnings import warn
from xml.etree import ElementTree
import jsonschema
from bson import SON, Regex
from pymongo import ReturnDocument, UpdateOne, DeleteOne, CursorType
from pymongo.collation import Collation
from dlx.config import Config
from dlx.db import DB
from dlx.file import File, Identifier
from dlx.marc.query import QueryDocument, Query, AtlasQuery, Condition, Or, Raw
from dlx.util import Table, Tokenizer
import logging

LOGGER = logging.getLogger()

### Exceptions

class AuthException(Exception):
    pass

class InvalidAuthXref(AuthException):
    def __init__(self, rtype, tag, code, xref):
        super().__init__(f'xref (auth#) is invalid: {tag}, {code}, {xref}')

class InvalidAuthValue(AuthException):
    def __init__(self, rtype, tag, code, value):
        super().__init__(f'Invalid authority-controlled value: {tag}, {code}, "{value}"')

class AmbiguousAuthValue(AuthException):
    def __init__(self, rtype, tag, code, value):
        super().__init__(f'Authority-controlled value: {tag}, {code}, "{value}" is a header for multiple auth records. Use the xref instead')

class InvalidAuthField(AuthException):
    def __init__(self, rtype, tag, code):
        super().__init__(f'{tag}, {code} is an authority-controlled field')

class AuthInUse(Exception):
     def __init__(self):
        super().__init__('Can\'t delete Auth record because it is in use by other records')

### Decorators

class Decorators():
    def check_connected(method):
        def wrapper(*args, **kwargs):
            if not DB.connected:
                raise Exception('Must be connected to DB before exececuting this function')
                
            return method(*args, **kwargs)

        return wrapper

### Set classes

class MarcSet(): 
    """Handles sets of MARC records.

    Atrributes
    ----------
    records : iterable
    count : int
    """
    # constructors

    @classmethod
    @Decorators.check_connected
    def from_query(cls, *args, **kwargs):
        """Instatiates a MarcSet object from a Pymongo database query.

        Parameters
        ----------
        filter : (dict|bson.SON), dlx.marc.Query
            A valid Pymongo query filter against the database or a dlx.marc.Query object
        *args, **kwargs : 
            Passes all remaining arguments to `pymongo.collection.Collection.find())

        Returns
        -------
        MarcSet
        """
        self = cls()
        
        if isinstance(args[0], Query):
            for cond in args[0].conditions:
                #if not isinstance(cond, Or):
                cond.record_type = self.record_type

            args[0].record_type = self.record_type
            query = args[0].compile()
        elif isinstance(args[0], Condition):
            args[0].record_type = self.record_type
            query = args[0].compile()
        elif isinstance(args[0], (list, tuple)):
            conditions = args[0]
            
            for cond in conditions:
                if not isinstance(cond, Or):
                    cond.record_type = self.record_type

            query = Query(*conditions).compile()
        else:
            query = args[0]

        args = [query, *args[1:]]
        self.query_params = [args, kwargs]
        Marc = self.record_class
        ac = kwargs.pop('auth_control', False)

        if 'collation' not in kwargs and Config.marc_index_default_collation:
            #warn('Collation not set. Using default collation set in config')
            kwargs['collation'] = Config.marc_index_default_collation

        self.records = map(lambda r: Marc(r, auth_control=ac), self.handle.find(*args, **kwargs))

        return self

    @classmethod
    def from_aggregation(cls, *args, **kwargs):
        # the aggregation results must return valid Marc records
        self = cls()
        self.query_params = [args, kwargs]
        Marc = self.record_class
        ac = kwargs.pop('auth_control', False)
        self.records = map(lambda r: Marc(r, auth_control=ac), self.handle.aggregate(*args, **kwargs))

        return self

    @classmethod
    def from_ids(cls, ids):
        return cls.from_query({'_id' : {'$in': ids}})

    @classmethod
    def sort_table_header(cls, header: list) -> list:
        # Sorts the header of a MarcSet in util.Table for for use in 
        # (de)serialization

        return sorted(
            header, 
            key=lambda x: ( 
                (re.match('\d+\.(\w+)', x)).group(1), # sort by tag
                int(re.match('(\d+)\.', x).group(1)), # sort by prefix group
                (re.match('\d\.\d{3}\$?(\w)?', x)).group(1) # sort by subfield code
            )
        )

    @classmethod
    def from_table(cls, table, auth_control=True, auth_flag=False, field_check=None, delete_subfield_zero=True):
        # does not support repeated subfield codes
        self = cls()
        self.records = []
        exceptions = []

        for temp_id in table.index.keys():
            record = cls().record_class()

            # Sort the headers so that subfield $0 is always first for use in 
            # subsequent subfields
            header_fields = list(table.index[temp_id].keys())
            header_fields = MarcSet.sort_table_header(header_fields)

            for field_name in header_fields:
                instance = 0
                value = table.index[temp_id][field_name]
                
                # parse the column header into tag, code and place
                if match := re.match(r'^(([1-9]+)\.)?(\d{3})(\$)?([a-z0-9])', str(field_name)):
                    if match.group(1):
                        instance = int(match.group(2))
                        instance -= 1 # place numbers start at 1 in col headers instead of 0

                    tag, code = match.group(3), match.group(5)
                elif len(field_name) == 3 and field_name[0:2] == '00':
                    tag, code = field_name, None
                else:
                    exceptions.append('Invalid column header "{}"'.format(field_name))
                    continue

                if record.get_value(tag, code, address=[instance, 0]):
                    exceptions.append('Column header {}.{}{} is repeated'.format(instance, tag, code))
                    continue

                # if the subfield field is authority-controlled, use subfield 
                # $0 as the xref. $0 will be the first subfield in the field if
                # it exists, because the header has been sorted.
                xref = None

                if Config.is_authority_controlled(self.record_type, tag, code):
                    if field := record.get_field(tag, place=instance):
                        if subfield := field.get_subfield('0'):
                            xref = int(subfield.value)
                            value = Auth.lookup(xref, code)

                # flag if specified field value is already in the system
                if field_check and field_check == tag + (code or ''):
                    if self.record_class.find_one(Condition(tag, {code: value}).compile()):
                        exceptions.append('{}${}: "{}" is already in the system'.format(tag, code, value))
                        continue

                # update the marc object
                field = record.get_field(tag, place=instance)
                address = [instance] if field else ['+']
                ambiguous = []

                try:
                    record.set(tag, code, xref if xref else value, address=address, auth_control=auth_control)
                except AmbiguousAuthValue as e:
                    ambiguous.push(Literal(code, value))     
                except Exception as e:
                    exceptions.append(str(e))
                
                # attempt to use multiple subfields to resolve ambiguity
                if ambiguous:
                    if xref := Auth.resolve_ambiguous(tag, ambiguous, record_type=self.record_type):
                        field.set(code, xref, place='+', auth_control=auth_control)
                    else:
                        exceptions.append(str(AmbiguousAuthValue(self.record_type, field.tag, '*', str([x.to_str() for x in ambiguous]))))

                # if this is the last cell in the row, delete any subfield $0
                if header_fields.index(field_name) == len(header_fields) - 1:
                    if delete_subfield_zero:
                        field = record.get_field(tag, instance)
                        field.subfields = list(filter(lambda x: x.code != '0', field.subfields))

            self.records.append(record)

        if exceptions:
            raise Exception('\n\n' + '\n'.join(exceptions) + '\n')

        self.count = len(self.records)

        return self

    @classmethod
    def from_excel(cls, path, auth_control=True, auth_flag=False, field_check=None, date_format='%Y%m%d'):
        table = Table.from_excel(path, date_format=date_format)

        return cls.from_table(table, auth_control=auth_control, field_check=field_check)

    @classmethod
    def from_xml_raw(cls, root, *, auth_control=False):
        assert isinstance(root, ElementTree.Element)
        self = cls()
        
        i = 0

        for r in root.findall('record'):
            i += 1
            self.records.append(self.record_class.from_xml_raw(r, auth_control=auth_control))

        return self
        
    @classmethod
    def from_xml(cls, string, auth_control=False):
        return cls.from_xml_raw(ElementTree.fromstring(string), auth_control=auth_control)

    @classmethod
    def from_mrk(cls, string, *, auth_control=True):
        self = cls()

        for record in string.split('\n\n'):

            record = self.record_class.from_mrk(record, auth_control=auth_control)

            if isinstance(record, Marc) and len(record.fields) > 0: 
                self.records.append(record)

        return self
    
    # instance

    def __iter__(self): return self
    def __next__(self): return next(self.records) 

    def __init__(self):
        self.records = [] # records # can be any type of iterable

    @property
    def count(self):
        if isinstance(self.records, (map, typing.Generator)):
            args, kwargs = self.query_params

            if isinstance(args[0], list):
                # aggregation pipeline
                count_ag = args[0] + [{'$count': 'c'}]
                self._count = next(self.handle.aggregate(count_ag, **kwargs), {}).get('c') or 0
            else:
                # regular query document
                if args[0] or kwargs.get('skip') or kwargs.get('limit'):
                    kwargs.pop('sort', None) # remove sort param if exists. count doesn't work with sort
                    kwargs['collation'] = None if DB.database_name == 'testing' else Config.marc_index_default_collation # param not supported in mongomock as of 4.1.2
                    self._count = self.handle.count_documents(*args, **kwargs)
                else:
                    # no criteria
                    self._count = self.handle.estimated_document_count()

            return self._count
        else:
            return len(self.records)

    @count.setter
    def count(self, val):
        self._count = val

    def cache(self):
        self.records = list(self.records)
        return self

    def remove(self, id):
        pass

    # serializations

    def to_mrc(self, *, write_id=True):
        # todo: stream instead of queue in memory
        mrc = ''

        for record in self.records:
            mrc += record.to_mrc(write_id=write_id)

        return mrc

    def to_xml(self, *, xref_prefix='', write_id=True):
        # todo: stream instead of queue in memory
        root = ElementTree.Element('collection')

        for record in self.records:
            root.append(record.to_xml_raw(xref_prefix=xref_prefix, write_id=write_id))

        return ElementTree.tostring(root, encoding='utf-8').decode('utf-8')

    def to_mrk(self, *, write_id=True):
        return '\n'.join([r.to_mrk(write_id=write_id) for r in self.records])
        
    def to_str(self):
        return '\n'.join([r.to_str() for r in self.records])

    def to_excel(self, path):
        pass

    def to_table(self, *, write_id=True) -> Table:
        table = Table()

        for i, record in enumerate(self.records):
            # field names in the table header are the in the form of f{place}.{tag}${subfield_code}
            # each record is one table row
            i += 1
            
            if write_id:
                table.set(i, '1.001', str(record.id))
            elif field := record.get_field('001'):
                table.set(i, '1.001', field.value)
                # ignore any other controlfields

            for tag in [x for x in record.get_tags() if not re.match('00', x)]:
                for place, field in enumerate(record.get_fields(tag)):
                    place += 1

                    xref = None

                    for subfield in field.subfields:
                        table.set(i, f'{place}.{field.tag}${subfield.code}', subfield.value)
                        
                        if hasattr(subfield, 'xref'):
                            xref = subfield.xref
                    
                    if xref:
                        table.set(i, f'{place}.{field.tag}$0', str(xref))

        # sort the table header
        table.header = MarcSet.sort_table_header(table.header)

        return table

    def to_csv(self, *, write_id=True) -> str:
        return self.to_table(write_id=write_id).to_csv()
    
    def to_tsv(self, *, write_id=True) -> str:
        return self.to_table(write_id=write_id).to_tsv()

class BibSet(MarcSet):
    def __init__(self, *args, **kwargs):
        self.handle = DB.bibs
        self.record_class = Bib
        self.record_type = 'bib'
        super().__init__(*args, **kwargs)

class AuthSet(MarcSet):
    def __init__(self, *args, **kwargs):
        self.handle = DB.auths
        self.record_class = Auth
        self.record_type = 'auth'
        super().__init__(*args, **kwargs)

### Record classes

class Marc(object):
    '''
    '''

    # Class methods

    #### database query handlers

    @classmethod
    def _increment_ids(cls):
        col = DB.handle[cls.record_type + '_id_counter']
        result = col.find_one_and_update({'_id': 1}, {'$inc': {'count': 1}}, return_document=ReturnDocument.AFTER)

        if result:
            if result['count'] <= cls.max_id():
                raise Exception('The ID incrementer is out of sync')
            
            return result['count']
        else:
            # this should only happen once
            i = cls.max_id() + 1
            col.insert_one({'_id': 1, 'count': i})

            return i

    @classmethod
    def max_id(cls):
        max_dict = next(cls.handle().aggregate([{'$sort' : {'_id' : -1}}, {'$limit': 1}, {'$project': {'_id': 1}}]), {})
        
        history_collection = DB.handle[cls.record_type + '_history']
        deleted_dict = next(history_collection.aggregate([{'$sort' : {'_id' : -1}}, {'$limit': 1}, {'$project': {'_id': 1}}]), {})
        
        m, d = max_dict.get('_id') or 0, deleted_dict.get('_id') or 0
            
        return m if m > d else d

    @classmethod
    @Decorators.check_connected
    def handle(cls):
        return DB.bibs if cls.__name__ == 'Bib' else DB.auths

    @classmethod
    def match_id(cls, idx):
        """
        Deprecated
        """

        warn('dlx.marc.Marc.match_id() is deprecated. Use dlx.marc.Marc.from_id() instead')

        return cls.find_one(filter={'_id' : idx})

    @classmethod
    def from_id(cls, idx, *args, **kwargs):
        return cls.from_query({'_id' : idx}, *args, **kwargs)

    @classmethod
    def match_ids(cls, *ids, **kwargs):
        """
        Deprecated
        """

        warn('dlx.marc.Marc.match_ids() is deprecated. Use dlx.marc.MarcSet.from_ids() instead')

        return cls.find(filter={'_id' : {'$in' : [*ids]}})

    @classmethod
    def match(cls, *matchers, **kwargs):
        """
        Deprecated
        """

        warn('dlx.marc.Marc.match() is deprecated. Use dlx.marc.MarcSet.from_query() instead')

        pymongo_kwargs = {}

        if 'filter' in kwargs:
            pymongo_kwargs['filter'] = kwargs['filter']
        else:
            pymongo_kwargs['filter'] = QueryDocument(*matchers).compile()

        if 'project' in kwargs:
            projection = {}

            for tag in kwargs['project']:
                projection[tag] = 1

            pymongo_kwargs['projection'] = projection

        # sort only works on _id field
        for arg in ('limit', 'skip', 'sort'):
            if arg in kwargs:
                pymongo_kwargs[arg] = kwargs[arg]

        cursor = cls.handle().find(**pymongo_kwargs)

        for doc in cursor:
            yield cls(doc)

    @classmethod
    def find(cls, *args, **kwargs):
        """
        Deprecated
        """

        warn('dlx.marc.Marc.find() is deprecated. Use dlx.marc.MarcSet.from_query() instead')

        cursor = cls.handle().find(*args, **kwargs)

        for doc in cursor:
            yield cls(doc)

    @classmethod
    def find_one(cls, *args, **kwargs):
        """
        Deprecated
        """

        warn('dlx.marc.Marc.find() is deprecated. Use dlx.marc.Marc.from_query() instead')

        found = cls.handle().find_one(*args, **kwargs)

        if found is not None:
            return cls(found)

    @classmethod
    def from_query(cls, *args, **kwargs):
        if 'collation' not in kwargs and Config.marc_index_default_collation:
            warn('Collation not set. Using default collation set in config')
            kwargs['collation'] = Config.marc_index_default_collation

        return next(cls.set_class.from_query(*args, **kwargs), None)

    @classmethod
    def count_documents(cls, *args, **kwargs):
        """
        Deprecated
        """

        warn('dlx.marc.Marc.count_documents() is deprecated. Use dlx.marc.MarcSet.count instead')

        return cls.handle().count_documents(*args, **kwargs)

    # Instance methods

    def __init__(self, doc={}, *, auth_control=False, **kwargs):
        self.data = doc
        self.id = int(doc['_id']) if '_id' in doc else None
        self.created = doc.get('created')
        self.created_user = doc.get('created_user')
        self.updated = doc.get('updated')
        self.user = doc.get('user')
        self.text = doc.get('text')
        self.words = doc.get('words')
        self.fields = []
        self.parse(doc, auth_control=auth_control)

    @property
    def controlfields(self):
        return list(filter(lambda x: x.tag[:2] == '00', sorted(self.fields, key=lambda x: x.tag)))

    @property
    def datafields(self):
        return list(filter(lambda x: x.tag[:2] != '00', sorted(self.fields, key=lambda x: x.tag)))

    def parse(self, doc, *, auth_control=False):
        for tag in filter(lambda x: re.match('^(\d{3}|[A-Z]{3})', x), doc.keys()):
            if tag == '000':
                self.leader = doc['000'][0]

            if tag[:2] == '00':
                for value in doc[tag]:
                    self.fields.append(Controlfield(tag, value))
            else:
                for field in filter(lambda x: [s.get('xref') or s.get('value') for s in x.get('subfields')], doc[tag]):                
                    self.fields.append(Datafield.from_dict(record_type=self.record_type, tag=tag, data=field, auth_control=auth_control))
                
    #### "get"-type methods

    def get_fields(self, *tags):
        if len(tags) == 0:
            return sorted(self.fields, key=lambda x: x.tag)

        return list(filter(lambda x: True if x.tag in tags else False, sorted(self.fields, key=lambda x: x.tag)))

    def get_field(self, tag, place=0):
        if place == 0:
            return next(filter(lambda x: True if x.tag == tag else False, self.fields), None)
        
        try:
            return self.get_fields(tag)[place]
        except IndexError:
            return

    def get_values(self, tag, *codes, **kwargs):
        if tag[:2] == '00':
            return [field.value for field in self.get_fields(tag)]
            
        values = [sub.value for sub in self.get_subfields(tag, *codes, place=kwargs.get('place'))]

        return list(filter(None, values))

    def get_value(self, tag, code=None, *, address=[0, 0], language=None):
        if len(address) != 2:
            raise Exception('Keyword agrgument "address" must be an iterable containing two ints')
            
        field = self.get_field(tag, place=address[0])    

        if isinstance(field, Controlfield):
            return field.value

        if isinstance(field, Datafield):
            sub = field.get_subfield(code, place=address[1])

            if sub:
                if language:
                    return sub.translated(language)
                else:
                    return sub.value

        return ''

    def get_tags(self):
        return sorted(set([x.tag for x in self.get_fields()]))

    def get_xrefs(self, *tags):
        xrefs = []

        for field in filter(lambda f: isinstance(f, Datafield), self.get_fields(*tags)):
            xrefs += field.get_xrefs()

        return xrefs

    def get_xref(self, tag, code, address=None):
        return self.get_subfield(tag, code, address=address).xref 

    def get_subfield(self, tag, code, address=None):
        if address:
            return self.get_field(tag, place=address[0]).get_subfield(code, place=address[1])

            i, j = 0, 0

            for i, field in enumerate(self.get_fields(tag)):
                for j, sub in enumerate(filter(lambda x: x.code == code, field.subfields)):
                    if [i, j] == address:
                        return sub
        else:
            return next(filter(lambda x: x.code == code, self.get_field(tag).subfields), None)

    def get_subfields(self, tag, *codes, **kwargs):
        place = kwargs.get('place')

        if isinstance(place, int):
            fields = [self.get_field(tag, place=place)]
        elif place is None:
            fields = self.get_fields(tag)
        else:
            raise Exception('Invalid place')

        subs = []

        for field in fields:
            if isinstance(field, Controlfield):
                return

            subs += list(filter(lambda x: x.code in codes, field.subfields))

        return subs

    def get_text(self, tag):
        pass

    #### "set"-type methods

    def set(self, tag, code, new_val, *, ind1=None, ind2=None, auth_control=True, address=[]):
        if not new_val and not ind1 and not ind2:
            return self
            
        field_place, subfield_place = 0, 0

        if len(address) > 0:
            field_place = address[0]

            if not isinstance(field_place, int) and field_place != '+':
                raise Exception('Invalid address')

            if len(address) > 1:
                subfield_place = address[1]

                if not isinstance(subfield_place, int) and subfield_place != '+':
                    raise Exception('Invalid address')

        fields = self.get_fields(tag)

        ### new field

        if len(fields) == 0 or field_place == '+':
            if tag[:2] == '00':
                field = Controlfield(tag, new_val)
                self.fields.append(field)
            else:
                field = Datafield(tag=tag, record_type=self.record_type)            
                field.set(code, new_val, ind1=ind1, ind2=ind2, auth_control=auth_control)
                self.fields.append(field)

            return self

        ### existing field

        if len(fields) < field_place:
            raise Exception('There is no field at {}/{}'.format(tag, field_place))

        field = fields[field_place]

        if isinstance(field, Controlfield):
            field.value = new_val
        else:
            field.set(code, new_val, ind1=ind1 or None, ind2=ind2 or None, place=subfield_place, auth_control=auth_control)

        return self

    def set_values(self, *tuples):
        for t in tuples:
            tag, sub, val = t[0], t[1], t[2]
            kwargs = t[3] if len(t) > 3 else {}
            self.set(tag, sub, val, **kwargs)

        return self

    def set_008(self):
        # sets position 0-5 and 7-10
        text = self.get_value('008').ljust(40, '|')
        date_tag, date_code = Config.date_field
        pub_date = self.get_value(date_tag, date_code)
        pub_year = pub_date[0:4].ljust(4, '|')
        cat_date = datetime.now(timezone.utc).strftime('%y%m%d')

        self.set('008', None, cat_date + text[6] + pub_year + text[11:])

    def delete_field(self, tag_or_field, place=0):
        if isinstance(tag_or_field, (Controlfield, Datafield)):
            field = tag_or_field
            self.fields = [f for f in self.fields if f != field]
        elif isinstance(place, int):
            tag = tag_or_field
            i, j = 0, 0

            for i, field in enumerate(self.fields):
                if field.tag == tag:
                    if j == place:
                        del self.fields[i]
                        return self

                    j += 1
        else:
            raise Exception('Invalid place')

        return self

    def delete_fields(self, *tags):
        self.fields = list(filter(lambda x: x.tag not in tags, self.datafields))

        return self

    ### store

    def validate(self):
        try:   
            jsonschema.validate(instance=self.to_dict(), schema=Config.jmarc_schema, format_checker=jsonschema.FormatChecker())
        except jsonschema.exceptions.ValidationError as e:
            msg = '{} in {} : {}'.format(e.message, str(list(e.path)), self.to_json())
            raise jsonschema.exceptions.ValidationError(msg)

    @Decorators.check_connected
    def commit(self, user='admin', auth_check=True, update_attached=True):
        new_record = True if self.id is None else False
        self.id = type(self)._increment_ids() if new_record else self.id
        self.validate()
        data = self.to_bson()
        self.updated = data['updated'] = datetime.now(timezone.utc)
        self.user = data['user'] = user
        previous_state = (DB.bibs if self.record_type == 'bib' else DB.auths).find_one({'_id': self.id})
        
        if previous_state:
            # disregard any provided created data and use existing
            if previous_state.get('created'):
                self.created = previous_state['created']
                self.created_user = previous_state['created_user']
            else:
                # the record is likely from the legacy system (inserted directly into DB, hence no audit data)
                #raise Exception(f'Created date not found for existing record {self.record_type} {self.id}')
                self.created = None
                self.created_user = None
        elif new_record:
            self.created = self.updated
            self.created_user = self.user
        else:
            # record has been created with an id that doesn't exist yet
            # this actually shouldn't be allowed but is needed for certain existing tests to pass
            if DB.database_name != 'testing': Config.warn(f'{self.record_type} {self.id} is being created with a user-specified ID')
            self.created = self.updated
            self.created_user = self.user

        data['created'] = self.created
        data['created_user'] = self.created_user

        def auth_validate():
            for i, field in enumerate(filter(lambda x: isinstance(x, Datafield), self.fields)):
                for subfield in field.subfields:
                    # auth control
                    if Config.is_authority_controlled(self.record_type, field.tag, subfield.code):
                        if not hasattr(subfield, 'xref'):
                           raise InvalidAuthField(self.record_type, field.tag, subfield.code) 

                        if not Auth.lookup(subfield.xref, subfield.code):
                            raise InvalidAuthXref(self.record_type, field.tag, subfield.code, subfield.xref)

        if auth_check: auth_validate()

        # clear the cache for any found xrefs so the calculated fields are up to date
        for field in self.datafields:
            for subfield in [x for x in field.subfields if hasattr(x, 'xref')]:
                Auth._cache.get(subfield.xref, {})[subfield.code] = None
            
        # maintenance functions
        # update field text indexes
        def index_field_text(*, threaded=True):
            try:
                all_text = []

                for i, field in enumerate(filter(lambda x: isinstance(x, Datafield), self.fields)):
                    # tag indexes
                    tag_col = DB.handle[f'_index_{field.tag}']
                    text = ' '.join([subfield.value or '' for subfield in field.subfields]) # subfield may no longer exist
                    scrubbed = Tokenizer.scrub(text)
                    all_text.append(scrubbed)

                    if threaded:
                        updates = [
                            UpdateOne(
                                {'_id': text},
                                {'$addToSet': {'subfields': {'code': subfield.code, 'value': subfield.value}}},
                                upsert=True
                            ) for subfield in field.subfields
                        ]

                        words = Tokenizer.tokenize(text)
                        count = Counter(words)

                        updates.append(
                            UpdateOne(
                                {'_id': text}, 
                                {'$set': {'text': f' {scrubbed} ', 'words': list(count.keys())}},
                            )   
                        )

                        tag_col.bulk_write(updates)
                        # create text index if it doesn't exist
                        tag_col.create_index([('subfields.value', 'text')], default_language='none')

                return ' '.join(all_text)
            except Exception as err:
                LOGGER.exception(err)
                raise err

        # all-text
        self.text = index_field_text(threaded=False)
        data['text'] = f' {self.text} ' # padded with spaces for matching
        data['words'] = self.words = Tokenizer.tokenize(self.text)
        #count = Counter(data['words'])
        #self.word_count = [{'stem': k, 'count': v} for k, v in count.items()]

        if DB.database_name == 'testing' or Config.threading == False:
            index_field_text()
        else:
            thread1 = threading.Thread(target=index_field_text, args=[])
            thread1.setDaemon(False) # stop the thread after complete
            thread1.start()
        
        # add logical fields
        def index_logical_fields():
            try:
                if previous_state:
                    # check old values to delete from browse collection
                    for logical_field in (Config.bib_logical_fields.keys() if self.record_type == 'bib' else Config.auth_logical_fields.keys()):
                        if logical_field == '_record_type': continue

                        if logical_field in previous_state:
                            old_values = previous_state[logical_field]
                        else: continue

                        new_values = self.logical_fields().get(logical_field)
                        if new_values is None: continue
                        updates = []

                        for value in old_values:
                            if value in new_values: continue

                            bibcount, authcount = 0, 0 
                            
                            if logical_field in Config.bib_logical_fields:
                                found_bibs = list(DB.bibs.find({logical_field: value}, limit=2, collation=Config.marc_index_default_collation))
                                bibcount = len(found_bibs)
                                
                                if bibcount > 1: 
                                    continue

                            if logical_field in Config.bib_logical_fields:
                            
                                found_auths = list(DB.auths.find({logical_field: value}, limit=2, collation=Config.marc_index_default_collation))
                                authcount = len(found_auths)
                                
                                if authcount > 1:
                                    continue

                            if bibcount + authcount == 0:
                                # no records exist with this value
                                updates.append(DeleteOne({'_id': value}))
                            elif self.record_type == 'auth':
                                if authcount == 1:
                                    if self.id == found_auths[0]['_id']:
                                        if bibcount == 0:
                                            updates.append(DeleteOne({'_id': value}))
                            elif self.record_type == 'bib':
                                if bibcount == 1:
                                    if self.id == found_bibs[0]['_id']:
                                        updates.append(DeleteOne({'_id': value}))

                        if updates:
                            DB.handle[f'_index_{logical_field}'].bulk_write(updates)

                # insert all the new data's logical fields into the index
                for logical_field, values in self.logical_fields().items():
                    if logical_field == '_record_type':
                        continue
                    
                    # text/browse indexes
                    updates = []

                    for val in values:
                        scrubbed = Tokenizer.scrub(val)
                        words = Tokenizer.tokenize(scrubbed)
                        count = Counter(words)
                        updates.append(
                            UpdateOne(
                                {'_id': val},
                                {   
                                    '$set': {'text': f' {scrubbed} ', 'words': list(count.keys())},
                                    '$addToSet': {'_record_type': self.logical_fields()['_record_type'][0]} # there is  only one record type in the array
                                },
                                upsert=True
                            )
                        )

                    DB.handle[f'_index_{logical_field}'].bulk_write(updates)
            except Exception as err:
                LOGGER.exception(err)
                raise err

        # assign logical fields here
        for field, vals in self.logical_fields().items(): 
            data[field] = vals

        # get rid of any logical fields in the data that no longer exist
        for field in (Config.bib_logical_fields if self.record_type == 'bib' else Config.auth_logical_fields).keys():
            if field not in self.logical_fields():
                self.data.pop(field, None)

        if DB.database_name == 'testing' or Config.threading == False:
            index_logical_fields()
        else:
            thread2 = threading.Thread(target=index_logical_fields, args=[])
            thread2.setDaemon(False) # stop the thread after complete
            thread2.start()

        # history
        def save_history():
            try:
                history_collection = DB.handle[self.record_type + '_history']
                record_history = history_collection.find_one({'_id': self.id})

                if record_history:
                    # capture previous state if record originated in another db
                    if record_history.get('history') is None:
                        if previous_state:
                            previous_state['user'] = 'system import'
                            record_history['history'] = [previous_state, data]
                        else:
                            record_history['history'] = [data]
                    else:
                        record_history['history'].append(data)
                else:
                    record_history = SON()
                    record_history['_id'] = self.id

                    if new_record:
                        record_history['created'] = SON({'user': user, 'time': datetime.now(timezone.utc)})

                    # capture previous state if record originated in another db
                    if previous_state:
                        previous_state['user'] = 'system import'
                        record_history['history'] = [previous_state, data]
                    else:
                        record_history['history'] = [data]

                history_collection.replace_one({'_id': self.id}, record_history, upsert=True)
            except Exception as err:
                LOGGER.exception(err)
                raise err

        if DB.database_name == 'testing' or Config.threading == False:
            save_history()
        else:
            thread3 = threading.Thread(target=save_history, args=[])
            thread3.setDaemon(False) # stop the thread after complete
            thread3.start()

        # commit
        result = type(self).handle().replace_one({'_id' : int(self.id)}, data, upsert=True)
        
        if not result.acknowledged:
            raise Exception('Commit failed')
        
        # manage caches
        if isinstance(self, Auth):
            # clear these caches
            for cache in ('_xcache', '_pcache', '_langcache'):
                setattr(Auth, cache, {})

            # update this cache
            Auth._cache[self.id] = {}

            if hf := self.heading_field:
                for subfield in hf.subfields:
                    Auth._cache[self.id][subfield.code] = self.heading_value(subfield.code)

        # auth attached records update
        def update_attached_records(auth):
            try:
                if auth.record_type != 'auth': raise Exception('Record type must be auth')

                for record in auth.list_attached():
                    def do_update():
                        try:
                            if isinstance(record, Auth):
                                # prevent feedback loops
                                if auth.id in [x.id for x in record.list_attached(usage_type='auth')]:
                                    record.commit(user=auth.user, auth_check=False, update_attached=False)
                                    return

                            # if the heading field tag changed, change the tag in linked record
                            if old_tag := Auth(previous_state).heading_field.tag:
                                if old_tag != self.heading_field.tag:
                                    for field in record.datafields:
                                        for subfield in filter(lambda x: hasattr(x, 'xref'), field.subfields):
                                            if subfield.xref == self.id:
                                                new_tag = field.tag[0] + self.heading_field.tag[1:]
                                                field.tag = new_tag

                            record.commit(user=auth.user, auth_check=False)
                        except Exception as err:
                            LOGGER.exception(err)
                            raise err

                        # for debugging
                        DB.handle['auth_linked_update_log'].insert_one({'record_type': record.record_type, 'record_id': record.id, 'action': 'updated', 'triggered_by': auth.id, 'time': datetime.now(timezone.utc)})

                    if 1: #DB.database_name == 'testing':
                        do_update()
                    else:
                        # subthreading here doesn't work ?
                        subthread = threading.Thread(target=do_update, args=[])
                        subthread.setDaemon(False) # stop the thread after complete
                        subthread.start()
            except Exception as err:
                    LOGGER.exception(err)
                    raise err

        if isinstance(self, Auth) and update_attached == True:
            if previous_state:
                    # only update attached record if the heading field changed
                    # don't check indicators
                    previous = Auth(previous_state)
                    linked_codes = Config.auth_linked_codes(self.heading_field.tag)
                    heading_serialized = [(x.code, x.value) for x in list(filter(lambda x: x.code in linked_codes, self.heading_field.subfields))]
                    prev_serialized = [(x.code, x.value) for x in list(filter(lambda x: x.code in linked_codes, previous.heading_field.subfields))]

                    if heading_serialized != prev_serialized or self.heading_field.tag != previous.heading_field.tag:
                        # the heading has changed
                        if DB.database_name == 'testing' or Config.threading == False: 
                            update_attached_records(self)
                        else:
                            thread4 = threading.Thread(target=update_attached_records, args=[self])
                            thread4.setDaemon(False) # stop the thread after complete
                            thread4.start()
        
        return self

    def delete(self, user='admin'):
        if isinstance(self, Auth):
            if self.in_use(usage_type='bib') or self.in_use(usage_type='auth'):
                raise AuthInUse()
        
            for cache in ('_cache', '_xcache', '_pcache', '_langcache'):
                getattr(Auth, cache)[self.id] = {}

        def update_browse_collections():
            try:
                # update browse index if necessary
                for field, values in self.logical_fields().items():
                    if field == '_record_type': 
                        continue
                    
                    updates = []

                    for val in values:
                        count = 0

                        if field in Config.bib_logical_fields:
                            count += len(list(DB.bibs.find({field: val}, limit=2, collation=Config.marc_index_default_collation)))
                        
                        if field in Config.auth_logical_fields:
                            count += len(list(DB.auths.find({field: val}, limit=2, collation=Config.marc_index_default_collation)))

                        if count in [0, 1]:
                            # this record is the only instance of the value
                            updates.append(DeleteOne({'_id': val}))

                    if updates:
                        DB.handle[f'_index_{field}'].bulk_write(updates)
            except Exception as err:
                LOGGER.exception(err)
                raise err

        if DB.database_name == 'testing' or Config.threading == False: 
            update_browse_collections()
        else:
            thread = threading.Thread(target=update_browse_collections, args=[])
            thread.setDaemon(False) # stop the thread after complete
            thread.start()
        
        history_collection = DB.handle[self.record_type + '_history']
        record_history = history_collection.find_one({'_id': self.id})

        if record_history is None:
            record_history = SON()
            record_history['history'] = [self.to_bson()]

        record_history['deleted'] = SON({'user': user, 'time': datetime.now(timezone.utc)})
        history_collection.replace_one({'_id': self.id}, record_history, upsert=True)    

        return type(self).handle().delete_one({'_id': self.id})

    def history(self):
        history_collection = DB.handle[self.record_type + '_history']
        record_history = history_collection.find_one({'_id': self.id})

        if record_history:
            return [type(self)(x) for x in record_history['history']]
        else:
            return []

    def logical_fields(self, *names):
        """Returns a dict of the record's logical fields"""
        
        self._logical_fields = {}
        logical_fields = getattr(Config, self.record_type + '_logical_fields') 
        
        for logical_field, tags in logical_fields.items():
            if names and logical_field not in names:
                continue
            
            for tag, subfield_groups in tags.items():
                for group in subfield_groups:
                    for field in self.get_fields(tag):
                        value = ' '.join(field.get_values(*group) or [])
                        
                        if value:
                            self._logical_fields.setdefault(logical_field, [])
                            self._logical_fields[logical_field].append(value)

        for subtype, match in Config.bib_type_map.items():
            if self.get_value(*match[:2]) == match[2]:
                self._logical_fields['_record_type'] = [subtype]
        
        self._logical_fields.setdefault('_record_type', ['default'])
        self._logical_fields['_record_type'].append(self.record_type)

        return self._logical_fields

    #### utlities

    def zmerge(self, to_merge):
            # sets any value from to_merge if the field doesn't exist in self
            # does not overwrite any values
        
            for field in to_merge.fields:
            
                if isinstance(field, Controlfield):
                    val = self.get_value(field.tag)
                    if val:
                        for pos in range(len(val)):
                            if val[pos] in [' ', '|']:
                                val[pos] = field.value[pos]       
                            self.set(field.tag, None, val)
                    else:
                    
                        self.set(field.tag, None, field.value)
                else:
                    for sub in field.subfields:
                        if not self.get_value(field.tag, sub.code):
                            self.set(field.tag, sub.code, sub.value)
        
            return self
            
    def xmerge(self, to_merge, overwrite=False):
        """Imports the fields from to_merge

        Positional arguments
        --------------------
        to_merge : dlx.marc.Marc

        Keyword Arguments
        -----------------
        overwrite : bool (default: False)
            If True, overwrites any fields with the same tag from self with 
            fields from to_merge. If False, adds the fields to self if the field
            doesn't already exist in self

        Returns
        -------
        self
        """

        assert isinstance(to_merge, Marc)

        diff = self.diff(to_merge)
        diffrec = type(self)()
        diffrec.fields = diff.b

        for tag in diffrec.get_tags():
            for i, field in enumerate(diffrec.get_fields(tag)):
                if isinstance(field, Controlfield):
                    if overwrite:
                        val = field.value

                        for pos in range(len(val)):
                            if val[pos] in [' ', '|']: 
                                val = val[:pos] + field.value[pos] + val[pos+1:]

                        self.set(field.tag, None, val, address=[i])

                    elif not self.get_value(field.tag):
                        self.fields.append(field)

                else:
                    for j, sub in enumerate(field.subfields):
                        if overwrite or not self.get_value(field.tag, sub.code, address=[i, j]):
                            self.set(field.tag, sub.code, getattr(sub, 'xref', None) or sub.value, address=[i, j])

        return self

    def diff(self, other):
        return Diff(self, other)

    def is_diff(self, other):
        """Returns True if the other record is different"""

        return Diff(self, other).different

    #### serializations

    def to_bson(self):
        bson = SON()
        bson['_id'] = self.id

        for tag in filter(lambda x: x[0:2] == '00', self.get_tags()):
            bson[tag] = [f.value for f in self.get_fields(tag)]

        for tag in filter(lambda x: x[0:2] != '00', self.get_tags()):
            bson[tag] = [f.to_bson() for f in self.get_fields(tag)]

        return bson

    def to_dict(self):
        d = {}
        d['_id'] = self.id

        for tag in filter(lambda x: x[0:2] == '00', self.get_tags()):
            d[tag] = [f.value for f in self.get_fields(tag)]

        for tag in filter(lambda x: x[0:2] != '00', self.get_tags()):
            fields = list(filter(lambda x: x.get('subfields'), [f.to_dict() for f in self.get_fields(tag)]))
            
            if fields:
                d[tag] = fields

        return d

    def to_json(self, to_indent=None):
        return json.dumps(self.to_dict(), indent=to_indent)

    def to_mij(self):
        mij = {}
        mij['leader'] = self.get_value('000')
        mij['fields'] = [field.to_mij() for field in self.get_fields()]

        return json.dumps(mij)

    def to_mrc(self, *tags, language=None, write_id=True):
        record = copy.deepcopy(self)

        if write_id:
            record.set('001', None, str(record.id))

        directory = ''
        data = ''
        next_start = 0
        field_terminator = u'\u001e'
        record_terminator = u'\u001d'

        for f in filter(lambda x: x.tag != '000', record.get_fields(*tags)):
            text = f.to_mrc(language=language)
            data += text
            field_length = len(text.encode('utf-8'))
            directory += f.tag + str(field_length).zfill(4) + str(next_start).zfill(5)
            next_start += field_length

        directory += field_terminator
        data += record_terminator
        leader_dir_len = len(directory.encode('utf-8')) + 24
        base_address = str(leader_dir_len).zfill(5)
        total_len = str(leader_dir_len + len(data.encode('utf-8'))).zfill(5)

        leader = record.get_value('000')

        if not leader:
            leader = '|' * 24
        elif len(leader) < 24:
            leader = record.leader.ljust(24, '|')

        new_leader = total_len \
            + leader[5:9] \
            + 'a' \
            + '22' \
            + base_address \
            + leader[17:20] \
            + '4500'        

        return new_leader + directory + data

    def to_mrk(self, *tags, language=None, write_id=True):
        record = copy.deepcopy(self) # so as not to alter the original object's data

        if write_id:
            record.set('001', None, str(record.id))
        
        return '\n'.join([field.to_mrk(language=language) for field in record.get_fields()]) + '\n'

    def to_str(self, *tags, language=None):
        # non-standard format intended to be human readable
        string = ''

        for field in self.get_fields(*tags):
            string += field.tag + '\n'

            if isinstance(field, Controlfield):
                string += '   ' + field.value + '\n'
            else:
                for sub in field.subfields:
                    if language and Config.linked_language_source_tag(self.record_type, field.tag, sub.code, language):
                        val = sub.translated(language)
                    else:
                        val = sub.value

                    string += '   ' + sub.code + ': ' + val + '\n'

        return string

    def to_xml_raw(self, *tags, language=None, xref_prefix='', write_id=True):
        record = copy.deepcopy(self) # so as not to alter the orginal object's underlying data

        if write_id:
            record.set('001', None, str(record.id))
        
        # todo: reimplement with `xml.dom` or `lxml` to enable pretty-printing
        root = ElementTree.Element('record')

        for field in record.get_fields(*tags):
            if isinstance(field, Controlfield):
                node = ElementTree.SubElement(root, 'controlfield')
                node.set('tag', field.tag)
                node.text = field.value
            else:
                node = ElementTree.SubElement(root, 'datafield')
                node.set('tag', field.tag)
                node.set('ind1', field.ind1)
                node.set('ind2', field.ind2)

                xref = None

                for sub in field.subfields:
                    val = sub.value
                    
                    if not val:
                        continue
                    
                    if hasattr(sub, 'xref'):
                        xref = sub.xref

                    subnode = ElementTree.SubElement(node, 'subfield')
                    subnode.set('code', sub.code)

                    if language and Config.linked_language_source_tag(record.record_type, field.tag, sub.code, language):
                        subnode.text = sub.translated(language)
                        continue   

                    subnode.text = val

                if xref:
                    subnode = ElementTree.SubElement(node, 'subfield')
                    subnode.set('code', '0')
                    subnode.text = xref_prefix + str(xref)

        return root

    def to_xml(self, *tags, language=None, xref_prefix='', write_id=True):
        return ElementTree.tostring(self.to_xml_raw(write_id= write_id, language=language, xref_prefix=xref_prefix), encoding='utf-8').decode('utf-8')

    def to_jmarcnx(self):
        xrec = type(self)()

        if self.id:
            xrec.id = self.id

        xrec.fields += self.controlfields

        for field in self.datafields:
            for i, subfield in enumerate(field.subfields):
                if isinstance(subfield, Linked):
                    new_subfield = Literal(subfield.code, subfield.value)
                    field.subfields[i] = new_subfield

            xrec.fields.append(field)

        return xrec.to_json()

    #### de-serializations

    def from_mij(self, string):
        pass

    @classmethod
    def from_mrc(cls, string):
        '''Parses a MARC21 string (.mrc) into dlx.Marc'''
        
        self = cls()
        base = string[12:17]
        directory = string[24:int(base)-24]
        data = string[base:]

        while directory:
            tag = directory[:3]
            length = directory[3:7]
            start = directory[7:12]
            
            directory = directory[12:]

    @classmethod
    def from_mrk(cls, string: str, auth_control=True, delete_subfield_zero=True):
        self = cls()

        for line in filter(None, string.split('\n')):
            match = re.match(r'=(\w{3})  (.*)', line)            
            tag, rest = match.group(1), match.group(2)
            if tag == 'LDR': tag = '000'

            if tag[:2] == '00':
                field = Controlfield(tag, rest)
            else:
                ind1, ind2 = [x.replace('\\', ' ') for x in rest[:2]]
                field = Datafield(record_type=cls.record_type, tag=tag, ind1=ind1, ind2=ind2)
                ambiguous = []
                
                # capture the xref from subfield $0, if exists
                if match := re.search('\$0(\d+)', rest[2:]):
                    xref = int(match.group(1))
                else:
                    xref = None

                # parse the subfields
                for chunk in filter(None, rest[2:].split('$')):
                    code, value = chunk[0], chunk[1:]

                    if Config.is_authority_controlled(self.record_type, tag, code):
                        value = xref if xref else value

                    try:
                        field.set(code, value, place='+', auth_control=auth_control)
                    except(AmbiguousAuthValue):
                        ambiguous.append(Literal(code, value))
                
                # attempt to use multiple subfields to resolve ambiguity
                if ambiguous:
                    if xref := Auth.resolve_ambiguous(tag, ambiguous, record_type=self.record_type):
                        field.set(code, xref, place='+', auth_control=auth_control)
                    else:
                        raise AmbiguousAuthValue(self.record_type, field.tag, '*', str([x.to_str() for x in ambiguous]))
            
                # remove subfield $0
                if delete_subfield_zero:
                    field.subfields = list(filter(lambda x: x.code != '0', field.subfields))

            self.fields.append(field)

        return self
    
    @classmethod
    def from_xml_raw(cls, root: ElementTree.Element, *, auth_control=True, delete_subfield_zero=True):
        if DB.database_name == 'testing' or Config.threading == False:
            Auth({'_id': 1}).set('150', 'a', 'Header').commit()

        assert isinstance(root, ElementTree.Element)
        self = cls()
            
        for node in filter(lambda x: re.search('controlfield$', x.tag), root):
            self.set(node.attrib['tag'], None, node.text)

        for field_node in filter(lambda x: re.search('datafield$', x.tag), root):
            tag = field_node.attrib['tag']
            field = Datafield(record_type=cls.record_type, tag=tag, ind1=field_node.attrib['ind1'], ind2=field_node.attrib['ind2'])
            tag_nodes = filter(lambda x: re.search('subfield$', x.tag), field_node)

            # capture the xref if any
            xref = None

            for subfield_node in copy.deepcopy(tag_nodes): # use a copy to prevent the iterating orginal
                if subfield_node.attrib['code'] == '0':
                    xref = int(''.join([x for x in subfield_node.text if ord(x) >= 48 and ord(x) <= 57]))

            # iterate though nodes and set the marc values
            ambiguous = []

            for subfield_node in tag_nodes:
                code = subfield_node.attrib['code']

                if auth_control and Config.is_authority_controlled(self.record_type, field.tag, subfield_node.attrib['code']):
                    value = xref if xref else subfield_node.text
                else:
                    value = str(subfield_node.text)

                # .set handles auth control. 
                try:
                    field.set(code, value, auth_control=auth_control, place='+')
                except(AmbiguousAuthValue):
                    ambiguous.append(Literal(code, value))

                # attempt to use multiple subfields to resolve ambiguity
                if ambiguous:
                    if xref := Auth.resolve_ambiguous(tag, ambiguous, record_type=self.record_type):
                        field.set(code, xref, auth_control=auth_control, place='+')
                    else:
                        raise AmbiguousAuthValue(self.record_type, tag, '*', str([x.to_str() for x in ambiguous]))
            
            if delete_subfield_zero:
                field.subfields = list(filter(lambda x: x.code != '0', field.subfields))
                
            self.fields.append(field)
            
        return self
        
    @classmethod
    def from_xml(cls, string, auth_control=True):
        return cls.from_xml_raw(ElementTree.fromstring(string), auth_control=auth_control)

    @classmethod
    def from_json(cls, string, auth_control=False):
        return cls(doc=json.loads(string), auth_control=auth_control)

class Bib(Marc):
    record_type = 'bib'
    set_class = BibSet

    def __init__(self, doc={}, **kwargs):
        self.record_type = 'bib'
        super().__init__(doc, **kwargs)

    #### shorctuts

    def symbol(self):
        return self.get_value('191', 'a')

    def symbols(self):
        return self.get_values('191', 'a')

    def title(self):
        return ' '.join(self.get_values('245', 'a', 'b', 'c'))

    def date(self):
        return self.get_value('269', 'a')

    #### files

    def files(self, *langs):
        if langs:
            langs = [langs]
        else:
            langs = ['AR', 'ZH', 'EN', 'FR', 'ES', 'RU', 'DE']

        symbol = self.symbol()
        
        files = [File.latest_by_identifier_language(Identifier('symbol', symbol), lang) for lang in langs]

        return [f.uri for f in filter(None, files)]

    def file(self, lang):
        symbol = self.symbol()

        return File.latest_by_identifier_language(Identifier('symbol', symbol), lang).uri

class Auth(Marc):
    record_type = 'auth'
    set_class= AuthSet
    _cache = {}
    _xcache = {}
    _pcache = {}
    _langcache = {}
    _acache = {}

    @classmethod
    def build_cache(cls):
        '''Stores all auth headings to the the lookup cache for use in long
        batch operations'''

        total, chars = DB.auths.estimated_document_count(), 0

        for i, auth in enumerate(AuthSet.from_query({})):
            j = i + 1

            for subfield in auth.heading_field.subfields:
                if auth.id in Auth._cache:
                    Auth._cache[auth.id][subfield.code] = subfield.value
                else:
                    Auth._cache[auth.id] = {subfield.code: subfield.value}

            if j % 1000 == 0 or j == total:
                status = f'Building auth cache: {j} / {total}'
                print(('\b' * chars) + status, end='', flush=True)
                chars = len(status)

        print('\n')

    @classmethod
    def lookup(cls, xref, code, language=None):
        '''Returns the authotiry controlled value for an xref and subfield code'''

        if language:
            cached = Auth._langcache.get(xref, {}).get(code, {}).get(language, None)
        else:
            cached = Auth._cache.get(xref, {}).get(code, None)
            
        if cached:
            return cached
            
        label_tags = Config.auth_heading_tags()
        label_tags += Config.auth_language_tags() if language else []
        auth = Auth.from_query({'_id': xref}, projection=dict.fromkeys(label_tags, 1))
        value = auth.heading_value(code, language) if auth else None

        if language:
            Auth._langcache[xref] = {code: {language: value}}
        else:
            if xref in Auth._cache:
                Auth._cache[xref][code] = value
            else:
                Auth._cache[xref] = {code: value}

        return value

    @classmethod
    def xlookup(cls, tag, code, value, *, record_type):
        '''Returns all the xrefs that match a given value in the given tag and subfield code'''

        auth_tag = Config.authority_source_tag(record_type, tag, code)

        if auth_tag is None:
            return

        if cached := Auth._xcache.get(value, {}).get(auth_tag, {}).get(code, None):
            return cached

        query = Query(Condition(auth_tag, {code: value}, record_type='auth'))
        auths = AuthSet.from_query(query.compile(), projection={'_id': 1})
        xrefs = [r.id for r in list(auths)]

        Auth._xcache.setdefault(value, {}).setdefault(auth_tag, {})[code] = xrefs

        return xrefs

    @classmethod
    def xlookup_multi(cls, tag, subfields, *, record_type):
        '''Lookup by multiple subfields'''
        
        # pairs of code and value {'a': 'val1', 'b': 'val2'}
        assert [isinstance(x, Subfield) for x in subfields]
        auth_tag = Config.authority_source_tag(record_type, tag, subfields[0].code) # assume same source tag

        if auth_tag is None:
            return

        values = ''.join([x.value for x in subfields])
        cached = Auth._xcache.get('__multi__', {}).get(values, {}).get(auth_tag, {})
        
        if cached:
            return cached

        query = Query(Condition(auth_tag, dict(zip([x.code for x in subfields], [x.value for x in subfields])), record_type='auth'))
        auths = AuthSet.from_query(query.compile(), projection={'_id': 1})
        xrefs = [r.id for r in list(auths)]
        Auth._xcache.setdefault('__multi__', {}).setdefault(values, {})[auth_tag] = xrefs

        return xrefs

    @classmethod
    def resolve_ambiguous(cls, *, tag: str, subfields: list['Subfield'], record_type: str) -> int:
        '''Determines if there is an exact authority match for specific subfields'''

        subfields_str = str([(x.code, x.value) for x in subfields])

        if xref := Auth._acache.get(subfields_str):
            return xref

        if matches := cls.xlookup_multi(tag, subfields, record_type=record_type):
            if len(matches) == 1:
                Auth._acache.setdefault(subfields_str, matches[0])

                return matches[0]
            elif len(matches) > 1:
                exact_matches = []

                for xref in matches:
                    auth_subfields = cls.from_id(xref).heading_field.subfields
                    auth_subfields = [(x.code, x.value) for x in auth_subfields]

                    if [(x.code, x.value) for x in subfields] == auth_subfields:
                        exact_matches.append(xref)

                    if exact_matches:
                        Auth._acache.setdefault(subfields_str, exact_matches[0])

                        return exact_matches[0]
                
        return None

    @classmethod
    def partial_lookup(cls, tag, code, string, *, record_type, limit=25):
        """Returns a list of tuples containing the authority-controlled values
        that match the given string

        Positional arguments
        --------------------
        tag : str
        code : str
        string : str
            The string to match against

        Keyword arguments
        -----------------
        record_type : 'bib' or 'auth'
        limit : int
            Limits the results. Default is 25

        Returns
        -------
        List(Tuple)
            A list of pairs, the first element in the pair is the string value
            and the second is the xref#
        """

        cached = Auth._pcache.get(tag, {}).get(code, {}).get(string, None)

        if cached:
            return cached

        auth_tag = Config.authority_source_tag(record_type, tag, code)

        if auth_tag is None:
            return

        query = Query(Condition(auth_tag, {code: Regex(string, 'i')}))
        auths = AuthSet.from_query(
            query.compile(),
            #collation=Collation(locale='en', strength=2),
            projection=dict.fromkeys(Config.auth_heading_tags(), 1),
            limit=limit
        )
        results = list(auths)
        
        Auth._pcache.setdefault(tag, {}).setdefault(code, {}).update({string: results})

        return results

    def __init__(self, doc={}, **kwargs):
        self.record_type = 'auth'
        self._heading_field = None
        super().__init__(doc, **kwargs)

    @property    
    def heading_field(self):
        """Returns the heading field of the authority record.
        
        Returns
        -------
        dlx.marc.Datafield
        """
            
        self._heading_field = next(filter(lambda field: field.tag[0:1] == '1', self.fields), None)
        
        return self._heading_field

    def heading_value(self, code, language=None):
        """Returns the value of the specified subfield of the heading field of 
        the authority record.
        
        Parameters
        ----------
        code : str
            The code of the subfield of the heading field to get.
        
        Returns
        -------
        str
        """
        
        if language:
            tag = self.heading_field.tag
            lang_tag = Config.language_source_tag(tag, language)
            source_field = self.get_field(lang_tag)

            if source_field is None:
                return '**Linked Auth Translation Not Found**'
        else:
            source_field = self.heading_field

            if source_field is None:
                return '**Linked Auth Label Not Found**'

        for sub in filter(lambda sub: sub.code == code, source_field.subfields):
            return sub.value

    def in_use(self, *, usage_type=None):
        """Returns the count of records using the authority.
        
        Parameters
        ---------
        usage_type : ("bib"|"auth"), None
            If None, counts total use in both
        
        Returns
        -------
        int
        """
        
        if not self.id:
            return
        
        def count(lookup_class, xref):
            tags = list(Config.bib_authority_controlled.keys()) if lookup_class == Bib else list(Config.auth_authority_controlled.keys())
            set_class = BibSet if lookup_class == Bib else AuthSet

            return set_class.from_query({'$or': [{f'{tag}.subfields.xref': xref} for tag in tags]}).count

        if usage_type is None:
            total = 0
            
            for cls in (Bib, Auth):
                total += count(cls, self.id)
                
            return total
        if usage_type == 'bib':
            return count(Bib, self.id)
        elif usage_type == 'auth':
            return count(Auth, self.id)          
        else:    
            raise Exception('Invalid usage_type')

    def list_attached(self, usage_type=None):
        """List the records attached to this auth record"""

        def list_records(lookup_class, xref: int) -> list[Marc]:
            tags = list(Config.bib_authority_controlled.keys()) if lookup_class == BibSet else list(Config.auth_authority_controlled.keys())
            return [record for tag in tags for record in lookup_class.from_query({f'{tag}.subfields.xref': xref})]

        if usage_type == 'bib':
            return list_records(BibSet, self.id)
        elif usage_type == 'auth':
            return list_records(AuthSet, self.id)
        elif usage_type is None:
            return [record for cls in (BibSet, AuthSet) for record in list_records(cls, self.id)]
        else:
            raise Exception('Invalid "usage_type"')

    def merge(self, *, user, losing_record):
        if not isinstance(losing_record, Auth):
            raise Exception("Losing record must be of type Auth")

        # for debugging
        DB.handle['merge_log'].insert_one({'record_type': 'auth', 'record_id': losing_record.id, 'action': 'losing', 'time': datetime.now(timezone.utc)})
        DB.handle['merge_log'].insert_one({'record_type': 'auth', 'record_id': self.id, 'action': 'gaining', 'time': datetime.now(timezone.utc)})

        def update_records(record_type, gaining, losing):
            authmap = getattr(Config, f'{record_type}_authority_controlled')
            
            # build query for all records that are linked to the losing auth
            conditions = []
                 
            for ref_tag, d in authmap.items():
                #for subfield_code, auth_tag in d.items():
                #    if auth_tag == losing.heading_field.tag:
                
                conditions.append(Raw({f'{ref_tag}.subfields.xref': losing.id}, record_type=record_type))

            if len(conditions) == 0:
                return 0
            
            cls = BibSet if record_type == 'bib' else AuthSet
            query = Query(Or(*conditions))
            
            #  update the records linked to the losing auth with the gaining auth id
            changed = 0

            for record in cls.from_query(query):
                state = record.to_bson()
                
                for i, field in enumerate(record.fields):
                    if isinstance(field, Datafield):
                        for subfield in field.subfields:
                            if hasattr(subfield, 'xref') and subfield.xref == losing.id:
                                subfield.xref = gaining.id
                        
                                if field in record.fields[0:i] + record.fields[i+1:]:
                                    del record.fields[i] # duplicate field

                if record.to_bson() != state:
                    # the record has actually changed

                    def do_commit():
                        # we can skip the auth validation because these records should already be validated
                        # Wrapping this in a try/except block and sending the exception to a configured log
                        # surfaces exceptions that occur in a thread. This is the only threaded function.
                        try:
                            record.commit(user=user, auth_check=False)
                        except Exception as err:
                            LOGGER.exception(err)
                            raise err

                        # for debugging
                        DB.handle['merge_log'].insert_one({'record_type': record_type, 'record_id': record.id, 'action': 'updated', 'time': datetime.now(timezone.utc)})

                    t = threading.Thread(target=do_commit, args=[])
                    t.setDaemon(False) # stop the thread after complete
                    t.start()
                
                changed += 1

            return changed
        
        changed = 0
        
        for record_type in ('bib', 'auth'):
            changed += update_records(record_type, self, losing_record)
        
        i = 0

        while changed > 0 and losing_record.in_use(usage_type='bib') or losing_record.in_use(usage_type='auth'):
            # wait for all the links to be updated, otherwise the delete fails
            i += 1

            if i > 1200:
                raise Exception("The merge is taking too long (> 1200 seconds)")

            time.sleep(1)

        losing_record.delete(user)

        # add to history
        DB.handle['auth_history'].update_one({'_id': losing_record.id}, {'$set': {'merged': {'into': self.id, 'time': datetime.now(timezone.utc)}}})

        # for debugging
        DB.handle['merge_log'].insert_one({'record_type': 'auth', 'record_id': losing_record.id, 'action': 'deleted', 'time': datetime.now(timezone.utc)})
        DB.handle['merge_log'].insert_one({'record_type': 'auth', 'record_id': self.id, 'action': 'merge complete', 'time': datetime.now(timezone.utc)})

class Diff():
    """Compare two Marc objects.

    Atrributes
    ----------
    records : list(dlx.marc.Marc)
        List of the two records being compared
    different : bool
        True if the records are different, else False
    same :
        True if the records are the same, else False
    a : list(dlx.marc.Field)
        The fields unique to record "a"
    b : list(dlx.marc.Field)
        The fields unique to record "b"
    c : list(dlx.marc.Field)
        The fields common to both records
    d : list(dlx.marc.Field)
        Fields that are common to both records but in a different order
    e : list(dlx.marc.Field)
        Fields that are duplicated in both records a different number of times
    """

    def __init__(self, a: Marc, b: Marc):
        assert all([isinstance(x, Marc) for x in (a, b)])
        self.records = (a, b)

        # fields unique to record a
        self.a = list(filter(lambda x: x not in b.fields, a.fields))
        
        # fields unique to record b
        self.b = list(filter(lambda x: x not in a.fields, b.fields))
        
        # fields common to both records
        self.c = list(filter(lambda x: x in b.fields, a.fields))
        
        # field orders are different
        self.d = [x for x in self.c if self.records[0].get_fields(x.tag).index(x) != self.records[1].get_fields(x.tag).index(x)]

        # fields that are duplicated a different number of times 
        a_fields = Counter([x.to_mrk() for x in a.fields])
        b_fields = Counter([x.to_mrk() for x in b.fields])

        self.e = [field for field in self.c if a_fields[field.to_mrk()] != b_fields[field.to_mrk()]]

        # boolean record equality check
        self.different = True if self.a or self.b or self.d or self.e else False
        self.same = not self.different

class History():
    def __init__(self):
        pass

    @classmethod
    def from_query(cls, query: Query, **kwargs) -> typing.Generator[None, CursorType, Marc]:
        '''Yields history reords that mtch the query as Marc objects'''

        self = cls()
        handle = DB.handle[self.record_type + '_history']


        for doc in handle.find({'history': {'$elemMatch': query.compile()}}, **kwargs):
            for version in doc['history']:
                yield self.record_class(version)

    @classmethod
    def find_deleted(cls, query: Query, **kwargs) -> typing.Generator[None, CursorType, int]:
        '''Yields the ids of deleted records matching the query'''

        self = cls()
        handle = DB.handle[self.record_type + '_history']

        for doc in handle.find({'history': {'$elemMatch': query.compile()}}, **kwargs):
            if doc.get('deleted'):
                yield doc['_id']

    @classmethod
    def deleted_by_date(cls, date_from: datetime, date_to: datetime = datetime.now(timezone.utc)) -> typing.Generator[None, CursorType, Marc]:
        '''Yields the ids of records delete between the given dates'''

        self = cls()
        handle = DB.handle[self.record_type + '_history']

        for doc in handle.find({'deleted.time': {'$gte': date_from, '$lt': date_to}}):
            if d:= doc.get('deleted'):
                yield doc['_id']

class BibHistory(History):
    def __init__(self):
        self.record_class = Bib
        self.record_type = 'bib'
        super().__init__()

class AuthHistory(History):
    def __init(self):
        self.record_class = Auth
        self.record_type = 'auth'
        super().__init__()

### Field classes

class Field():
    def __init__(self):
        raise Exception('Cannot instantiate fom base class')

    def to_bson(self):
        raise Exception('This is a stub')

class Controlfield(Field):
    def __eq__(self, other):
        if not isinstance(other, Controlfield):
            return False
        
        if self.tag != other.tag:
            return False

        return self.value == other.value

    def __init__(self, tag, value, record_type=None):
        self.record_type = record_type
        self.tag = tag
        self.value = value

    def set(self, value):
        self.value = value

    def to_mij(self):
        return {self.tag: self.value}

    def to_mrc(self, term=u'\u001e', language=None):
        return self.value + term

    def to_mrk(self, language=None):
        return '={}  {}'.format(self.tag, self.value)

class Datafield(Field):
    def __eq__(self, other):
        if not isinstance(other, Datafield):
            return False

        if self.tag != other.tag:
            return False
        
        return self.to_dict() == other.to_dict()

    @classmethod
    def from_dict(cls, *, record_type, tag, data, auth_control=False):
        self = cls()
        self.record_type = record_type
        self.tag = tag

        self.ind1 = data['indicators'][0]
        self.ind2 = data['indicators'][1]

        assert len(data['subfields']) > 0
        
        for sub in data['subfields']:
            if 'xref' in sub:
                if auth_control:
                    self.set(sub['code'], int(sub['xref']), place='+')
                else:
                    self.subfields.append(Linked(sub['code'], sub['xref']))
            elif 'value' in sub:
                if auth_control:
                    self.set(sub['code'], str(sub['value']), place='+')
                else:
                    self.subfields.append(Literal(sub['code'], sub['value']))
            else:
                raise ValueError

        return self

    @classmethod
    def from_json(cls, *args, **kwargs):
        kwargs['data'] = json.loads(kwargs['data'])

        return cls.from_dict(*args, **kwargs)

    @classmethod
    def from_jmarcnx(cls, *args, **kwargs):
        return cls.from_json(*args, **kwargs)

    def __init__(self, tag=None, ind1=None, ind2=None, subfields=None, record_type=None):
        self.record_type = record_type
        self.tag = tag
        self.ind1 = ind1 or ' '
        self.ind2 = ind2 or ' '
        self.subfields = subfields or []

    @property
    def indicators(self):
        return [self.ind1, self.ind2]

    def get_value(self, code, *, place=0):
        sub = self.get_subfield(code, place=place)

        return sub.value if sub else ''

    def get_values(self, *codes):
        values = []

        for code in codes:
            i = 0

            while self.get_value(code, place=i):
                values.append(self.get_value(code, place=i))
                i += 1

        return values

    def get_xrefs(self):
        return list(set([sub.xref for sub in filter(lambda x: hasattr(x, 'xref'), self.subfields)]))

    def get_xref(self, code):
        return next((sub.xref for sub in filter(lambda x: hasattr(x, 'xref') and x.code == code, self.subfields)), None)

    def get_subfields(self, code):
        return filter(lambda x: x.code == code, self.subfields)

    def get_subfield(self, code, place=None):
        if place is None or place == 0:
            return next(self.get_subfields(code), None)

        for i, sub in enumerate(self.get_subfields(code)):
            if i == place:
                return sub

    def set(self, code, new_val, *, ind1=None, ind2=None, place=0, auth_control=True):
        if not new_val and not ind1 and not ind2:
            return self
            
        if auth_control and not self.record_type:
            raise Exception('Datafield attribute "record_type" must be set to determine authority control')

        if auth_control == True and Config.is_authority_controlled(self.record_type, self.tag, code):
            if isinstance(new_val, int):
                xref = new_val

                if not Auth.lookup(xref, code): 
                    raise InvalidAuthXref(self.record_type, self.tag, code, new_val)

            else:
                xrefs = Auth.xlookup(self.tag, code, new_val, record_type=self.record_type)

                if len(xrefs) == 0:
                    raise InvalidAuthValue(self.record_type, self.tag, code, new_val)
                elif len(xrefs) > 1:
                    raise AmbiguousAuthValue(self.record_type, self.tag, code, new_val)

                new_val = xrefs[0]
        else:
            auth_control = False

        if new_val:
            # existing subfield
            if auth_control == False:
                # force string because only xrefs can be ints
                new_val = str(new_val)

            # walk the tree to replace the subfield object
            i, j = 0, 0
            
            for i, sub in enumerate(self.subfields):
                
                if sub.code == code:
                    if j == place:
                        self.subfields[i] = Linked(code, new_val) if auth_control else Literal(code, new_val)
            
                        return self
                    
                    j += 1

            # new subfield
            if place in ('+', 0):
                self.subfields.append(Linked(code, new_val) if auth_control else Literal(code, new_val))
            elif place > j or not isinstance(place, int):
                raise Exception(f'Invalid subfield place {place}')

        if ind1: self.ind1 = ind1
        if ind2: self.ind2 = ind2

        return self

    def to_bson(self):
        b = SON()
        b['indicators'] = self.ind1, self.ind2,
        b['subfields'] = [sub.to_bson() for sub in self.subfields]

        return b

    def to_dict(self):
        d = {}        
        d['indicators'] = [self.ind1, self.ind2]
        d['subfields'] = [sub.to_dict() for sub in filter(lambda x: x.value, self.subfields)]

        return d

    def to_json(self, to_indent=None):
        return json.dumps(self.to_dict(), indent=to_indent)

    def to_mij(self):
        mij = {self.tag: {}}

        mij[self.tag]['ind1'] = self.ind1
        mij[self.tag]['ind2'] = self.ind2

        subs = []

        for sub in self.subfields:
            subs.append({sub.code : sub.value})

        mij[self.tag]['subfields'] = subs

        return mij

    def to_mrc(self, delim=u'\u001f', term=u'\u001e', language=None):
        string = self.ind1 + self.ind2

        for sub in self.subfields:
            if language and Config.linked_language_source_tag(self.record_type, self.tag, sub.code, language):
                value = sub.translated(language)
            else: 
                value = sub.value or ''

            string += ''.join([delim + sub.code + value])

        return string + term

    def to_mrk(self, language=None):
        field = copy.deepcopy(self) # so as not to alter the original object's data
        inds = field.ind1 + field.ind2
        inds = inds.replace(' ', '\\')
        inds = inds.replace('_', '\\')

        # add first xref found to $0 if $0 doesn't already exist
        if subfield := next(filter(lambda x: hasattr(x, 'xref'), field.subfields), None):
            if field.get_subfield('0') is None:
                field.subfields.append(Literal('0', str(subfield.xref)))

        string = f'={field.tag}  {inds}'

        for sub in field.subfields:
            if language and Config.linked_language_source_tag(self.record_type, field.tag, sub.code, language):
                value = sub.translated(language)
            else: 
                value = sub.value

            string += f'${sub.code}{value}'

        return string

### Subfield classes

class Subfield():
    def __init__(self):
        raise Exception('Cannot instantiate fom base class')

    def to_bson(self):
        raise Exception('This is a stub')

    @classmethod
    def is_linked(cls):
        if cls.__name__ == 'Linked':
            return True
        else:
            return False

class Literal(Subfield):
    def __init__(self, code, value):
        self.code = code
        self.value = value

    def to_bson(self):
        b = SON()
        b['code'], b['value'] = self.code, self.value

        return b

    def to_dict(self):
        return {'code': self.code, 'value': self.value}

class Linked(Subfield):
    def __init__(self, code, xref):
        self.code = code
        self.xref = int(xref)
        self._value = None

    @property
    def value(self):
        value = Auth.lookup(self.xref, self.code)
        
        if not value:
            warn(f'Linked authority {self.xref} not found')
            
        return value

    def translated(self, language):
        return Auth.lookup(self.xref, self.code, language)

    def to_bson(self):
        b = SON()
        b['code'], b['xref'] = self.code, self.xref

        return b

    def to_dict(self):
        return {'code': self.code, 'value': self.value, 'xref': self.xref}

### Matcher classes
# deprecated

class Matcher(Condition):
    # for backwards compatibility

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        warn('dlx.marc.Matcher is deprecated. Use dlx.marc.Condition instead')

class OrMatch(Or):
    # for backwards compatibility

    def __init__(self, *matchers):
        super().__init__(*matchers)
        self.matchers = matchers

        warn('dlx.marc.OrMatch is deprecated. Use dlx.marc.query.Or instead')

# end
