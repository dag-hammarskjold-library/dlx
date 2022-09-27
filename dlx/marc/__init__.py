"""dlx.marc"""

import re, json, threading
from datetime import datetime
from warnings import warn
from xml.etree import ElementTree
import jsonschema
from bson import SON, Regex
from pymongo import ReturnDocument, UpdateOne
from pymongo.collation import Collation
from dlx.config import Config
from dlx.db import DB
from dlx.file import File, Identifier
from dlx.marc.query import QueryDocument, Query, Condition, Or, TagOnly
from dlx.util import Table

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
        
        self.records = map(lambda r: Marc(r, auth_control=ac), self.handle.find(*args, **kwargs))

        return self

    @classmethod
    def from_ids(cls, ids):
        return cls.from_query({'_id' : {'$in': ids}})

    @classmethod
    def from_table(cls, table, auth_control=True, auth_flag=False, field_check=None):
        # does not support repeated subfield codes
        self = cls()
        self.records = []
        exceptions = []

        for temp_id in table.index.keys():
            record = cls().record_class()

            for field_name in table.index[temp_id].keys():
                instance = 0
                value = table.index[temp_id][field_name]

                if value == '':
                    continue

                match = re.match(r'^(([1-9]+)\.)?(\d{3})(\$)?([a-z0-9])', str(field_name))

                if match:
                    if match.group(1):
                        instance = int(match.group(2))
                        instance -= 1 # place numbers start at 1 in col headers instead of 0

                    tag, code = match.group(3), match.group(5)
                elif len(field_name) == 3 and field_name[0:2] == '00':
                    tag, code = field_name, None
                else:
                    exceptions.append('Invalid column header "{}"'.format(field_name))
                    continue

                if record.get_value(tag, code, address=[instance,0]):
                    exceptions.append('Column header {}.{}{} is repeated'.format(instance, tag, code))
                    continue

                if field_check and field_check == tag + (code or ''):
                    if self.record_class.find_one(Condition(tag, {code: value}).compile()):
                        exceptions.append('{}${}: "{}" is already in the system'.format(tag, code, value))
                        continue

                if record.get_field(tag, place=instance):
                    try:
                        record.set(tag, code, value, address=[instance], auth_control=auth_control)
                    except Exception as e:
                        exceptions.append(str(e))
                else:
                    try:
                        record.set(tag, code, value, address=['+'], auth_control=auth_control)
                    except Exception as e:
                        exceptions.append(str(e))

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
        
        for r in root.findall('record'):
            self.records.append(self.record_class.from_xml_raw(r, auth_control=auth_control))
            
        return self
        
    @classmethod
    def from_xml(cls, string):
        return cls.from_xml_raw(ElementTree.fromstring(string))
    
    # instance

    def __iter__(self): return self
    def __next__(self): return next(self.records)

    def __init__(self, records=[]):
        self.records = records # can be any type of iterable

    @property
    def count(self):
        import types
        
        if isinstance(self.records, (map, types.GeneratorType)):
            args, kwargs = self.query_params

            if args[0] or kwargs.get('skip') or kwargs.get('limit'):
                self._count = self.handle.count_documents(*args, **kwargs)
            else:
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

    def to_mrc(self):
        # todo: stream instead of queue in memory
        mrc = ''

        for record in self.records:
            mrc += record.to_mrc()

        return mrc

    def to_xml(self, xref_prefix=''):
        # todo: stream instead of queue in memory
        root = ElementTree.Element('collection')

        for record in self.records:
            root.append(record.to_xml_raw(xref_prefix=xref_prefix))

        return ElementTree.tostring(root, encoding='utf-8').decode('utf-8')

    def to_mrk(self):
        return '\n'.join([r.to_mrk() for r in self.records])
        
    def to_str(self):
        return '\n'.join([r.to_str() for r in self.records])

    def to_excel(self, path):
        pass

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
        self.id = int(doc['_id']) if '_id' in doc else None
        self.updated = doc['updated'] if 'updated' in doc else None
        self.user = doc['user'] if 'user' in doc else None
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
        cat_date = datetime.utcnow().strftime('%y%m%d')

        self.set('008', None, cat_date + text[6] + pub_year + text[11:])

    def delete_field(self, tag, place=0):
        if isinstance(place, int):
            i, j = 0, 0

            for i, field in enumerate(self.fields):
                if field.tag == tag:
                    if j == place:
                        del self.fields[i]

                        return

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
    def commit(self, user='admin', auth_check=True):
        new_record = True if self.id is None else False
        
        if new_record:
            self.id = type(self)._increment_ids()
        
        self.validate()
        data = self.to_bson()
        self.updated = data['updated'] = datetime.utcnow()
        self.user = data['user'] = user
       
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
        
        # maintenance functions
        # update field text indexes
        def index_field_text():
            for i, field in enumerate(filter(lambda x: isinstance(x, Datafield), self.fields)):
                # tag indexes
                tag_col = DB.handle[f'_index_{field.tag}']
                text = ' '.join([subfield.value for subfield in field.subfields])

                updates = [
                    UpdateOne(
                        {'_id': text},
                        {'$addToSet': {'subfields': {'code': subfield.code, 'value': subfield.value}}},
                        upsert=True
                    ) for subfield in field.subfields
                ]

                tag_col.bulk_write(updates)

                # create text index if it doesn't exist
                for k, v in tag_col.index_information().items():
                    if v['key'][0][0] == '_fts':
                        pass #col.drop(k)
                    else:
                        tag_col.create_index([('subfields.value', 'text')])

        thread1 = threading.Thread(target=index_field_text, args=[])
        thread1.setDaemon(False) # stop the thread after complete
        thread1.start()
        
        # add logical fields
        def calculate_logical_fields():
            for logical_field, values in self.logical_fields().items():
                if logical_field == '_record_type':
                    continue

                data[logical_field] = values

                # browse indexes
                updates = [
                    UpdateOne(
                        {'_id': val},
                        {
                            # '$setOnInsert': {'_id': val}, # ? is this an optimization?
                            '$addToSet': {'_record_type': self.logical_fields()['_record_type'][0]} # there is  only one record type in the array
                        },
                        upsert=True
                    ) for val in values
                ]

                DB.handle[f'_index_{logical_field}'].bulk_write(updates)

        thread2 = threading.Thread(target=calculate_logical_fields, args=[])
        thread2.setDaemon(False) # stop the thread after complete
        thread2.start()

        # history
        def save_history():
            history_collection = DB.handle[self.record_type + '_history']
            record_history = history_collection.find_one({'_id': self.id})

            if record_history:
                record_history.setdefault('history', []) # record may not have history if migrated form another db
                record_history['history'].append(data)
            else:
                record_history = SON()
                record_history['_id'] = self.id

                if new_record:
                    record_history['created'] = SON({'user': user, 'time': datetime.utcnow()})

                record_history['history'] = [data]

            history_collection.replace_one({'_id': self.id}, record_history, upsert=True)

        thread3 = threading.Thread(target=save_history, args=[])
        thread3.setDaemon(False) # stop the thread after complete
        thread3.start()

        # commit
        result = type(self).handle().replace_one({'_id' : int(self.id)}, data, upsert=True)
        
        if result.acknowledged:
            # clear the caches in case there is a new auth value
            if isinstance(self, Auth):
                for cache in ('_cache', '_xcache', '_pcache', '_langcache'):
                    setattr(Auth, cache, {})
            
            return self
            
        raise Exception('Commit failed')

    def delete(self, user='admin'):
        if isinstance(self, Auth):
            if self.in_use(usage_type='bib') or self.in_use(usage_type='auth'):
                raise AuthInUse()
        
            for cache in ('_cache', '_xcache', '_pcache', '_langcache'):
                setattr(Auth, cache, {})

        # update browse index if necessary
        for field, values in self.logical_fields().items():
            for val in values:
                if type(self).handle().count_documents({field: val}) == 1:
                    # this record is the only instance of the value
                    DB.handle[f'{field}_index'].delete_one({'_id': val})
        
        history_collection = DB.handle[self.record_type + '_history']
        record_history = history_collection.find_one({'_id': self.id})

        if record_history is None:
            record_history = SON()
            record_history['history'] = [self.to_bson()]

        record_history['deleted'] = SON({'user': user, 'time': datetime.utcnow()})
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

        # there can only be one type but all logical fields are expected to be arrays
        self._logical_fields.setdefault('_record_type', ['default'])

        if self.record_type == 'bib':
            for type, match in Config.bib_type_map.items():
                if self.get_value(*match[:2]) == match[2]:
                    self._logical_fields['_record_type'] = [type]
                            
        return self._logical_fields

    #### utlities

    def merge(self, to_merge):
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

    def to_mrc(self, *tags, language=None):
        directory = ''
        data = ''
        next_start = 0
        field_terminator = u'\u001e'
        record_terminator = u'\u001d'

        for f in filter(lambda x: x.tag != '000', self.get_fields(*tags)):
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

        leader = self.get_value('000')

        if not leader:
            leader = '|' * 24
        elif len(leader) < 24:
            leader = self.leader.ljust(24, '|')

        new_leader = total_len \
            + leader[5:9] \
            + 'a' \
            + '22' \
            + base_address \
            + leader[17:20] \
            + '4500'        

        return new_leader + directory + data

    def to_mrk(self, *tags, language=None):
        string = ''

        for field in self.get_fields():
            string += field.to_mrk(language=language) + '\n'

        return string

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

    def to_xml_raw(self, *tags, language=None, xref_prefix=''):
        # todo: reimplement with `xml.dom` or `lxml` to enable pretty-printing
        root = ElementTree.Element('record')

        for field in self.get_fields(*tags):
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

                    if language and Config.linked_language_source_tag(self.record_type, field.tag, sub.code, language):
                        subnode.text = sub.translated(language)
                        continue   

                    subnode.text = val

                if xref:
                    subnode = ElementTree.SubElement(node, 'subfield')
                    subnode.set('code', '0')
                    subnode.text = xref_prefix + str(xref)

        return root

    def to_xml(self, *tags, language=None, xref_prefix=''):
        return ElementTree.tostring(self.to_xml_raw(language=language, xref_prefix=xref_prefix), encoding='utf-8').decode('utf-8')

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

    def from_mrc(self, string):
        pass

    @classmethod
    def from_mrk(cls, string, auth_control=True):
        record = cls()

        for line in filter(None, string.split('\n')):
            match = re.match(r'=(\d{3})  (.*)', line)
            tag, rest = match.group(1), match.group(2)

            if tag[:2] == '00':
                field = Controlfield(tag, rest)
            else:
                ind1, ind2 = [x.replace('\\', ' ') for x in rest[:2]]
                field = Datafield(record_type=cls.record_type, tag=tag, ind1=ind1, ind2=ind2)

                for chunk in filter(None, rest[2:].split('$')):
                    code, value = chunk[0], chunk[1:]
                    field.set(code, value, place='+', auth_control=auth_control)

            record.fields.append(field)

        return record
    
    @classmethod
    def from_xml_raw(cls, root, *, auth_control=False):
        assert isinstance(root, ElementTree.Element)
        self = cls()
            
        for c in filter(lambda x: re.search('controlfield$', x.tag), root):
            self.set(c.attrib['tag'], None, c.text)

        for d in filter(lambda x: re.search('datafield$', x.tag), root):
            field = Datafield(record_type=cls.record_type, tag=d.attrib['tag'], ind1=d.attrib['ind1'], ind2=d.attrib['ind2'])
            
            for s in filter(lambda x: re.search('subfield$', x.tag), d):
                field.set(s.attrib['code'], s.text, auth_control=auth_control, place='+')
                
            self.fields.append(field)
            
        return self
        
    @classmethod
    def from_xml(cls, string):
        return cls.from_xml_raw(ElementTree.fromstring(string))

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

    @classmethod
    def lookup(cls, xref, code, language=None):
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
        auth_tag = Config.authority_source_tag(record_type, tag, code)

        if auth_tag is None:
            return

        cached = Auth._xcache.get(value, {}).get(auth_tag, {}).get(code, None)

        if cached:
            return cached

        query = Query(Condition(auth_tag, {code: value}))
        auths = AuthSet.from_query(query.compile(), projection={'_id': 1})
        xrefs = [r.id for r in list(auths)]

        Auth._xcache.setdefault(value, {}).setdefault(auth_tag, {})[code] = xrefs

        return xrefs

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
        
        if self._heading_field:
            return self._heading_field
            
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
            
            total = 0
            
            for tag in tags:
                total += lookup_class.count_documents({f'{tag}.subfields.xref': xref})
                
            return total
        
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
            
class Diff():
    """Compare two Marc objects.

    Atrributes
    ----------
    a : list(dlx.marc.Field)
        The fields unique to record "a"
    b : list(dlx.marc.Field)
        The fields unique to record "b"
    c : list(dlx.marc.Field)
        The fields common to both records
    """

    def __init__(self, a, b):
        """Initilizes the object. Sets attribute "a" to a list of the fields 
        unique to record a. Sets attribute "b" to a list of the fields unique 
        to record b. Sets attribute "c" to a list of the fields common to both 
        records. 

        Positional arguments
        --------------------
        a : Marc
        b : Marc
        """
        assert isinstance(a, Marc)
        assert isinstance(b, Marc)

        self.a = list(filter(lambda x: x not in b.fields, a.fields))
        self.b = list(filter(lambda x: x not in a.fields, b.fields))
        self.c = list(filter(lambda x: x in b.fields, a.fields))

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
        subs = filter(lambda sub: sub.code in codes, self.subfields)

        return list(filter(None, [sub.value for sub in subs]))

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
            # walk to the tree to replace the subfield object
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
                value = sub.value

            string += ''.join([delim + sub.code + value])

        return string + term

    def to_mrk(self, language=None):
        inds = self.ind1 + self.ind2
        inds = inds.replace(' ', '\\')

        string = '={}  {}'.format(self.tag, inds)

        for sub in self.subfields:
            if language and Config.linked_language_source_tag(self.record_type, self.tag, sub.code, language):
                value = sub.translated(language)
            else: 
                value = sub.value

            string += ''.join(['${}{}'.format(sub.code, value)])

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
