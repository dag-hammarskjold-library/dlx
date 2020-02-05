'''
'''

import re, json
from warnings import warn
from xml.etree import ElementTree as XML

import jsonschema
from bson import SON

from dlx.config import Config
from dlx.db import DB
from dlx.query import jfile as FQ
from dlx.marc.query import QueryDocument, Condition, Or
from dlx.util import Table

### Set classes

class MarcSet():
    # constructors

    @classmethod
    def from_query(cls, *args, **kwargs):
        if isinstance(args[0], QueryDocument) or isinstance(args[0], Condition):
            query = args[0].compile()
            args = [query, *args[1:]]
        elif isinstance(args[0], (list, tuple)):
            for cond in arg[0]:
                cond.record_type = self.record_class.record_type

            query = QueryDocument(*conditions).compile()
            args = [query, *args[1:]]

        self = cls()
        self.query_params = [args, kwargs]
        Marc = self.record_class
        self.records = map(lambda r: Marc(r), self.handle.find(*args, **kwargs))

        return self

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
                    #raise Exception('Invalid column header "{}"'.format(field_name))
                    exceptions.append('Invalid column header "{}"'.format(field_name))
                    continue
                    
                if record.get_value(tag, code, address=[instance,0]):
                    #raise Exception('Column header {}.{}{} is repeated'.format(instance, tag, code))
                    exceptions.append('Column header {}.{}{} is repeated'.format(instance, tag, code))
                    continue
                    
                if field_check and field_check == tag + (code or ''):
                    if self.record_class.find_one(Condition(tag, {code: value}).compile()):
                        #raise Exception('{}${}: "{}" is already in the system'.format(tag, code, value))
                        exceptions.append('{}${}: "{}" is already in the system'.format(tag, code, value))
                        continue
                        
                if record.get_field(tag, place=instance):
                    try:
                        record.set(tag, code, value, address=[instance], auth_control=auth_control, auth_flag=auth_flag)
                    except Exception as e:
                        exceptions.append(str(e))
                else:
                    try:
                        record.set(tag, code, value, address=['+'], auth_control=auth_control, auth_flag=auth_flag)
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

        return cls.from_table(table, auth_control=auth_control, auth_flag=auth_flag, field_check=field_check)

    def __init__(self, *records):
        self.records = records or None # can be any type of iterable

    @property
    def count(self):
        if hasattr(self, '_count'):
            return self._count

        if hasattr(self, 'query_params') and isinstance(self.records, map):
            args, kwargs = self.query_params
            self._count = self.handle.count_documents(*args)
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

    def to_xml(self):
        # todo: stream instead of queue in memory
        root = XML.Element('collection')

        for record in self.records:
            root.append(record.to_xml_raw())

        return XML.tostring(root, 'utf-8').decode('utf-8')
            
    def to_excel(self, path):
        pass

class BibSet(MarcSet):
    def __init__(self):
        self.handle = DB.bibs
        self.record_class = Bib
        super().__init__()

class AuthSet(MarcSet):
    def __init__(self):
        self.handle = DB.auths
        self.record_class = Auth
        super().__init__()

### Record classes
     
class Marc(object):
    '''
    '''

    class _Decorators():
        def check_connection(method):
            def wrapper(*args, **kwargs):
                DB.check_connection()
                return method(*args, **kwargs)

            return wrapper

    # Class methods

    #### database query handlers

    @classmethod
    @_Decorators.check_connection
    def handle(cls):
        DB.check_connection()

        if cls.__name__ in ('Bib', 'JBIB'):
            col = 'bibs'
        elif cls.__name__ in ('Auth', 'JAUTH'):
            col = 'auths'
        else:
            raise Exception('Must call `handle()` from subclass `Bib` or `Auth`, or `JBIB` or `JAUTH` (deprecated)')

        return getattr(DB, col)

    @classmethod
    def match_id(cls, id):
        """Finds the record by ID.

        Parameters
        ----------
        id : int

        Returns
        -------
        dlx.Bib / dlx.Auth
            Depending on which subclass it was called on.

        Examples
        --------
        >>> bib = dlx.Bib.match_id(100000)
        >>> print(bib.symbol())
        """

        return cls.find_one(filter={'_id' : id})

    @classmethod
    def match_ids(cls, *ids, **kwargs):
        """Finds records by a list of IDs.

        Parameters
        ----------
        *ids : int

        Returns
        -------
        type.GeneratorType
            Yields instances of `dlx.Bib` or `dlx.Auth` depending on which subclass it was called on.

        Examples
        --------
        >>> bibs = dlx.Bib.match_ids(99999, 100000)
        >>> for bib in bibs:
        >>>     print(bib.symbol())
        """

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
        """Performs a `pymongo` query.

        This method calls `pymongo.collection.Collection.find()` directly on the 'bibs' or `auths` database
        collection.

        Parameters
        ----------
        filter : bson.SON
            A valid `pymongo` query filter against the raw JMARC data in the database.
        *kwargs    : ...
            Passes all remaining arguments to `pymongo.collection.Collection.find())

        Returns
        -------
        type.GeneratorType
            Yields instances of `dlx.Bib` or `dlx.Auth`.
        """

        cursor = cls.handle().find(*args, **kwargs)

        for doc in cursor:
            yield cls(doc)

    @classmethod
    def find_one(cls, *args, **kwargs):
        """Performs a `Pymongo` query.

        The same as `dlx.Marc.find()` except it returns only the first result as a `dlx.Bib` or `dlx.Auth`
        instance.
        """

        found = cls.handle().find_one(*args, **kwargs)

        if found is not None:
            return cls(found)

    @classmethod
    def count_documents(cls, *args, **kwargs):
        return cls.handle().count_documents(*args, **kwargs)

    #### database index creation

    @classmethod
    def controlfield_index(cls, tag):
        cls.handle().create_index(tag)

    @classmethod
    def literal_index(cls, tag):
        cls.handle().create_index(tag)
        cls.handle().create_index(tag + '.subfields.code')
        cls.handle().create_index(tag + '.subfields.value')

    @classmethod
    def linked_index(cls, tag):
        cls.handle().create_index(tag)
        cls.handle().create_index(tag + '.subfields.code')
        cls.handle().create_index(tag + '.subfields.xref')

    @classmethod
    def hybrid_index(cls, tag):
        cls.handle().create_index(tag)
        cls.handle().create_index(tag + '.subfields.code')
        cls.handle().create_index(tag + '.subfields.value')
        cls.handle().create_index(tag + '.subfields.xref')

    # Instance methods

    def __init__(self, doc={}):
        self.controlfields = []
        self.datafields = []

        if doc is None:
            doc = {}

        if '_id' in doc:
            self.id = int(doc['_id'])

        self.parse(doc)
        
    @property
    def fields(self):
        return self.controlfields + self.datafields

    def parse(self, doc):
        for tag in filter(lambda x: False if x == '_id' else True, doc.keys()):
            if tag == '000':
                self.leader = doc['000'][0]

            if tag[:2] == '00':
                for value in doc[tag]:
                    self.controlfields.append(Controlfield(tag, value, record_type=self.record_type))
            else:
                for field in doc[tag]:
                    ind1 = field['indicators'][0]
                    ind2 = field['indicators'][1]
                    subfields = []

                    for sub in field['subfields']:
                        if 'value' in sub:
                            subfields.append(Literal(sub['code'], sub['value']))
                        elif 'xref' in sub:
                            subfields.append(Linked(sub['code'], sub['xref']))

                    self.datafields.append(Datafield(tag, ind1, ind2, subfields, record_type=self.record_type))

    #### "get"-type methods

    def get_fields(self, *tags):
        if len(tags) == 0:
            return sorted(self.controlfields + self.datafields, key=lambda x: x.tag)

        return filter(lambda x: True if x.tag in tags else False, sorted(self.controlfields + self.datafields, key=lambda x: x.tag))

        #todo: return sorted by tag

    def get_field(self, tag, **kwargs):
        fields = self.get_fields(tag)

        if 'place' in kwargs:
            for skip in range(0, kwargs['place']):
                next(fields, None)

        return next(fields, None)

    def get_dict(self, tag, *kwargs):
        if 'place' in kwargs:
            place = kwargs['place']
            return list(self.get_fields(tag))[place]
        else:
            return next(self.get_fields(tag), None)

    def get_values(self, tag, *codes, **kwargs):
        if 'place' in kwargs:
            val = self.get_field(tag, **kwargs)
            fields = [val] if val else []
        else:
            fields = self.get_fields(tag)

        vals = []

        for field in fields:
            if isinstance(field, Controlfield):
                return [field.value]
            else:
                if len(codes) == 0:
                    subs = field.subfields
                else:
                    subs = filter(lambda sub: sub.code in codes, field.subfields)

                for sub in subs:
                    vals.append(sub.value)

        return vals

    def gets(self, tag, *codes, **kwargs):
        return self.get_values(tag, *codes, **kwargs)

    def get_value(self, tag, code=None, address=None, **kwargs):
        if address:
            if len(address) != 2:
                raise Exdception('Invalid address')
                
            try:
                return self.get_values(tag, code, place=address[0])[address[1] or 0]
            except IndexError:
                return ''
                
        field = self.get_field(tag)

        if field is None:
            return ''

        if isinstance(field, Controlfield):
            return field.value

        sub = next(filter(lambda sub: sub.code == code, field.subfields), None)
        
        if 'language' in kwargs:
            #val = sub.value # force the lookup ??
            return sub.translated(kwargs['language'])
        
        return sub.value if sub else ''

    def get(self, tag, code=None, **kwargs):
         return self.get_value(tag, code, **kwargs)

    def get_tags(self):
        return sorted([x.tag for x in self.get_fields()])

    def get_xrefs(self, *tags):
        xrefs = []

        for field in filter(lambda f: isinstance(f, Datafield), self.get_fields(*tags)):
            xrefs = xrefs + field.get_xrefs()

        return xrefs

    def get_text(self, tag):
        pass

    #### "set"-type methods

    def set(self, tag, code, new_val, auth_control=True, auth_flag=False, **kwargs):
        ### WIP
        # kwargs: address [pair], matcher [Pattern/list]
        
        if auth_control == True and Config.is_authority_controlled(self.record_type, tag, code):
            try:
                new_val + int(new_val)
            except ValueError:
                raise Exception('Authority-controlled field {}${} must be set to an xref (integer)'.format(tag, code))
                
            auth_controlled = True
        elif auth_flag == True and Config.is_authority_controlled(self.record_type, tag, code):
            auth_tag = Config.authority_source_tag(self.record_type, tag, code)
            query = QueryDocument(Condition(tag=auth_tag, subfields={code: new_val}))
            authset = AuthSet.from_query(query)
            
            if authset.count == 0:
                raise Exception('Authority-controlled field {}${} value "{}" is invalid'.format(tag, code, new_val))
            
            auth_controlled = False
        else:
            new_val = str(new_val)
            auth_controlled = False

        try:
            fplace = kwargs['address'][0]
        except KeyError:
            fplace = 0

        try:
            splace = kwargs['address'][1]
        except:
            splace = 0

        try:
            matcher = kwargs['matcher']
        except KeyError:
            matcher = None

        ###

        fields = list(self.get_fields(tag))

        if fplace == '*':
            for i in range(0, len(fields)):
                kwargs['address'] = [i, splace]
                self.set(tag, code, new_val, **kwargs)

            return self
        elif isinstance(fplace, int):
            pass
        elif fplace == '+':
            pass
        else:
            raise Exception('Invalid address')

        if len(fields) == 0 or fplace == '+':
            valtype = 'value' if auth_controlled == False else 'xref'

            if tag[:2] == '00':
                self.parse({tag : [new_val]})
            else:
                self.parse({tag : [{'indicators' : [' ', ' '], 'subfields' : [{'code' : code, valtype : new_val}]}]})

            return self

        try:
            field = fields[fplace]
        except IndexError:
            raise Exception('There is no field at {}/{}'.format(tag, fplace))

        if tag[:2] == '00':
            if isinstance(matcher, re.Pattern):
                if matcher.search(field.value): field.value = new_val
            else:
                field.value = new_val

            return self

        subs = list(filter(lambda sub: sub.code == code, field.subfields))

        if len(subs) == 0 or splace == '+':
            if auth_controlled == True:
                field.subfields.append(Linked(code, new_val))
            else:
                field.subfields.append(Literal(code, new_val))

            return self

        elif isinstance(splace, int):
            subs = [subs[splace]]
        elif splace == '*':
            pass
        else:
            raise Exception('Invalid address')

        for sub in subs:
            if isinstance(sub, Literal):
                if isinstance(matcher, re.Pattern):
                    if matcher.search(sub.value): sub.value = new_val
                elif matcher == None:
                    sub.value = new_val
                else:
                    raise Exception('"matcher" must be a `re.Pattern` for a literal value')

            elif isinstance(sub, Linked):
                if isinstance(matcher, (tuple, list)):
                    if sub.xref in matcher: sub.xref = new_val
                elif matcher == None:
                    sub.xref = new_val
                else:
                    raise Exception('"matcher" must be a list or tuple of xrefs for a linked value')

        return self

    def set_values(self, *tuples):
        for t in tuples:
            tag, sub, val = t[0], t[1], t[2]
            kwargs = t[3] if len(t) > 3 else {}
            self.set(tag, sub, val, **kwargs)

        return self

    def set_indicators(self, tag, place, ind1, ind2):
        field = list(self.get_fields(tag))[place]

        if ind1 is not None:
            field.indicators[0] = ind1

        if ind2 is not None:
            field.indicators[1] = ind2

        return self

    def change_tag(self, old_tag, new_tag):
        pass

    def delete_tag(self, tag, place=0):
        pass

    ### store

    def validate(self):
        try:
            jsonschema.validate(instance=self.to_dict(), schema=Config.jmarc_schema)
        except jsonschema.exceptions.ValidationError as e:
            msg = '{} in {} : {}'.format(e.message, str(list(e.path)), json.dumps(doc, indent=4))
            raise jsonschema.exceptions.ValidationError(msg)

    def commit(self):
        # clear the cache so the new value is available
        if isinstance(self, Auth): Auth._cache = {}

        self.validate()

        # upsert (replace if exists, else new)
        return self.collection().replace_one({'_id' : int(self.id)}, self.to_bson(), upsert=True)

    #### utlities
    
    def merge(self, to_merge):
        # sets any value from to_merge if the field doesn't exist in self
        # does not overwrite any values
        
        for field in to_merge.fields:
            if isinstance(field, Controlfield):
                if not self.get_value(field.tag):
                    self.set(field.tag, None, field.value)
            else:
                for sub in field.subfields:
                    if not self.get_value(field.tag, sub.code):
                        self.set(field.tag, sub.code, sub.value)
                
        return self
        
    def collection(self):
        if isinstance(self, Bib):
            return DB.bibs
        elif isinstance(self, Auth):
            return DB.auths

    def check(self, tag, val):
        pass

    def diff(self, marc):
        pass

    #### serializations

    def to_bson(self):
        bson = SON()
        bson['_id'] = int(self.id)

        for tag in self.get_tags():
            bson[tag] = [field.to_bson() for field in self.get_fields(tag)]

        return bson

    def to_dict(self):
        return self.to_bson().to_dict()

    def to_json(self, to_indent=None):
        return json.dumps(self.to_dict(), indent=to_indent)

    def to_mij(self):
        mij = {}
        mij['leader'] = self.leader
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

        if not hasattr(self, 'leader'):
            self.leader = '|' * 24
        elif len(self.leader) < 24:
            self.leader = self.leader.ljust(24, '|')

        new_leader = total_len \
            + self.leader[5:9] \
            + 'a' \
            + '22' \
            + base_address \
            + self.leader[17:20] \
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

    def to_xml_raw(self, *tags, language=None):
        # todo: reimplement with `xml.dom` or `lxml` to enable pretty-printing
        root = XML.Element('record')

        for field in self.get_fields(*tags):
            if isinstance(field, Controlfield):
                node = XML.SubElement(root, 'controlfield')
                node.set('tag', field.tag)
                node.text = field.value
            else:
                node = XML.SubElement(root, 'datafield')
                node.set('tag', field.tag)
                node.set('ind1', field.ind1)
                node.set('ind2', field.ind2)

                for sub in field.subfields:
                    subnode = XML.SubElement(node, 'subfield')
                    subnode.set('code', sub.code)
                    
                    if language and Config.linked_language_source_tag(self.record_type, field.tag, sub.code, language):
                        subnode.text = sub.translated(language)
                        continue   
                        
                    subnode.text = sub.value
                    
        return root

    def to_xml(self, *tags, language=None):
        return XML.tostring(self.to_xml_raw(language=language)).decode('utf-8')

    #### de-serializations
    # these formats don't fully support linked values.

    # todo: data coming from these formats should be somehow flagged as
    # "externally sourced" and not committed to the DB without revision.
    #
    # alternatively, we can try to match string values from known DLX auth-
    # controlled fields with DLX authority strings and automatically assign
    # the xref (basically, the Horizon approach)

    def from_mij(self, string):
        pass

    def from_mrc(self, string):
        pass

    def from_mrk(self, string):
        pass

    def from_xml(self, string):
        pass

class Bib(Marc):
    def __init__(self, doc={}):
        self.record_type = 'bib'
        super().__init__(doc)

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
        symbol = self.symbol()
        cursor = DB.files.find(FQ.latest_by_id('symbol', symbol))

        ret_vals = []

        for doc in cursor:
            for lang in langs:
                if lang in doc['languages']:
                    ret_vals.append(doc['uri'])

        return ret_vals

    def file(self, lang):
        symbol = self.symbol()

        return DB.files.find_one(FQ.latest_by_id_lang('symbol', symbol, lang))['uri']

class Auth(Marc):
    _cache = {}

    @classmethod
    def lookup(cls, xref, code, language=None):
        DB.check_connection()
        
        if xref in cls._cache:
            if code in cls._cache[xref]:
                if language:
                    if language in cls._cache[xref][code]:
                        return cls._cache[xref][code][language]
        else:
            cls._cache[xref] = {}
            
        projection = dict.fromkeys(Config.auth_heading_tags(), True)
        
        if language:
            for x in Config.get_language_tags():
                projection[x] = True
        
        auth = Auth.find_one({'_id': xref}, projection)
        value = auth.heading_value(code, language) if auth else '**Linked Auth Not Found**'
        
        if language:
            cls._cache[xref][code] = {}
            cls._cache[xref][code][language] = value
        else:
            cls._cache[xref][code] = value

        return value

    def __init__(self, doc={}):
        self.record_type = 'auth'
        super().__init__(doc)

        self.heading_field = next(filter(lambda field: field.tag[0:1] == '1', self.get_fields()), None)

    def heading_value(self, code, language=None):
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

### Field classes

class Field():
    def __init__(self):
        raise Exception('Cannot instantiate fom base class')

    def to_bson(self):
        raise Exception('This is a stub')

class Controlfield(Field):
    def __init__(self, tag, value, record_type=None):
        self.record_type = record_type
        self.tag = tag
        self.value = value

    def to_bson(self):
        return self.value

    def to_mij(self):
        return {self.tag: self.value}

    def to_mrc(self, term=u'\u001e', language=None):
        return self.value + term

    def to_mrk(self, language=None):
        return '{}  {}'.format(self.tag, self.value)

class Datafield(Field):
    def __init__(self, tag, ind1, ind2, subfields, record_type=None):
        self.record_type = record_type
        self.tag = tag
        self.ind1 = ind1
        self.ind2 = ind2
        self.subfields = subfields
    
    def get_value(self, code):
        sub = next(filter(lambda sub: sub.code == code, self.subfields), None)
        
        return sub.value if sub else ''
        
    def get_values(self, *codes):
        subs = filter(lambda sub: sub.code in codes, self.subfields)
        
        return [sub.value for sub in subs]
        
    def get_xrefs(self):
        return [sub.xref for sub in filter(lambda x: hasattr(x, 'xref'), self.subfields)]

    def to_bson(self):
        return SON (
            data = {
                'indicators' : [self.ind1, self.ind2],
                'subfields' : [sub.to_bson() for sub in self.subfields]
            }
        )

    def to_mij(self):
        serialized = {self.tag: {}}

        serialized[self.tag]['ind1'] = self.ind1
        serialized[self.tag]['ind2'] = self.ind2

        subs = []

        for sub in self.subfields:
            if isinstance(sub, Linked):
                subs.append({sub.code : Auth.lookup(sub.xref, sub.code)})
            else:
                subs.append({sub.code : sub.value})

        serialized[self.tag]['subfields'] = subs

        return serialized

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

        string = '{}  {}'.format(self.tag, inds)
        
        for sub in self.subfields:
            if language and Config.linked_language_source_tag(self.record_type, self.tag, sub.code, language):
                value = sub.translated(language)
            else: 
                value = sub.value
            
            string += ''.join(['${}{}'.format(sub.code, sub.value)])

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
        return SON(data = {'code' : self.code, 'value' : self.value})

class Linked(Subfield):
    def __init__(self, code, xref):
        self.code = code
        self.xref = int(xref)
        self._value = None

    @property
    def value(self):
        return Auth.lookup(self.xref, self.code)
        
    def translated(self, language):
        return Auth.lookup(self.xref, self.code, language)

    def to_bson(self):
        return SON(data = {'code' : self.code, 'xref' : self.xref})

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
