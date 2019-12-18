'''
'''

from warnings import warn
import re, string, json
import jsonschema
from bson import SON

from dlx.config import Configs
from dlx.db import DB
#from dlx.query import jmarc as Q
from dlx.query import jfile as FQ
from dlx.marc.query import QueryDocument, Condition, Or

from xml.etree import ElementTree as XML
#from lxml.etree import tostring as write_xml

from pandas import read_excel

### Set classes

class MARCSet(object):
    # constructors
    
    @classmethod
    def from_query(cls,*args,**kwargs):
        if isinstance(args[0],QueryDocument):
            query = args[0].compile()
            args = [query]
        
        self = cls()
        MARC = self.record_class
        self.count = self.handle.count_documents(*args,**kwargs)
        self.records = map(lambda r: MARC(r), self.handle.find(*args,**kwargs))
        
        return self
    
    @classmethod    
    def from_dataframe(cls,df):
        pass
    
    def from_excel(cls,file):
        df = read_excel(file)
        return cls.from_dataframe(df)
    
    def __init__(self):
        self.records = None # can be any type iterable
        
    def cache(self):
        self.records = list(self.records)
        return self
        
    def remove(self,id):
        pass

    # serializations
    
    def to_mrc(self):
        mrc = ''
        
        for record in self.records:
            mrc += record.to_mrc()
            
        return mrc
    
    def to_xml(self):
        xml = ''
        
        for record in self.records:
            xml += str(record.to_xml())
            
        return xml
    
class BibSet(MARCSet):
    def __init__(self):
        self.handle = DB.bibs
        self.record_class = Bib
        super().__init__()
        
class AuthSet(MARCSet):
    def __init__(self):
        self.handle = DB.auths
        self.record_class = Auth
        super().__init__()


### Record classes
     
class MARC(object):
    '''
    '''

    class _Decorators(object):
        def check_connection(method):
            def wrapper(*args,**kwargs):
                DB.check_connection()
                return method(*args,**kwargs)
    
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
            
        return getattr(DB,col)
        
    @classmethod
    def match_id(cls,id):
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
    def match_ids(cls,*ids,**kwargs):
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
    def match(cls,*matchers,**kwargs):
        """
        Deprecated
        """
        
        warn('dlx.marc.MARC.match() is deprecated. Use dlx.marc.MARC.find() instead')
             
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
        for arg in ('limit', 'skip','sort'):
            if arg in kwargs:
                pymongo_kwargs[arg] = kwargs[arg]
            
        cursor = cls.handle().find(**pymongo_kwargs)    
                        
        for doc in cursor:
            yield cls(doc)
        
    @classmethod
    def find(cls,*args,**kwargs):
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
        
        cursor = cls.handle().find(*args,**kwargs)
        
        for doc in cursor:
            yield cls(doc)
    
    @classmethod
    def find_one(cls,*args,**kwargs):
        """Performs a `Pymongo` query.
        
        The same as `dlx.Marc.find()` except it returns only the first result as a `dlx.Bib` or `dlx.Auth`
        instance.
        """
        
        found = cls.handle().find_one(*args,**kwargs)
        
        if found is not None:
            return cls(found)
    
    @classmethod
    def count_documents(cls,*args,**kwargs):
        return cls.handle().count_documents(*args,**kwargs)
        
    #### database index creation
    
    @classmethod
    def controlfield_index(cls,tag):
        cls.handle().create_index(tag)
    
    @classmethod
    def literal_index(cls,tag):
        cls.handle().create_index(tag)
        cls.handle().create_index(tag + '.subfields.code')
        cls.handle().create_index(tag + '.subfields.value')
    
    @classmethod
    def linked_index(cls,tag):
        cls.handle().create_index(tag)
        cls.handle().create_index(tag + '.subfields.code')
        cls.handle().create_index(tag + '.subfields.xref')
    
    @classmethod
    def hybrid_index(cls,tag):
        cls.handle().create_index(tag)
        cls.handle().create_index(tag + '.subfields.code')
        cls.handle().create_index(tag + '.subfields.value')
        cls.handle().create_index(tag + '.subfields.xref')
    
    # Instance methods 
    
    def __init__(self,doc={}):
        self.controlfields = []
        self.datafields = []
        
        if doc is None: 
            doc = {}
        
        if '_id' in doc:
            self.id = int(doc['_id'])
        
        self.parse(doc)
                    
    def parse(self,doc):
        for tag in filter(lambda x: False if x == '_id' else True, doc.keys()):
            if tag == '000':
                self.leader = doc['000'][0]
                
            if tag[:2] == '00':
                for value in doc[tag]:
                    self.controlfields.append(Controlfield(tag,value))
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
                        
                    self.datafields.append(Datafield(tag,ind1,ind2,subfields))
    
    #### "get"-type methods
    
    def get_fields(self,*tags):
        if len(tags) == 0:
            return self.controlfields + self.datafields
            
        return filter(lambda x: True if x.tag in tags else False, self.controlfields + self.datafields)
        
        #todo: return sorted by tag
            
    def get_field(self,tag,**kwargs):
        fields = self.get_fields(tag)
        
        if 'place' in kwargs:
            for skip in range(0,kwargs['place']):
                next(fields, None)
            
        return next(fields, None)
            
    def get_dict(self,tag,*kwargs):
        if 'place' in kwargs:
            place = kwargs['place']
            return list(self.get_fields(tag))[place]
        else:
            return next(self.get_fields(tag), None)
        
    def get_values(self,tag,*codes,**kwargs):
        if 'place' in kwargs:
            fields = [self.get_field(tag,**kwargs)]
        else:
            fields = self.get_fields(tag)

        vals = []
                    
        for field in fields:
            if isinstance(field,Controlfield):
                return [field.value]
            else:
                if len(codes) == 0:
                    subs = field.subfields
                else:
                    subs = filter(lambda sub: sub.code in codes, field.subfields)
                
                for sub in subs:
                    vals.append(sub.value)
                    
        return vals
    
    def gets(self,tag,*codes,**kwargs):
        return self.get_values(tag,*codes,**kwargs)
    
    def get_value(self,tag,code=None,**kwargs):
        if 'address' in kwargs:
            address = kwargs['address']
            
            return self.get_values(tag,code,place=address[0])[address[1] or 0]
            
        field = self.get_field(tag)
        
        if field is None:
            return ''
           
        if isinstance(field,Controlfield):
            return field.value
        
        sub = next(filter(lambda sub: sub.code == code, field.subfields),None)
        
        return sub.value if sub else ''
    
    def get(self,tag,code=None,**kwargs):
         return self.get_value(tag,code,**kwargs)
         
    def get_tags(self):
        return sorted([x.tag for x in self.get_fields()])
        
    def get_xrefs(self,*tags):
        xrefs = []
        
        for field in filter(lambda f: isinstance(f, Datafield), self.get_fields(*tags)):
            xrefs = xrefs + field.get_xrefs()
        
        return xrefs
   
    def get_text(self,tag):
        pass
    
    #### "set"-type methods
    
    def set(self,tag,code,new_val,**kwargs):
        ### WIP
        # kwargs: address [pair], matcher [Pattern/list]
        
        if Configs.is_authority_controlled(tag,code):
            try:
                new_val + int(new_val) 
            except ValueError:
                raise Exception('Authority-controlled field {}${} must be set to an xref (integer)'.format(tag,code))
               
            auth_controlled = True
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
            for i in range(0,len(fields)):
                kwargs['address'] = [i,splace]
                self.set(tag,code,new_val,**kwargs)
            
            return self
        elif isinstance(fplace,int):
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
                self.parse({tag : [{'indicators' : [' ',' '], 'subfields' : [{'code' : code, valtype : new_val}]}]})
                
            return self
            
        try:   
            field = fields[fplace]
        except IndexError:
            raise Exception('There is no field at {}/{}'.format(tag,fplace))
            
        if tag[:2] == '00':
            if isinstance(matcher,re.Pattern):
                if matcher.search(field.value): field.value = new_val
            else:
                field.value = new_val
            
            return self
            
        subs = list(filter(lambda sub: sub.code == code, field.subfields))
        
        if len(subs) == 0 or splace == '+':
            if auth_controlled == True:
                field.subfields.append(Linked(code,new_val))
            else:
                field.subfields.append(Literal(code,new_val))
                
            return self
            
        elif isinstance(splace,int):
            subs = [subs[splace]]
        elif splace == '*':
            pass
        else:
            raise Exception('Invalid address')
            
        for sub in subs:
            if isinstance(sub,Literal):
                if isinstance(matcher,re.Pattern):
                    if matcher.search(sub.value): sub.value = new_val
                elif matcher == None:
                    sub.value = new_val
                else:
                    raise Exception('"matcher" must be a `re.Pattern` for a literal value')
                
            elif isinstance(sub,Linked):
                if isinstance(matcher,(tuple,list)):
                    if sub.xref in matcher: sub.xref = new_val
                elif matcher == None:
                    sub.xref = new_val
                else:
                    raise Exception('"matcher" must be a list or tuple of xrefs for a linked value')
            
        return self
    
    def set_values(self,*tuples):
        for t in tuples:
            tag,sub,val = t[0],t[1],t[2]
            kwargs = t[3] if len(t) > 3 else {}
            self.set(tag,sub,val,**kwargs)
         
        return self
    
    def set_indicators(self,tag,place,ind1,ind2):
        field = list(self.get_fields(tag))[place]
        
        if ind1 is not None:
            field.indicators[0] = ind1
        
        if ind2 is not None:        
            field.indicators[1] = ind2
            
        return self
            
    def change_tag(self,old_tag,new_tag):
        pass
        
    def delete_tag(self,tag,place=0):
        pass
        
    ### store
    
    def validate(self):
        try:
            jsonschema.validate(instance=self.to_dict(),schema=Configs.jmarc_schema)
        except jsonschema.exceptions.ValidationError as e:
            msg = '{} in {} : {}'.format(e.message, str(list(e.path)), json.dumps(doc,indent=4))
            raise jsonschema.exceptions.ValidationError(msg)
    
    def commit(self):
        # clear the cache so the new value is available
        if isinstance(self,Auth): Auth._cache = {}
        
        self.validate()
        
        # upsert (replace if exists, else new)
        return self.collection().replace_one({'_id' : int(self.id)}, self.to_bson(), True)
    
    #### utlities 
        
    def collection(self):
        if isinstance(self,Bib):
            return DB.bibs
        elif isinstance(self,Auth):
            return DB.auths
    
    def check(self,tag,val):
        pass
    
    def diff(self,marc):
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
        
    def to_json(self,to_indent=None):
        return json.dumps(self.to_dict(),indent=to_indent)
    
    def to_mij(self):
        mij = {}
        mij['leader'] = self.leader    
        mij['fields'] = [field.to_mij() for field in self.get_fields()]
        
        return json.dumps(mij)
        
    def to_mrc(self):
        directory = ''
        data = ''
        next_start = 0
        field_terminator = u'\u001e'
        record_terminator = u'\u001d'
        
        for f in filter(lambda x: x.tag != '000', self.get_fields()):
            text = f.to_mrc()
            data += text
            field_length = len(text.encode('utf-8'))
            directory += f.tag + str(field_length).zfill(4) + str(next_start).zfill(5)
            next_start += field_length
            
        directory += field_terminator
        data += record_terminator
        leader_dir_len = len(directory.encode('utf-8')) + 24 
        base_address = str(leader_dir_len).zfill(5)
        total_len = str(leader_dir_len + len(data.encode('utf-8'))).zfill(5)
        
        new_leader = total_len \
            + self.leader[5:9] \
            + 'a' \
            + self.leader[10:11] \
            + base_address \
            + self.leader[17:21] \
            + '4500'

        return new_leader + directory + data
    
    def to_mrk(self,*tags):
        string = ''
        
        for f in self.get_fields():    
            string += f.tag + '  '     
            string += MARC.field_text(f,'$','') + '\n'
        
        return string
    
    def to_str(self,*tags):
        # non-standard format intended to be human readable 
        string = ''
        
        for f in self.get_fields(*tags): 
            string += f.tag + '\n'     
            
            if isinstance(f,Controlfield):
                string += '   ' + f.value + '\n'
            else:
                #string += '\t' + '[' + f.ind1 + f.ind2 + ']\n'
                for s in f.subfields:
                    val = s.value if isinstance(s,Literal) else Auth.lookup(s.xref,s.code)
                    string += '   ' + s.code + ': ' + val + '\n'
                
            string += '-' * 25 + '\n';
        
        return string
            
    def to_xml(self,*tags):
        # todo: reimplement with `xml.dom` or `lxml` to enable pretty-printing
        
        root = XML.Element('record')
        
        for field in self.get_fields(*tags):
            if isinstance(field,Controlfield):
                node = XML.SubElement(root,'controlfield')
                node.set('tag',field.tag)
                node.text = field.value
            else:
                node = XML.SubElement(root,'datafield')
                node.set('tag',field.tag)
                node.set('ind1',field.ind1)
                node.set('ind2',field.ind2)
                
                for sub in field.subfields:
                    subnode = XML.SubElement(node,'subfield')
                    subnode.set('code',sub.code)
                    subnode.text = sub.value
                
        return XML.tostring(root,'utf-8')
        
    #### de-serializations
    # these formats don't fully support linked values.
    
    # todo: data coming from these formats should be somehow flagged as 
    # "externally sourced" and not committed to the DB without revision.
    #
    # alternatively, we can try to match string values from known DLX auth-
    # controlled fields with DLX authority strings and automatically assign
    # the xref (basically, the Horizon approach)
        
    def from_mij(self,string):
        pass
        
    def from_mrc(self,string):
        pass
        
    def from_mrk(self,string):
        pass
        
    def from_xml(self,string):
        pass

class Bib(MARC):
    def __init__(self,doc={}):
        super().__init__(doc)
        
    #### shorctuts
    
    def symbol(self):
        return self.get_value('191','a')
        
    def symbols(self):
        return self.get_values('191','a')
        
    def title(self):
        return ' '.join(self.get_values('245','a','b','c'))
    
    def date(self):
        return self.get_value('269','a')
        
    #### files 
        
    def files(self,*langs):
        symbol = self.symbol()
        cursor = DB.files.find(FQ.latest_by_id('symbol',symbol))
        
        ret_vals = []
        
        for doc in cursor:
            for lang in langs:
                if lang in doc['languages']:
                    ret_vals.append(doc['uri'])
            
        return ret_vals
    
    def file(self,lang):
        symbol = self.symbol()
        
        return DB.files.find_one(FQ.latest_by_id_lang('symbol',symbol,lang))['uri']
          
class Auth(MARC):
    _cache = {}
     
    @classmethod
    def lookup(cls,xref,code):
        DB.check_connection()
        
        if xref in cls._cache:
            if code in cls._cache[xref]:
                return cls._cache[xref][code]
        else:
            cls._cache[xref] = {}
        
        auth = Auth.find_one({'_id': xref},{'100': 1, '110': 1, '111': 1, '130': 1, '150': 1, '151': 1, '190': 1})
           
        if auth is None:
            value = 'N/A'
        else:    
            value = auth.header_value(code)
            
        cls._cache[xref][code] = value
            
        return value
            
    def __init__(self,doc={}):
        super().__init__(doc)
        
        self.header = next(filter(lambda field: field.tag[0:1] == '1', self.get_fields()), None)
                
    def header_value(self,code):
        if self.header is None:
            return
            
        for sub in filter(lambda sub: sub.code == code, self.header.subfields):
            return sub.value
    
### Field classes
        
class Field(object):
    def __init__(self):
        raise Exception('Cannot instantiate fom base class')
        
    def to_bson(self):
        raise Exception('This is a stub')

class Controlfield(Field):
    def __init__(self,tag,value):
        self.tag = tag
        self.value = value
    
    def to_bson(self):
        return self.value
        
    def to_mij(self):
        return {self.tag: self.value}
        
    def to_mrc(self):
        return self.value
    
class Datafield(Field):
    def __init__(self,tag,ind1,ind2,subfields):
        self.tag = tag
        self.ind1 = ind1
        self.ind2 = ind2
        self.subfields = subfields
        
    def get_xrefs(self):
        return [sub.xref for sub in filter(lambda x: hasattr(x,'xref'), self.subfields)]
            
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
            if isinstance(sub,Linked):
                subs.append({sub.code : Auth.lookup(sub.xref,sub.code)})
            else:
                subs.append({sub.code : sub.value}) 
                
        serialized[self.tag]['subfields'] = subs
                
        return serialized
        
    def to_mrc(self,delim=u'\u001f',term=u'\u001e'):
        text = self.ind1 + self.ind2
            
        for sub in (self.subfields):
            if hasattr(sub,'value'):
                text += delim + sub.code + sub.value
            else:
                text += delim + sub.code + Auth.lookup(sub.xref,sub.code)
        
        text += term
        
        return text
        
### Subfield classes
        
class Subfield(object):
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
    def __init__(self,code,value):
        self.code = code
        self.value = value
        
    def to_bson(self):
        return SON(data = {'code' : self.code, 'value' : self.value})
    
class Linked(Subfield):    
    def __init__(self,code,xref):
        self.code = code
        self.xref = int(xref)
        self._value = None
        
    @property
    def value(self):
        return Auth.lookup(self.xref,self.code)

    def to_bson(self):
        return SON(data = {'code' : self.code, 'xref' : self.xref})

### Matcher classes
# deprecated
        
class Matcher(Condition):
    # for backwards compatibility
    
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        
        warn('dlx.marc.Matcher is deprecated. Use dlx.marc.query.Condition instead')
                
class OrMatch(Or):
    # for backwards compatibility
    
    def __init__(self,*matchers):
        super().__init__(*matchers)
        self.matchers = matchers
        
        warn('dlx.marc.OrMatch is deprecated. Use dlx.marc.query.Or instead')
            
# end
