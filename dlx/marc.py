'''
'''

import re, string, json
import jsonschema
from bson import SON

from dlx.config import Configs
from dlx.db import DB
from dlx.query import jmarc as Q
from dlx.query import jfile as FQ

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
        
        return cls.find_one({'_id' : id})
        
    @classmethod
    def match_ids(cls,*ids):
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
        
        return cls.find({'_id' : {'$in' : [*ids]}})
    
    @classmethod
    def match_value(cls,tag,code,val):
        """Performs a query for a single MARC value in the database and returns a generator object which yields the results.
        
        Parameters
        ----------
        tag : str
            The field tag to match.
        code : str / None
            The subfield code to match. Use `None` as the code if matching a controlfield value.
        val : str / Pattern
            Exact string value or compiled pattern to match. 
    
        Returns
        -------
        type.GeneratorType
            Yields instances of `dlx.Bib` or `dlx.Auth` depending on which subclass it was called on.   
    
        Examples
        -------
        >>> auths = dlx.Auth.match_value('100', 'a', re.compile('Hammarskjöld'))
        >>>    auth = next(auths)
        >>>    print(auth.get_value('100','a'))
        """
        
        cursor = cls.handle().find(Q.match_value(tag,code,val))
        
        for doc in cursor:
            yield cls(doc)
        
        return cls(cls.handle().find_one(Q.match_value(tag,code,val)))
    
    @classmethod    
    def match_values(cls,*tuples):
        """Performs a query for a multiple MARC values in the database and returns a generator object which yields the results.
        
        The query will be a boolean `and` search.
    
        Parameters
        ----------
        *tuples : tag [str], code [str], val [str / Pattern] 
            Accepts arbitrary number of tuples composed of the code and value to 
            match against. Value can be a str or Pattern.
    
            Use `None` as the subfield code if the field is a controlfield.
    
        Returns
        -------
        type.GeneratorType
            Yields instances of `dlx.Bib` or `dlx.Auth` depending on which subclass it was called on.   
    
        Examples
        -------
        >>> bibs = dlx.Bib.match_values(,
        >>>     ('269', 'a', re.compile('^1999')),
        >>>     ('650', 'a', 'HUMAN RIGHTS')
        >>> ) 
        >>> for bib in bib:
        >>>     print(bib.symbol())
        """
        cursor = cls.handle().find(Q.and_values(*tuples))
        
        for doc in cursor:
            yield cls(doc)
            
    @classmethod    
    def match_values_or(cls,*tuples):
        """Performs a query for a multiple MARC values in the database and returns a generator object which yields the results.
        
        The same as `dlx.Marc.match_values()` except that the query will be a boolean `or` search.
        """        
        
        cursor = cls.handle().find(Q.or_values(*tuples))
        
        for doc in cursor:
            yield cls(doc)
    
    @classmethod
    def match_field(cls,tag,*tuples):
        """Performs a query for a multiple subfield values in the database within the same MARC field.
               
        Parameters
        ----------
        tag : str
            The field tag to match.
        *tuples : (code [str], val [str / Pattern])
            Accepts arbitrary number of tuples composed of the code and value to 
            match against. Value can be a str or Pattern.
        
            Use `None` as the subfield code if the field is a controlfield.
        
        Returns
        -------
        type.GeneratorType
            Yields instances of `dlx.Bib` or `dlx.Auth` depending on which subclass it was called on.
        
        Examples
        -------
        >>> bibs = dlx.Bib.match_field(
        >>>     '191', 
        >>>     ('b', 'A/'), 
        >>>     ('c', '73')
        >>> ) 
        >>> for bib in bibs: 
        >>>     print(bib.symbol())
        """
    
        cursor = cls.handle().find(Q.match_field(tag,*tuples))
        
        for doc in cursor:
            yield cls(doc)
    
    @classmethod
    def match_fields(cls,*tuples_of_tuples):
        """Performs a query for a series of fields containing multiple subfield values within.
        
        The query will be a boolean `and` search.
        
        Parameters
        ----------
        *tuples_of_tuples : (tag [str], *more_tuples : (code [str], val [str / Pattern]))
            Accepts an arbitrary number of "tuples of tuples" where the first element is the tag,
            and the rest of the elements are tuples composed of a code and value to match against.
            
            `val` can be composed of a string value or compiled pattern to match against.
        
        Returns
        -------
        type.GeneratorType
            Yields instances of `dlx.Bib` or `dlx.Auth`.
        
        Examples
        ---------
        >>> bibs = dlx.Bib.match_fields(
        >>>     (
        >>>         '191', 
        >>>         ('a', re.compile('^A/RES/'),
        >>>         ('c', '73')
        >>>     ),
        >>>     (
        >>>         '650', 
        >>>         ('a', 'HUMAN RIGHTS')
        >>>     )
        >>> )
        """
        
        cursor = cls.handle().find(Q.and_fields(*tuples_of_tuples))

        for doc in cursor:
            yield cls(doc)
            
    @classmethod    
    def match_fields_or(cls,*tuples_of_tuples):
        """Performs a query for a series of fields containing multiple subfield values within.
        
        The same as `dlx.Marc.match_fields() except that the query will be a boolean `or` search.
        """
        
        cursor = cls.handle().find(Q.or_fields(*tuples_of_tuples))

        for doc in cursor:
            yield cls(doc)
            
    @classmethod    
    def match_xrefs(cls,tag,code,*xrefs):
        """Performs a query for all the records that contain an Xref in a list of Xrefs.
        
        Parameters
        ---------
        tag : str
        code : str
        *xrefs : int
            Variable-length list of Xrefs to mathc against
        
        Returns
        -------
        type.GeneratorType
            Yields instances of `dlx.Bib` or `dlx.Auth`.
        """
        
        cursor = cls.handle().find(Q.match_xrefs(tag,code,*xrefs))
        
        for doc in cursor:
            yield cls(doc)
            
    @classmethod
    def match_multi(cls,takes,excludes):
        """This is provisional and very slow because it runs all the conditions as separate searches"""
        
        sets = []
        
        for ex in excludes:
            sets.append(set([bib.id for bib in list(ex)]))
        
        exclude_ids = list(sets[0].union(*sets))
        
        sets = []
        
        for take in takes:
            sets.append(set([bib.id for bib in list(take)]))
            
        take_ids = filter(lambda x: x not in exclude_ids,list(sets[0].intersection(*sets)))
        
        return Bib.match_ids(*take_ids)
    
    @classmethod    
    def match(cls,*matchers,**kwargs):
        """
        Only supports `not` and `not_exists` keywords so far. WIP
        """
        
        query = cls.compile_matchers(*matchers)
        args = [query]
        
        if 'project' in kwargs.keys():
            projection = {}
            
            for tag in kwargs['project']:
                projection[tag] = 1
                
            args.append(projection)
        
        pymongo_kwargs = {}
        
        # sort only works on _id field
        for arg in ('limit', 'skip','sort'):
            if arg in kwargs.keys():
                pymongo_kwargs[arg] = kwargs[arg]
            
        if pymongo_kwargs.keys():
            cursor = cls.handle().find(*args,**pymongo_kwargs)    
        else:
            cursor = cls.handle().find(*args)
                
        for doc in cursor:
            yield cls(doc)
    
    @classmethod
    def compile_matchers(cls,*matchers):
        match_docs = []
        
        for matcher in matchers:
            if isinstance(matcher,OrMatch):
                or_match_docs = []
           
                for m in matcher.matchers:
                    or_match_docs.append(m.compile())
                             
                match_docs.append(
                    SON(
                        data = 
                            {'$or': or_match_docs}
                        )
                    )
            else:               
                match_docs.append(matcher.compile())
                
        query = SON(
            data = {'$and': match_docs}
        )
        
        return query
        
    @classmethod
    def find(cls,filter,*pymongo_params):
        """Performs a `pymongo` query.

        This method calls `pymongo.collection.Collection.find()` directly on the 'bibs' or `auths` database 
        collection.
        
        Parameters
        ----------
        filter : bson.SON
            A valid `pymongo` query filter against the raw JMARC data in the database.            
        *pymongo_params    : ...
            Passes all remaining arguments to `pymongo.collection.Collection.find())
        
        Returns
        -------
        type.GeneratorType
            Yields instances of `dlx.Bib` or `dlx.Auth`.
        """
        
        cursor = cls.handle().find(filter,*pymongo_params)
        
        for doc in cursor:
            yield cls(doc)
    
    @classmethod
    def find_one(cls,filter):
        """Performs a `Pymongo` query.
        
        The same as `dlx.Marc.find()` except it returns only the first result as a `dlx.Bib` or `dlx.Auth`
        instance.
        """
        
        found = cls.handle().find_one(filter)
        
        if found is not None:
            return cls(found)
        
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
        
        if '_id' in doc.keys():
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
                        if 'value' in sub.keys():
                            subfields.append(Literal(sub['code'], sub['value']))    
                        elif 'xref' in sub.keys():
                            subfields.append(Linked(sub['code'], sub['xref']))
                        
                    self.datafields.append(Datafield(tag,ind1,ind2,subfields))
    
    #### "get"-type methods
    
    def get_fields(self,*tags):
        if len(tags) == 0:
            return self.controlfields + self.datafields
            
        return filter(lambda x: True if x.tag in tags else False, self.controlfields + self.datafields)
        
        #todo: return sorted by tag
            
    def get_field(self,tag,**kwargs):
        if 'place' in kwargs.keys():
            place = kwargs['place']
            return list(self.get_fields(tag))[place]
        else:
            return next(self.get_fields(tag), None)
            
    def get_dict(self,tag,*kwargs):
        if 'place' in kwargs.keys():
            place = kwargs['place']
            return list(self.get_fields(tag))[place]
        else:
            return next(self.get_fields(tag), None)
        
    def get_values(self,tag,*codes,**kwargs):
        if 'place' in kwargs.keys():
            fields = [self.get_field(tag,**kwargs)]
        else:
            fields = self.get_fields(tag)
        
        if len(codes) == 0:
            codes = list(string.ascii_lowercase + string.digits)
            
        vals = []
                    
        for field in fields:
            if isinstance(field,Controlfield):
                return [field.value]
                
            for sub in filter(lambda sub: sub.code in codes, field.subfields):
                if isinstance(sub,Literal):
                    vals.append(sub.value)
                elif isinstance(sub,Linked):
                    val = Auth.lookup(sub.xref,sub.code)
                    vals.append(val)
                    
        return vals
    
    def gets(self,tag,*codes,**kwargs):
        return self.get_values(tag,*codes,**kwargs)
    
    def get_value(self,tag,code=None,**kwargs):
        if 'address' in kwargs.keys():
            address = kwargs['address']
            
            return self.get_values(tag,code,place=address[0])[address[1] or 0]
            
        field = self.get_field(tag)
        
        if field is None:
            return ''
           
        if isinstance(field,Controlfield):
            return field.value
              
        for sub in filter(lambda sub: sub.code == code, field.subfields):
            return sub.value
        
        return ''
    
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
        if isinstance(self,Auth): MARC._cache = {}
        
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
            
    def to_xml(self):
        pass
        
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
    def __init__(self,doc={}):
        super().__init__(doc)
        
        self.header = next(filter(lambda field: field.tag[0:1] == '1', self.get_fields()), None)
    
    @classmethod
    def lookup(cls,xref,code):
        DB.check_connection()
        
        try:
            return MARC._cache[xref][code]
        except:
            auth = Auth.match_id(xref)
            
            if auth is None:
                value = 'N/A'
            else:    
                value = auth.header_value(code)
                
            if xref not in MARC._cache.keys():
                MARC._cache[xref] = {}
                
            MARC._cache[xref][code] = value
                
            return value
                
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
            
class Matcher(object):
    valid_modifiers = ['or','not','exists','not_exists']
    
    @property
    def subfields(self):
        return self._subfields
    
    @subfields.setter
    def subfields(self,subs):
        self._subfields = [*subs]
    
    def __init__(self,tag=None,*subs,**kwargs):    
        if tag:
            self.tag = tag
        if subs is not None:
            self._subfields = [*subs]
            
        self.modifier = ''
        
        if 'modifier' in kwargs.keys():
            mod = kwargs['modifier'].lower()
            
            if mod in Matcher.valid_modifiers:
                self.modifier = mod
            else:
                raise Exception
                
    def compile(self):
        subs = self.subfields
        mod = self.modifier.lower()
        
        return Q.match_field(self.tag,*subs,modifier=mod)
                
class OrMatch(object):
    def __init__(self,*matchers):
        self.matchers = matchers

        
# end
