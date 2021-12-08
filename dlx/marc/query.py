import json, re
from warnings import warn
from bson import SON, Regex
from bson.json_util import dumps
from dlx.db import DB
from dlx.config import Config

class InvalidQueryString(Exception):
    pass
    
class Query():
    @classmethod
    def from_string(cls, string, *, record_type=None):
        # todo: indicators, OR, NOT, all-subfield
        self = cls()
        self.record_type = record_type
        
        def tokenize(string):
            tokens = re.split(' ?(AND|OR) ?', string)
            
            return tokens
        
        def is_regex(string):
            pairs = [('/', '/'), ('\\', '\\'), ('`', '`')]

            for p in pairs:
                if string[0] == p[0] and (string[-1] == p[1] or (string[-2] == p[1] and string[-1] == 'i')):
                    return True
                
        def process_string(string):
            if is_regex(string):
                if string[-1] == 'i':
                    # case insensitive
                    return Regex(string[1:-2], 'i')
                else:
                    return Regex(string[1:-1])
            elif '*' in string:
                return Regex('^' + string.replace('*', '.*') + '$')
            else:
                return string
                
        def parse(token):
            '''Returns: dlx.query.Condition'''
            
            # fully qualified syntax
            match = re.match('(\d{3})(.)(.)([a-z0-9]):(.*)', token)
            
            if match:
                tag, ind1, ind2, code, value = match.group(1, 2, 3, 4, 5)
                
                return Condition(tag, {code: process_string(value)})
                
            # tag only syntax 
            # matches a single subfield only
            match = re.match('(\d{3}):(.*)', token)
            
            if match:
                tag, value = match.group(1, 2)
                
                if tag == '001':
                    # interpret 001 as id
                    try:
                        return Raw({'_id': int(value)})
                    except ValueError:
                        raise InvalidQueryString(f'ID must be a number')
                elif tag[:2] == '00':
                    return Raw({tag: value})
                    
                # todo: handle auth controlled fields
                
                return Raw({f'{tag}.subfields.value': process_string(value)})
                
            # id search
            match = re.match('id:(.*)', token)

            if match:
                value = match.group(1)
                
                try:
                    return Raw({'_id': int(value)})
                except ValueError:
                    raise InvalidQueryString(f'ID must be a number')
                    
            # xref (records that reference a given auth#)
            match = re.match(f'xref:(.*)', token)

            if match:
                value = match.group(1)
                
                try:
                    xref = int(value)
                except ValueError:
                    raise InvalidQueryString(f'xref must be a number')

                if self.record_type == 'bib':
                    tags = list(Config.bib_authority_controlled.keys())
                elif self.record_type == 'auth':
                    tags = list(Config.auth_authority_controlled.keys())
                else:
                    raise Exception('"Query().record_type" must be set to "bib" or "auth" to do xref search')
                    
                conditions = []
                
                for tag in tags:
                    conditions.append(Raw({f'{tag}.subfields.xref': xref}))
                
                return Or(*conditions)
            
            # logical field
            match = re.match(f'(\w+):(.*)', token)
            
            if match:
                logical_fields = list(Config.bib_logical_fields.keys()) + list(Config.auth_logical_fields.keys())
                field, value = match.group(1, 2)
                
                field = 'symbol' if field == 's' else field #todo: make aliases config
                
                if field in logical_fields:
                    return Raw({field: process_string(value)})    
                else:
                    raise InvalidQueryString(f'Unrecognized query field "{field}"')
                    
            # free text
            return Wildcard(token)
        
        tokens = tokenize(string)
        
        if len(tokens) == 1:
            self.conditions.append(parse(tokens[0]))
        else:
            for i, token in enumerate(tokens):
                if token == 'AND':
                    self.conditions.append(parse(tokens[i-1]))
                    self.conditions.append(parse(tokens[i+1]))
                elif token == 'OR':
                    condition = Or(parse(tokens[i-1]), parse(tokens[i+1]))
                    self.conditions.append(condition)

        return self
        
    def __init__(self, *conditions):
        self.record_type = None
        self.conditions = conditions or []

    def add_condition(self, *conditions):
        self.conditions += conditions

    def compile(self):
        compiled = []

        for condition in self.conditions:
            if isinstance(condition, Or):
                ors = [c.compile() for c in condition.conditions]
                compiled.append({'$or': ors})
            else:
                compiled.append(condition.compile())

        if len(compiled) == 1:
            return compiled[0]
        else:
            return {'$and': compiled}

    def to_json(self):
        return dumps(self.compile())
        
class QueryDocument(Query):
    def __init__(self, *args, **kwargs):
        warn('dlx.marc.QueryDocument is deprecated. Use dlx.marc.Query instead')
        
        super().__init__(*args, **kwargs)
        
class BibQuery(Query):
    record_type = 'bib'
    
    def __init__(self, *args, **kwargs):
        self.record_type = 'bib'
        super().__init__(*args, **kwargs)
    
class AuthQuery(Query):
    record_type = 'auth'
    
    def __init__(self, *args, **kwargs):
        self.record_type = 'auth'
        super().__init__(*args, **kwargs)
    
class Or(object):
    def __init__(self, *conditions):
        self.conditions = conditions

class Condition(object):
    valid_modifiers = ['not', 'exists', 'not_exists']

    @property
    def subfields(self):
        return self._subfields

    @subfields.setter
    def subfields(self, subs):
        if isinstance(subs, dict):
            self._subfields = [(key, val) for key, val in subs.items()]
        else:
            self._subfields = [*subs]

    def __init__(self, tag=None, *subs, record_type=None, **kwargs):
        if record_type not in (None, 'bib', 'auth'):
            raise Exception('Invalid record type')
        
        self.record_type = record_type
        
        if tag:
            self.tag = tag
        elif 'tag' in kwargs:
            self.tag = kwargs['tag']

        if len(subs) > 0:
            if isinstance(subs[0], dict):
                self.subfields = subs[0]
            else:
                self.subfields = subs
        else:
            self._subfields = []

        if 'subfields' in kwargs:
            subs = kwargs['subfields']

            if isinstance(subs, dict):
                self._subfields = [(key, subs[key]) for key in subs.keys()]
            else:
                self._subfields = subs

        self.modifier = ''

        if 'modifier' in kwargs:
            mod = kwargs['modifier'].lower()

            if mod in Condition.valid_modifiers:
                self.modifier = mod
            else:
                raise Exception('Invalid modifier: "{}"'.format(mod))

    def compile(self):
        if not self.record_type:
            warn('Record type is not set for query condition. Defaulting to bib')
            self.record_type = 'bib'
            
        tag = self.tag
        subconditions = []

        for sub in self.subfields:
            code = sub[0]
            val = sub[1]

            if not Config.is_authority_controlled(self.record_type, tag, code):
                subconditions.append(
                    SON({'$elemMatch': {'code': code, 'value': val}})
                )
            else:      
                if isinstance(val, int):
                    xrefs = [val]
                else:
                    auth_tag = Config.authority_source_tag(self.record_type, tag, code)
                    lookup = SON({f'{auth_tag}.subfields': SON({'$elemMatch': {'code': code, 'value': val}})})
                    xrefs = [doc['_id'] for doc in DB.auths.find(lookup, {'_id': 1})]

                subconditions.append(
                    SON({'$elemMatch': {'code': code, 'xref': xrefs[0] if len(xrefs) == 1 else {'$in' : xrefs}}})
                )

        submatch = subconditions[0] if len(subconditions) == 1 else {'$all' : subconditions}

        if not self.modifier:
            return SON({tag: {'$elemMatch': {'subfields': submatch}}})
        else:
            if self.modifier == 'not':
                return SON({'$or': [{tag: {'$not': {'$elemMatch': {'subfields': submatch}}}}, {tag: {'$exists': False}}]})
            elif self.modifier == 'exists':
                return {tag: {'$exists': True}}
            elif self.modifier == 'not_exists':
                return {tag: {'$exists': False}}
            else:
                raise Exception('Invalid modifier')
                
class BibCondition(Condition):
    def __init__(self, *args, **kwargs):
        kwargs['record_type'] = 'bib'
        super().__init__(*args, **kwargs)

class AuthCondition(Condition):
    def __init__(self, *args, **kwargs):
        kwargs['record_type'] = 'auth'
        super().__init__(*args, **kwargs)

class Wildcard():
    """Wildcard text condition"""
    
    def __init__(self, string=''):
        self.string = string
    
    def compile(self):
        return {'$text': {'$search': f'{self.string}'}}
        
class Raw():
    """Raw MongoDB query document condition"""
    
    def __init__(self, doc):
        self.condition = doc
        
    def compile(self):
        return self.condition
         