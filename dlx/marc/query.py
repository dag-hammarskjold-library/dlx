from cgitb import text
import sys, json, re, copy
from datetime import datetime, timedelta
from warnings import warn
from nltk import PorterStemmer
from bson import SON, Regex
from bson.json_util import dumps
from dlx.db import DB
from dlx.config import Config
from dlx.util import Tokenizer

class InvalidQueryString(Exception):
    pass
    
class Query():
    @classmethod
    def from_string(cls, string, *, record_type='bib', modifier=None):
        # todo: indicators, all-subfield
        self = cls()
        self.record_type = record_type
        
        def tokenize(string):
            tokens = re.split(r'\s*(AND|OR|NOT)\s+', string)
            tokens = list(filter(None, tokens))

            return list(filter(None, tokens))
        
        def is_regex(string):
            pairs = [('/', '/'), ('\\', '\\'), ('`', '`')]

            for p in pairs:
                if string[0] == p[0] and (string[-1] == p[1] or (string[-2] == p[1] and string[-1] == 'i')):
                    return True
                
        def process_string(string):
            # convert to regex object if regex
            if is_regex(string):
                if string[-1] == 'i':
                    # case insensitive
                    return Regex(string[1:-2], 'i')
                else:
                    return Regex(string[1:-1])
            elif '*' in string:
                string = string.replace('(', r'\(')
                string = string.replace(')', r'\)')
                string = string.replace('[', r'\]')
                string = string.replace(']', r'\[')
                return Regex('^' + string.replace('*', '.*?') + '$')
            else:
                return string

        def add_quotes(string):
            # these xformations must be done in the correct order

            # extract quoted phrases
            quoted = re.findall('(".*?")', string)
            for _ in quoted: string = string.replace(_, '')

            # extract dashed words
            dashed = re.findall('\B(-\S+)', string)
            for _ in dashed: string = string.replace(_, '')

            rest = [f'"{x}"' for x in filter(None, re.split('\s+', string))]

            return ' '.join(rest + quoted + dashed)
                
        def parse(token, modifier=None):
            '''Returns: dlx.query.Condition'''
            
            # fully qualified syntax
            match = re.match(r'(\d{3})(.)(.)([a-z0-9]):(.*)', token)
            
            if match:
                tag, ind1, ind2, code, value = match.group(1, 2, 3, 4, 5)
                value = process_string(value)

                # regex
                if isinstance(value, Regex):
                    return Condition(tag, {code: value}, modifier=modifier)

                # exact match
                if value[0] == '\'' and value[-1] == '\'':
                    return Condition(tag, {code: value[1:-1]}, modifier=modifier)

                # text
                matches = DB.handle[f'_index_{tag}'].find({'$text': {'$search': add_quotes(value)}})
                matched_subfield_values = []

                for m in matches:
                    matched_subfield_values += [x['value'] for x in m['subfields']]

                stemmed_terms, filtered = Tokenizer.tokenize(value), []

                for val in matched_subfield_values:
                    stemmed_val_words = Tokenizer.tokenize(val)

                    if all(x in stemmed_val_words for x in stemmed_terms):
                        filtered.append(val)

                matched_subfield_values = filtered       

                if sys.getsizeof(matched_subfield_values) > 1e6: # 1 MB
                    raise Exception(f'Text search "{value}" has too many hits on field "{tag}". Try narrowing the search')

                if modifier == 'not':
                    q = {
                        '$or': [
                            {f'{tag}.subfields': {'$elemMatch': {'code': code, 'value': {'$not': {'$in': matched_subfield_values}}}}},
                            {f'{tag}.subfields': {'$not': {'$elemMatch': {'code': {'code': code}}}}}
                        ]
                    }
                else:
                    q = {f'{tag}.subfields': {'$elemMatch': {'code': code, 'value': {'$in': matched_subfield_values}}}}

                auth_ctrl = Config.bib_authority_controlled if self.record_type == 'bib' else Config.auth_authority_controlled

                if tag in auth_ctrl:
                    source_tag = list(auth_ctrl[tag].values())[0]

                    xrefs = map(
                        lambda x: x['_id'], 
                        DB.auths.find({f'{source_tag}.subfields.value': {'$in': matched_subfield_values}}, {'_id': 1})
                    )

                    q = {'$or': [q, {f'{tag}.subfields.xref': {'$in': list(xrefs)}}]}

                return Raw(q)

                return Condition(tag, {code: process_string(value)}, record_type=record_type, modifier=modifier)
                
            # tag only syntax 
            # matches a single subfield only
            match = re.match(r'(\d{3}):(.*)', token)
            
            if match:
                tag, value = match.group(1, 2)
                value = process_string(value)
                
                if tag == '001':
                    # interpret 001 as id
                    try:
                        return Raw({'_id': int(value)})
                    except ValueError:
                        raise InvalidQueryString(f'ID must be a number')
                elif tag[:2] == '00':
                    return Raw({tag: value}, record_type=record_type)

                # regex
                if isinstance(value, Regex):
                    return TagOnly(tag, value, modifier=modifier)

                # exact match
                if value[0] == '\'' and value[-1] == '\'':
                    return TagOnly(tag, value[1:-1], modifier=modifier)
                
                # text
                matches = DB.handle[f'_index_{tag}'].find({'$text': {'$search': add_quotes(value)}})
                matched_subfield_values = []

                for m in matches:
                    matched_subfield_values += [x['value'] for x in m['subfields']]

                stemmed_terms, filtered = Tokenizer.tokenize(value), []

                for val in matched_subfield_values:
                    stemmed_val_words = Tokenizer.tokenize(val)

                    if all(x in stemmed_val_words for x in stemmed_terms):
                        filtered.append(val)
                
                matched_subfield_values = filtered

                if sys.getsizeof(matched_subfield_values) > 1e6: # 1 MB
                    raise Exception(f'Text search "{value}" has too many hits on field "{tag}". Try narrowing the search')

                if modifier == 'not':
                    q = {f'{tag}.subfields.value': {'$not': {'$in': matched_subfield_values}}}
                else:
                    q = {f'{tag}.subfields.value': {'$in': matched_subfield_values}}

                auth_ctrl = Config.bib_authority_controlled if self.record_type == 'bib' else Config.auth_authority_controlled

                if tag in auth_ctrl:
                    source_tag = list(auth_ctrl[tag].values())[0]

                    xrefs = map(
                        lambda x: x['_id'], 
                        DB.auths.find({f'{source_tag}.subfields.value': {'$in': matched_subfield_values}}, {'_id': 1})
                    )

                    q = {'$or': [q, {f'{tag}.subfields.xref': {'$in': list(xrefs)}}]}

                return Raw(q)

            # id search
            match = re.match('id:(.*)', token)

            if match:
                if modifier:
                    raise Exception(f'modifier "{modifier}" not valid for ID search')

                value = match.group(1)
                
                try:
                    return Raw({'_id': int(value)}, record_type=record_type)
                except ValueError:
                    raise InvalidQueryString(f'ID must be a number')

            # audit dates
            match = re.match('(created|updated)([:<>])(.*)', token)

            if match:
                field, operator, value = match.group(1, 2, 3)
                date = datetime.strptime(value, '%Y-%m-%d')

                if operator == '<':
                    return Raw({field: {'$lte': date}})
                elif operator == '>':
                    return Raw({field: {'$gte': date}})
                else:
                    return Raw({'$and': [{field: {'$gte': date}}, {field: {'$lte': date + timedelta(days=1)}}]})

            # audit users
            match = re.match('(created_user|user):(.*)', token)

            if match:
                field, value = match.group(1, 2)

                return Raw({field: process_string(value)})

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
            match = re.match(f'(\\w+):(.*)', token)
            
            if match:
                logical_fields = list(Config.bib_logical_fields.keys()) + list(Config.auth_logical_fields.keys())
                field, value = match.group(1, 2)
                #todo: make aliases config
                field = 'symbol' if field == 's' else field
                field = 'subject' if field == 'heading' else field
                field = 'date' if field == 'meeting date' else field
                field = 'symbol' if field == 'meeting record' else field
                
                if field in logical_fields:
                    # exact match
                    if value[0] == '\'' and value[-1] == '\'':
                        return Raw({field: value[1:-1] }, record_type=record_type)

                    # regex
                    if isinstance(process_string(value), Regex):
                        if modifier == 'not':
                            return Raw({field: {'$not': process_string(value)}}, record_type=record_type)
                        else:
                            return Raw({field: process_string(value)}, record_type=record_type)
                    
                    # text
                    matches = DB.handle[f'_index_{field}'].find({'$text': {'$search': add_quotes(value)}})
                    values = [x['_id'] for x in matches]

                    if sys.getsizeof(values) > 1e6: # 1 MB
                        raise Exception(f'Text search "{value}" has too many hits on field "{field}". Try narrowing the search')

                    if modifier == 'not':
                        return Raw({field: {'$not': {'$in': values}}}, record_type=record_type)
                    else:
                        return Raw({field: {'$in': values}}, record_type=record_type)
                else:
                    raise InvalidQueryString(f'Unrecognized query field "{field}"')

            # free text
            #token = add_quotes(token)
            return Text(token, record_type=record_type)
        
        string = re.sub(r'^\s+', '', string) # leading
        string = re.sub(r'\s+$', '', string) # trailing
        tokens = tokenize(string)

        # parse tokens
        for i, token in enumerate(tokens):
            if token == 'NOT':
                tokens[i] = None
            elif i > 0 and tokens[i-1] == None:
                tokens[i] = parse(token, modifier='not')
                continue
            elif token not in ('AND', 'OR'): 
                tokens[i] = parse(token)

        tokens = list(filter(None, tokens))
        
        # take out the ors
        for i, token in enumerate(tokens):
            if token == 'OR':
                start, inc, ors = i, 0, []
                ors.append(copy.copy(tokens[start-1]))
                tokens[i-1] = None

                while len(tokens) > start+inc and tokens[start+inc] == 'OR':
                    ors.append(copy.copy(tokens[start+inc+1]))
                    tokens[start+inc], tokens[start+inc+1] = None, None
                    inc += 2

                condition = Or(*ors)
                self.conditions.append(condition)

        # add the rest as ands
        for i, token in enumerate(tokens):
            if token == 'AND':
                if tokens[i-1] and tokens[i-1] not in self.conditions:
                    self.conditions.append(tokens[i-1])
                
                if tokens[i+1]:
                    self.conditions.append(tokens[i+1])

        if not self.conditions:
            self.conditions = [tokens[0]]

        return self
        
    def __init__(self, *conditions):
        self.record_type = None
        self.conditions = conditions or []

    def add_condition(self, *conditions):
        self.conditions += conditions

    def compile(self):
        compiled = []

        for condition in self.conditions:
            compiled.append(condition.compile())

        # TODO serialize data
        #if sys.getsizeof(json.dumps(compiled)) > 4e6: # 4 MB
        #   raise Exception("Text search is too broad")

        if len(compiled) == 1:
            return compiled[0]
        else:
            return {'$and': compiled}

    def to_json(self):
        return dumps(self.compile())

    def to_string(self):
        for c in self.conditions:
            pass

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
        
    def compile(self):
        return {'$or': [c.compile() for c in self.conditions]}

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

        if kwargs.get('modifier'):
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

class Text():   
    def __init__(self, string='', *, record_type=None, modifier=None):
        self.string = string
        self.record_type = record_type
    
    def compile(self):
        #return {'$text': {'$search': f'{self.string}'}}
        
        # new text search
        quoted = re.findall(r'(".+?")', self.string)
        
        for _ in quoted:
            self.string = self.string.replace(_, '')

        hyphenated = re.findall(r'(\w+\-\w+)', self.string)

        for _ in hyphenated:
            self.string = self.string.replace(_, '')

        negated = [x[1] for x in re.findall(r'(^|\s)(\-\w+)', self.string)]

        for _ in negated:
            self.string = self.string.replace(_, '')

        words, data = Tokenizer.tokenize(self.string), {}
        
        if words:
            data.update({'words': {'$all': Tokenizer.tokenize(self.string)}})
        
        # regex on the text field is too slow
        #if len(quoted) > 1:
        #    data['$and'] = [{'text': Regex(f'{Tokenizer.scrub(x)}')} for x in quoted]
        #elif len(quoted) == 1:
        #    data['text'] = Regex(f'{Tokenizer.scrub(quoted[0])}')

        # use the text index for double-quoted strings and hyphentated words
        text_searches = []

        if quoted:    
            text_searches += quoted

        if hyphenated:
            text_searches += [f'"{x}"' for x in hyphenated]

        if negated:
            text_searches += negated
        
        if text_searches:
            data['$text'] = {'$search': ' '.join(text_searches)}

        return data

class Wildcard(Text):
    # Deprecated
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
class Raw():
    """Raw MongoDB query document condition"""
    
    def __init__(self, doc, *, record_type=None):
        self.condition = doc
        self.record_type = record_type
        
    def compile(self):
        return self.condition

class TagOnly():
    """Tag and value condition"""
    
    def __init__(self, tag, value, *, record_type=None, modifier=None):
        if record_type:
            self.record_type = record_type
        else:
            warn('Record type is not set for query condition. Defaulting to bib')
            self.record_type = 'bib'
            
        auth_ctrl = Config.bib_authority_controlled if self.record_type == 'bib' else Config.auth_authority_controlled
        
        if tag in auth_ctrl:
            source_tag = list(auth_ctrl[tag].values())[0]
            
            xrefs = map(
                lambda x: x['_id'], 
                DB.auths.find({f'{source_tag}.subfields.value': value}, {'_id': 1})
            )
            
            if modifier is None:
                self.condition = Or(
                    Raw({f'{tag}.subfields.value': value}),
                    Raw({f'{tag}.subfields.xref': {'$in': list(xrefs)}})
                )
            elif modifier == 'not':
                self.condition = Raw(
                    {
                        '$and': [
                            {f'{tag}.subfields.value': {'$not': value if isinstance(value, Regex) else {'$eq': value}}},
                            {f'{tag}.subfields.xref': {'$not': {'$in': list(xrefs)}}}
                        ]
                    }
                )
        else:
            if modifier is None:
                self.condition = Raw({f'{tag}.subfields.value': value})
            elif modifier == 'not':
                self.condition = Raw({f'{tag}.subfields.value': {'$not': value if isinstance(value, Regex) else {'$eq': value}}})
        
    def compile(self):
        return self.condition.compile()

class Any(TagOnly):
    # deprecated
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)    
    