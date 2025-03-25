import sys, json, re, copy
from datetime import datetime, timedelta
from uuid import uuid1
from warnings import warn
from nltk import PorterStemmer
from bson import SON, Regex
from bson.json_util import dumps
from dlx.db import DB
from dlx.config import Config
from dlx.util import Tokenizer

class InvalidQueryString(Exception):
    pass

class WildcardRegex(Regex):
    # for differentiating regex to run against text index vs actual value
    def __init__(self, string=None):
        super().__init__(string, 'i')
    
class Query():
    @classmethod
    def from_string(cls, string, *, record_type='bib', modifier=None):
        # todo: indicators, all-subfield
        self = cls()
        self.record_type = record_type

        def tokenize(string):
            tokens = []
            buffer = ''
            in_single_quotes = False
            in_double_quotes = False
            in_regex = False

            for i, char in enumerate(string):
                buffer += char

                if char == "'" and string[i-1] == ':':
                    in_single_quotes = True
                elif char == "'":
                    in_single_quotes = False
                elif char == '/' and string[i-1] == ':':
                    in_regex = True
                elif char == '/' and string[i-1] != '\\':
                    in_regex = False
                elif char == '"':
                    if in_single_quotes == False and in_regex == False:
                        in_double_quotes = not in_double_quotes

                if in_single_quotes == False and in_double_quotes == False and in_regex == False:
                    # check if operator is detected; split the terms and the operators into tokens
                    match = re.match(r'^(.*)(^|\s)(AND|OR|NOT)\s$', buffer)
                    
                    if match:
                        term, operator = match.group(1, 3)
                        term = term.strip()

                        if not tokens or tokens[-1] != term:
                            tokens.append(term)
                        
                        tokens.append(operator)
                        buffer = ''
                        
            # capture last (or only) token
            tokens.append(buffer.strip())
            tokens = list(filter(None, tokens))

            if in_single_quotes or in_double_quotes:
                raise InvalidQueryString("Unresolved quotes")
            elif in_regex:
                raise InvalidQueryString("Unclosed regex")

            return tokens
        
        def is_regex(string):
            pairs = [('/', '/'), ('\\', '\\'), ('`', '`')]

            for p in pairs:
                if string[0] == p[0] and (string[-1] == p[1] or (string[-2] == p[1] and string[-1] == 'i')):
                    return True
                elif string[0] == p[0]:
                    raise InvalidQueryString(f'Invalid regex: "{token}"')
                
        def process_string(string):
            # convert to regex object if regex
            if is_regex(string):
                if string[-1] == 'i':
                    return Regex(string[1:-2], 'i')
                else:
                    return Regex(string[1:-1])
            elif '*' in string:
                # create regex pattern using wildcard character
                if string == '*':
                    # special string for checking if field exists
                    return string

                placeholder = str(uuid1()).replace('-', '')
                string = string.replace('*', placeholder)
                string = re.escape(string)
                string = string.replace(placeholder, '.*')
                string = string if string[0:2] == '.*' else '^' + string
                string = string if string[-2:] == '.*' else string + '$'

                return WildcardRegex(string)
            else:
                # do nothing
                return string
                
        def parse(token, modifier=None):
            '''Returns: dlx.query.Condition'''
            
            # fully qualified syntax
            if match := re.match(r'(\d{3})(.)(.)([a-z0-9]):(.*)', token):
                tag, ind1, ind2, code, value = match.group(1, 2, 3, 4, 5)
                value = process_string(value)

                # exists
                if value == '*':
                    if modifier == 'not':
                        return Raw({f'{tag}.subfields': {'$not': {'$elemMatch': {'code': code}}}})
                    else:
                        return Raw({f'{tag}.subfields': {'$elemMatch': {'code': code}}})

                # exact match
                if not isinstance(value, Regex):
                    if value[0] == "\'" and value[-1] == "\'":
                        return Condition(tag, {code: value[1:-1]}, modifier=modifier, record_type=self.record_type)
                    elif value[0] == "\'" and value[-1] != "\'":
                        raise InvalidQueryString(f'Invalid exact match using single quote: "{token}"')

                # regex
                if isinstance(value, Regex):
                    if DB.handle[f'_index_{tag}'].estimated_document_count() > 100_000 and len(value.pattern) < 8:
                        # this is likely to be faster querying directly against the record collection
                        return Condition(tag, {code: value}, modifier=modifier, record_type=self.record_type)

                    if isinstance(value, WildcardRegex):
                        matches = DB.handle[f'_index_{tag}'].find({'subfields': {'$elemMatch': {'code': code, 'value': value}}})
                    else:
                        matches = DB.handle[f'_index_{tag}'].find({'subfields.value': value})
                        #return Condition(tag, {code: value}, modifier=modifier, record_type=self.record_type)

                    matched_subfield_values = []

                    for m in matches:
                        matched_subfield_values += list(
                            filter(
                                lambda y: re.search(value.pattern, y, flags=value.flags), 
                                [x['value'] for x in filter(lambda z: z['code'] == code, m['subfields'])]
                            )
                        )
                else:
                    # text
                    quoted = re.findall(r'"(.+?)"', value)
                    quoted = [Tokenizer.scrub(x) for x in quoted]
                    negated = [x[1] for x in re.findall(r'(^|\s)(\-\w+)', value)]
                    negated = [Tokenizer.scrub(x) for x in negated]

                    for _ in negated:
                        value = value.replace(_, '')

                        if not value.strip():
                            raise Exception('Search term can\'t contain only negations')

                    q = {
                        '$and': [
                            {'words': {'$all': Tokenizer.tokenize(value)}}
                        ]
                    }

                    if negated:
                        q['$and'].append({'words': {'$nin': Tokenizer.tokenize(' '.join(negated))}})

                    for phrase in quoted:
                        q['$and'].append({'text': Regex(fr'\b{phrase}\b')})
                    
                    matches = DB.handle[f'_index_{tag}'].find(q)
                    matched_subfield_values = []

                    for m in matches:
                        matched_subfield_values += [x['value'] for x in filter(lambda z: z['code'] == code, m['subfields'])]

                    matched_subfield_values = list(filter(None, matched_subfield_values))

                    stemmed_terms, filtered = Tokenizer.tokenize(value), []

                    for val in matched_subfield_values:
                        stemmed_val_words = Tokenizer.tokenize(val)

                        if all(x in stemmed_val_words for x in stemmed_terms):
                            filtered.append(val)

                    matched_subfield_values = list(set(filtered))      

                if sys.getsizeof(matched_subfield_values) > 1e6: # 1 MB
                    if isinstance(value, Regex):
                        # fall back to normal regex
                        return Condition(tag, {code: value}, modifier=modifier)
                    
                    raise InvalidQueryString(f'Text search "{value}" has too many hits on field "{tag}". Try narrowing the search')
                elif len(matched_subfield_values) == 0:
                    return Raw({'_id': 0}) # query that matches no documents
                
                if modifier == 'not':
                    q = {f'{tag}': {'$not': {'$elemMatch': {'subfields': {'$elemMatch': {'code': code, 'value': {'$in': matched_subfield_values}}}}}}}
                else:
                    q = {f'{tag}.subfields': {'$elemMatch': {'code': code, 'value': {'$in': matched_subfield_values}}}}

                auth_ctrl = Config.bib_authority_controlled if self.record_type == 'bib' else Config.auth_authority_controlled

                if tag in auth_ctrl:
                    if code in auth_ctrl[tag].keys():
                        source_tag = list(auth_ctrl[tag].values())[0]

                        xrefs = map(
                            lambda x: x['_id'], 
                            DB.auths.find(
                                {f'{source_tag}.subfields.value': {'$in': matched_subfield_values}},
                                projection={'_id': 1},
                                collation=Config.marc_index_default_collation
                            )
                        )
                        xrefs = list(xrefs)

                        if len(xrefs) > 0:
                            if modifier == 'not':
                                q = {'$and': [q, {f'{tag}.subfields.xref': {'$nin': xrefs}}]}
                            else:
                                q = {'$or': [q, {f'{tag}.subfields.xref': {'$in': xrefs}}]}

                return Raw(q)
                
            # tag only syntax 
            # matches a single subfield only
            if match := re.match(r'(\d{3}):(.*)', token):
                tag, value = match.group(1, 2)
                value = process_string(value)
                
                if tag == '001':
                    # interpret 001 as id
                    try:
                        return Raw({'_id': int(value)})
                    except ValueError:
                        raise InvalidQueryString(f'ID must be a number')
                elif tag[:2] == '00':
                    return Raw({tag: value})

                # exists
                if value == '*':
                    return Raw({tag: {'$exists': False if modifier == 'not' else True}})

                # exact match
                if not isinstance(value, Regex):
                    if value[0] == '\'' and value[-1] == '\'':
                        return TagOnly(tag, value[1:-1], modifier=modifier, record_type=record_type)
                    elif value[0] == '\'' and value[-1] != '\'':
                        raise InvalidQueryString(f'Invalid exact match using single quote: "{token}"')

                # regex
                if isinstance(value, Regex):
                    if DB.handle[f'_index_{tag}'].estimated_document_count() > 100_000 and len(value.pattern) < 8:
                        # this is likely to be faster querying directly against the record collection
                        return TagOnly(tag, value, modifier=modifier, record_type=self.record_type)
                    
                    matches = DB.handle[f'_index_{tag}'].find({'subfields.value': value})
                    matched_subfield_values = []
                    
                    for m in matches:
                        matched_subfield_values += list(
                            filter(
                                lambda y: re.search(value.pattern, y, flags=value.flags), 
                                [x['value'] for x in m['subfields']]
                            )
                        )
                # text
                else:
                    quoted = re.findall(r'"(.+?)"', value)
                    quoted = [Tokenizer.scrub(x) for x in quoted]
                    # capture words starting with hyphen (denotes "not" search)
                    negated = [x[1] for x in re.findall(r'(^|\s)(\-\w+)', value)]
                    negated = [Tokenizer.scrub(x) for x in negated]

                    for _ in negated:
                        value = value.replace(_, '')

                        if not value.strip():
                            raise Exception('Search term can\'t contain only negations')

                    q = {'$and': [{'words': {'$all': Tokenizer.tokenize(value)}}]}

                    if negated:
                        q['$and'].append({'words': {'$nin': Tokenizer.tokenize(' '.join(negated))}})

                    for phrase in quoted:
                        q['$and'].append({'text': Regex(fr'\b{phrase}\b')})

                    matches = DB.handle[f'_index_{tag}'].find(q)
                    matched_subfield_values = []

                    for m in matches:
                        matched_subfield_values += [x['value'] for x in m['subfields']]

                    matched_subfield_values = list(set(filter(None, matched_subfield_values)))
                    stemmed_terms, filtered = Tokenizer.tokenize(value), []

                    for val in matched_subfield_values:
                        stemmed_val_words = Tokenizer.tokenize(val)

                        if all(x in stemmed_val_words for x in stemmed_terms):
                            filtered.append(val)

                    matched_subfield_values = filtered
                
                if sys.getsizeof(matched_subfield_values) > 1e6: # 1 MB
                    if isinstance(value, Regex):
                        # fall back to normal regex
                        return TagOnly(tag, value, modifier=modifier, record_type=record_type)

                    raise InvalidQueryString(f'Text search "{value}" has too many hits on field "{tag}". Try narrowing the search')
                elif len(matched_subfield_values) == 0:
                    return Raw({'_id': 0}) # query that matches no documents
                
                if modifier == 'not':
                    q = {f'{tag}': {'$not': {'$elemMatch': {'subfields': {'$elemMatch': {'value': {'$in': matched_subfield_values}}}}}}}
                else:
                    q = {f'{tag}.subfields.value': {'$in': matched_subfield_values}}

                auth_ctrl = Config.bib_authority_controlled if self.record_type == 'bib' else Config.auth_authority_controlled

                if tag in auth_ctrl:
                    source_tag = list(auth_ctrl[tag].values())[0]

                    xrefs = map(
                        lambda x: x['_id'], 
                        DB.auths.find(
                            {f'{source_tag}.subfields.value': {'$in': matched_subfield_values}},
                            projection={'_id': 1},
                            collation=Config.marc_index_default_collation
                        )
                    )
                    xrefs = list(xrefs)

                    if len(xrefs) > 0:
                        if modifier == 'not':
                            q = {'$and': [q, {f'{tag}.subfields.xref': {'$nin': xrefs}}]}
                        else:
                            q = {'$or': [q, {f'{tag}.subfields.xref': {'$in': xrefs}}]}

                return Raw(q)

            # id search
            if match := re.match('id:(.*)', token):
                if modifier:
                    raise Exception(f'modifier "{modifier}" not valid for ID search')

                value = match.group(1)
                
                try:
                    return Raw({'_id': int(value)})
                except ValueError:
                    raise InvalidQueryString(f'ID must be a number')

            # audit dates
            if match := re.match('(created|updated)([:<>])(.*)', token):
                field, operator, value = match.group(1, 2, 3)
                date = datetime.strptime(value, '%Y-%m-%d')

                if operator == '<':
                    return Raw({field: {'$lte': date}})
                elif operator == '>':
                    return Raw({field: {'$gte': date}})
                else:
                    return Raw({'$and': [{field: {'$gte': date}}, {field: {'$lte': date + timedelta(days=1)}}]})

            # audit users
            if match := re.match('(created_user|user):(.*)', token):
                field, value = match.group(1, 2)

                return Raw({field: process_string(value)})

            # xref (records that reference a given auth#)
            if match := re.match(f'xref:(.*)', token):
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

                if modifier == 'not':
                    return Raw({'$and': [{f'{tag}.subfields.xref': {'$not': {'$eq': xref}}} for tag in tags]})
                else:
                    return Raw({'$or': [{f'{tag}.subfields.xref': xref} for tag in tags]})
            
            # logical field
            if match := re.match(f'(\\w+):(.*)', token):
                logical_fields = list(Config.bib_logical_fields.keys()) + list(Config.auth_logical_fields.keys())
                field, value = match.group(1, 2)
                #todo: make aliases config
                field = 'symbol' if field == 's' else field
                field = 'subject' if field == 'heading' else field
                field = 'date' if field == 'meeting date' else field
                field = 'symbol' if field == 'meeting record' else field
                
                if field in logical_fields:
                    # exists
                    if value == '*':
                        return Raw({field: {'$exists': False if modifier == 'not' else True}})

                    # exact match
                    if value[0] == '\'' and value[-1] == '\'':
                        return Raw({field: value[1:-1] })
                    elif value[0] == '\'' and value[-1] != '\'':
                        raise InvalidQueryString(f'Invalid exact match using single quote: "{token}"')

                    value = process_string(value)

                    # regex
                    if isinstance(value, Regex):
                        q = {'_id': value}

                    # text
                    else:
                        quoted = re.findall(r'"(.+?)"', value)
                        quoted = [Tokenizer.scrub(x) for x in quoted]
                        negated = [x[1] for x in re.findall(r'(^|\s)(\-\w+)', value)]
                        negated = [Tokenizer.scrub(x) for x in negated]

                        for _ in negated:
                            value = value.replace(_, '')

                            if not value.strip():
                                raise Exception('Search term can\'t contain only negations')
                            
                        q = {'$and': [{'words': {'$all': Tokenizer.tokenize(value)}}]}

                        if negated:
                            q['$and'].append({'words': {'$nin': Tokenizer.tokenize(' '.join(negated))}})

                        for phrase in quoted:
                            q['$and'].append({'text': Regex(fr'\b{phrase}\b')})

                    matches = DB.handle[f'_index_{field}'].find(q)
                    values = [x['_id'] for x in matches]

                    if sys.getsizeof(values) > 1e6: # 1 MB
                        if isinstance(value, Regex):
                            # fall back to normal regex
                            return Raw({field: value}, modifier=modifier)
                
                        raise InvalidQueryString(f'Text search "{value}" has too many hits on field "{field}". Try narrowing the search')
                    elif len(values) == 0:
                        return Raw({'_id': 0}) # query that matches no documents

                    if modifier == 'not':
                        return Raw({field: {'$not': {'$in': values}}})
                    else:
                        return Raw({field: {'$in': values}})
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
                if i > 0 and tokens[i-1] not in ('AND', 'OR'):
                    raise InvalidQueryString('"NOT" must be preceeded by "AND", "OR"')

                if not len(tokens) > i + 1:
                    raise InvalidQueryString('"NOT" can\'t be at end of search string')

                if not re.match(r'^[^"\']+:', tokens[i + 1]):
                    raise InvalidQueryString('"NOT" not valid for all fields text search')

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

        if not self.conditions:
            return

        for condition in self.conditions:
            compiled.append(condition.compile())

        # TODO serialize data
        #if sys.getsizeof(json.dumps(compiled)) > 4e6: # 4 MB
        #   raise Exception("Text search is too broad")

        if len(compiled) == 1:
            return compiled[0]
        else:
            return {'$and': compiled}

    def to_dict(self):
        return self.compile()

    def to_json(self):
        return dumps(self.compile())

    def to_string(self):
        for c in self.conditions:
            pass

class QueryDocument(Query):
    def __init__(self, *args, **kwargs):
        warn('dlx.marc.QueryDocument is deprecated. Use dlx.marc.Query instead', DeprecationWarning)
        
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
        # if self.tag in [x for x in Config.bib_authority_controlled.keys()] + [x for x in Config.auth_authority_controlled.keys()]:
        
        if not self.record_type:
            if self.tag in [x for x in Config.bib_authority_controlled.keys()] + [x for x in Config.auth_authority_controlled.keys()]:
                warn('Record type is not set for query condition. Defaulting to bib')

                print(self.tag)
            
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
                    xrefs = [doc['_id'] for doc in DB.auths.find(lookup, projection={'_id': 1}, collation=Config.marc_index_default_collation)]

                subconditions.append(
                    SON({'$elemMatch': {'code': code, 'xref': xrefs[0] if len(xrefs) == 1 else {'$in' : xrefs}}})
                )

        submatch = subconditions[0] if len(subconditions) == 1 else {'$all' : subconditions}

        if not self.modifier:
            return SON({f'{tag}.subfields': submatch})
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
        # new text search

        # capture double quoted strings
        quoted = re.findall(r'(".+?")', self.string)
        # capture words starting with hyphen (denotes "not" search)
        negated = [x[1] for x in re.findall(r'(^|\s)(\-\w+)', self.string)]
        copied_string = self.string

        for _ in negated:
            copied_string = copied_string.replace(_, '')

        exclude = ('the', 'of', 'to', 'at', 'and', 'in', 'on', 'by', 'at', 'it', 'its')
        words = Tokenizer.tokenize(copied_string)
        words = list(filter(lambda x: x not in exclude, words))
        #words = list(filter(lambda x: len(x) > 1, words)) # ignore one-letter words

        data = {}
        
        if negated:
            negated = Tokenizer.tokenize(' '.join(negated))

            if words:
                data.update({'$and': [{'words': {'$all': words}}, {'words': {'$nin': negated}}]})
            else:
                raise Exception('Search term can\'t contain only negations')
        elif words:
            data.update({'words': {'$all': words}})

        if len(quoted) > 1:
            data['$and'] = [{'text': Regex(rf'\s{Tokenizer.scrub(x)}\s')} for x in quoted]
        elif len(quoted) == 1:
            data['text'] = Regex(rf'\s{Tokenizer.scrub(quoted[0])}\s')

        # use the text index for these cases
        text_searches = []
        #if negated: text_searches += negated
        #if quoted: text_searches += quoted
        #if hyphenated: text_searches += [f'"{x}"' for x in hyphenated]
        
        if text_searches:
            data['$text'] = {'$search': ' '.join(text_searches)}
        
        return data

    def atlas_compile(self):
        return {
            "$search": {
                "index": "default",
                "text": {
                    "path": {"wildcard": "*"},
                    "query": self.string
                }
            }
        }

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
            if tag in list(Config.bib_authority_controlled.keys()) + list(Config.auth_authority_controlled.keys()):
                warn('Record type is not set for query condition. Defaulting to bib')
            
            self.record_type = 'bib'
            
        auth_ctrl = Config.bib_authority_controlled if self.record_type == 'bib' else Config.auth_authority_controlled
        
        if tag in auth_ctrl:
            source_tag = list(auth_ctrl[tag].values())[0]
            
            xrefs = map(
                lambda x: x['_id'], 
                DB.auths.find({f'{source_tag}.subfields.value': value}, projection={'_id': 1}, collation=Config.marc_index_default_collation)
            )
            xrefs = list(xrefs)
            
            if modifier is None:
                self.condition = Or(
                    Raw({f'{tag}.subfields.value': value}),
                    Raw({f'{tag}.subfields.xref': {'$in': xrefs}})
                ) if xrefs else Raw(
                    {f'{tag}.subfields.value': value}
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

class AtlasQuery():
    '''Compiles into an aggregation pipeline'''

    def __init__(self):
        self.conditions = []
        self.match = None # the standard query to use in $match stage

    @classmethod
    def from_string(cls, string, *, record_type=None):
        self = cls()
        standard_query = Query.from_string(string)
        standard_conditions = standard_query.conditions
        self.conditions = []
        
        # remove text conditions for conversion to Atlas conditions
        for i, cond in enumerate(standard_conditions):
            if isinstance(cond, Text):
                self.conditions.append(standard_conditions.pop(i))

        # save the rest of the standard query to use in $match aggregation stage
        self.match = Query(*standard_conditions) if standard_conditions else None

        return self

    def compile(self):
        pipeline = []

        if self.conditions:
            pipeline += [x.atlas_compile() for x in self.conditions]

        if self.match:
            pipeline.append({'$match': self.match.compile()})

        return pipeline
