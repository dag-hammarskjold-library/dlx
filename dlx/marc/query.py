
from bson import SON
from bson.json_util import dumps
from dlx.db import DB
from dlx.config import Config

class QueryDocument():
    @classmethod
    def from_string(cls, string):
        raise NotImplementedError
        
    def __init__(self, *conditions):
        self.conditions = conditions

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
        self.record_type = kwargs.get('record_type', 'bib').lower()

        if self.record_type not in ('bib', 'auth'):
            raise Exception('Invalid record type')

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
        tag = self.tag
        subconditions = []

        for sub in self.subfields:
            code = sub[0]
            val = sub[1]

            if not Config.is_authority_controlled(self.record_type, tag, code):
                subconditions.append(
                    SON({'$elemMatch':{'code':code, 'value':val}})
                )
            else:
                if isinstance(val, int):
                    xrefs = [val]
                else:
                    auth_tag = Config.authority_source_tag(self.record_type, tag, code)
                    lookup = SON(
                        {auth_tag+'.subfields':SON({'$elemMatch':{'code':code, 'value':val}})}
                    )
                    xrefs = [doc['_id'] for doc in DB.auths.find(lookup, {'_id':1})]

                subconditions.append(
                    SON({'$elemMatch': {'code': code, 'xref': xrefs[0] if len(xrefs) == 1 else {'$in' : xrefs}}})
                )

        submatch = subconditions[0] if len(subconditions) == 1 else {'$all' : subconditions}

        if not self.modifier:
            return SON({tag:{'$elemMatch':{'subfields':submatch}}})
        else:
            if self.modifier == 'not':
                return SON({'$or':[{tag:{'$not':{'$elemMatch':{'subfields':submatch}}}}, {tag:{'$exists':False}}]})
            elif self.modifier == 'exists':
                return {tag: {'$exists': True}}
            elif self.modifier == 'not_exists':
                return {tag: {'$exists': False}}
            else:
                raise Exception('Invalid modifier')
