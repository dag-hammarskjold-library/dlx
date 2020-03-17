"""
Configurations
"""

import os
import json

class Config():

    # schemas
    schema_dir = os.path.dirname(__file__) + '/schemas/'

    with open(schema_dir + 'jmarc.schema.json') as f:
        jmarc_schema = json.loads(f.read())

    #with open(schema_dir + '/jfile.schema.json') as f:
    #   jfile_schema = json.loads(f.read())
    
    date_field = ['269', 'a']
    
    bib_index_fields = {
        '191': 'hybrid',
        '269': 'literal',
        '791': 'hybrid', 
        '930': 'literal', 
        '998': 'literal'
    }
    
    auth_index_fields = {
        '100': 'literal',
        '110': 'literal',
        '111': 'literal',
        '150': 'literal',
        '190': 'literal'
    }

    # this is used by dlx.query to locate the linked value
    bib_authority_controlled = {
        '191': {'b': '190', 'c': '190'},
        '600': {'a': '100'},
        '610': {'a': '110'},
        '611': {'a': '111'},
        '630': {'a': '130'},
        '650': {'a': '150'},
        '651': {'a': '151'},
        '700': {'a': '100'},
        '710': {'a': '110'},
        '711': {'a': '111'},
        '730': {'a': '130'},
        '791': {'b': '190', 'c' : '190'},
        '991': {'a': '191', 'b': '191', 'c': '191', 'd': '191'}
    }

    auth_authority_controlled = {
        #'491': {'a': '191'}, # ?
        '500': {'a': '100'},
        '510': {'a': '100'},
        '511': {'a': '100'},
        '550': {'a': '100'},
        '551': {'a': '100'},
    }
    
    auth_language_tag = {
        '150': {'fr': '993', 'es': '994'},
        '151': {'fr': '993', 'es': '994'}
    }

    @staticmethod
    def is_authority_controlled(record_type, tag, code):
        if record_type == 'bib':
            index = Config.bib_authority_controlled
        elif record_type == 'auth':
            index = Config.auth_authority_controlled

        if tag not in index:
            return False

        if code in index[tag]:
            return True

        return False

    @staticmethod
    def authority_source_tag(record_type, tag, code):
        if record_type == 'bib':
            index = Config.bib_authority_controlled
        elif record_type == 'auth':
            index = Config.auth_authority_controlled

        if tag not in index:
            return

        if code in index[tag]:
            return index[tag][code]
            
        return
    
    @staticmethod    
    def auth_heading_tags():
        r = []
        
        for tag, langs in list(Config.bib_authority_controlled.items()) + list(Config.auth_authority_controlled.items()):
            r += [tag for tag in langs.values()]
        
        return list(set(r))
    
    @staticmethod
    def language_source_tag(tag, language):
        tags = Config.auth_language_tag
        
        if tag in tags:
            if language in tags[tag]:
                return tags[tag][language]
                
        return
        
    @staticmethod
    def linked_language_source_tag(record_type, tag, code, language):
        auth_tag = Config.authority_source_tag(record_type, tag, code)
        
        return Config.language_source_tag(auth_tag, language)
    
    @staticmethod    
    def get_language_tags():
        r = []
        
        for tag, langs in Config.auth_language_tag.items():
            for t in langs.values():
                r.append(t)
        
        return r
