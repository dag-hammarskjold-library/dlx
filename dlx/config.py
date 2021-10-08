"""
Configurations
"""

import os
import json

class Config():

    # schemas
    schema_dir = os.path.dirname(__file__) + '/schemas/'

    with open(schema_dir + 'jmarc.schema.json') as ms:
        jmarc_schema = json.loads(ms.read())

    with open(schema_dir + 'jfile.schema.json') as fs:
        jfile_schema = json.loads(fs.read())
    
    date_field = ['269', 'a']
    
    # this is used by dlx.query to locate the linked value
    bib_authority_controlled = {
        '191': {'b': '190', 'c': '190'},
        '600': {'a': '100', 'g': '100'},
        '610': {'a': '110', 'g': '110'},
        '611': {'a': '111', 'g': '111'},
        '630': {'a': '130', 'g': '130'},
        '650': {'a': '150'},
        '651': {'a': '151'},
        '700': {'a': '100', 'g': '100'},
        '710': {'a': '110', '9': '110'},
        '711': {'a': '111', 'g': '111'},
        '730': {'a': '130'},
        '791': {'b': '190', 'c' : '190'},
        '830': {'a': '130'},
        '991': {'a': '191', 'b': '191', 'c': '191', 'd': '191'}
    }

    auth_authority_controlled = {
        #'491': {'a': '191'}, # ?
        '500': {'a': '100'},
        '510': {'a': '110'},
        '511': {'a': '111'},
        '550': {'a': '150'},
        '551': {'a': '151'},
    }
    
    auth_language_tag = {
        '150': {'fr': '993', 'es': '994', 'ar': '995', 'zh': '996', 'ru': '997'},
        '151': {'fr': '993', 'es': '994', 'ar': '995', 'zh': '996', 'ru': '997'},
    }
    
    bib_index = ['269', '930', '998'] + list(bib_authority_controlled.keys())
    bib_index_case_insensitive = ['191']  
    
    auth_index = ['100', '110', '111', '130', '150', '190']
    auth_index_case_insensitive = ['100', '110', '111']
    
    logical_fields = {
        # field names must be unique to both bibs and auths
        # WIP
        'title': {
            '245': ['a', 'b', 'c'],
            '246': ['a', 'b', 'c']
        },
        'symbol': {
            '191': ['a'],
            '791': ['a']
        }
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
        
        for tag, code in list(Config.bib_authority_controlled.items()) + list(Config.auth_authority_controlled.items()):
            r += [t for t in code.values()]
        
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
    def auth_language_tags():
        r = []
        
        for tag in Config.auth_language_tag.values():
            for lang in tag.keys():
                r.append(tag[lang])
        
        return list(set(r))
