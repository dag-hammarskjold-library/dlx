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
        '491': {'a': '191'},
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
    
    auth_index = ['100', '110', '111', '130', '150', '190', '998'] + list(auth_authority_controlled.keys())
    auth_index_case_insensitive = ['100', '110', '111']
    
    bib_logical_fields = {
        'symbol': {
            '079': ['a'],
            '767': ['o'],
            '191': ['a', 'z'],
            '791': ['a', 'z']
        },
        'subject': {
            '600': ['abcdq'],
            '610': ['abcdfgkn'],
            '611': ['acdegknq'],
            '630': ['adfgklnp'],
            '650': ['a'],
            '991': ['abcd']
        },
        'title': {
            '130': ['adfgklnp'],
            '130': ['adfgklnp'],
            '490': ['a'],
            '495': ['a'],
            '245': ['p'],
            '495': ['a'],
            '765': ['t'],
            '767': ['t'],
            '770': ['t'],
            '772': ['t'],
            '773': ['t'],
            '775': ['t'],
            '776': ['t'],
            '777': ['t'],
            '780': ['t'],
            '785': ['t'],
            '787': ['t'],
            '210': ['ab'],
            '222': ['ab'],
            '239': ['ab'],
            '740': ['anp'],
            '242': ['abnp'],
            '245': ['abhnp'],
            '243': ['adfgnp'],
            '246': ['abfgnp'],
            '247': ['abfgnp'],
            '240': ['adfgklnp']
        },
        'notes': {
            '490': ['a'],
            '495': ['a'],
            '500': ['a'],
            '505': ['a'],
            '598': ['a'],
            '501': ['a'],
            '502': ['a'],
            '504': ['a'],
            '506': ['a'],
            '510': ['a'],
            '515': ['a'],
            '516': ['a'],
            '518': ['a'],
            '520': ['a'],
            '521': ['a'],
            '522': ['a'],
            '523': ['a'],
            '524': ['a'],
            '525': ['a'],
            '530': ['a'],
            '533': ['a'],
            '534': ['a'],
            '535': ['a'],
            '536': ['a'],
            '538': ['a'],
            '540': ['a'],
            '541': ['a'],
            '544': ['a'],
            '545': ['a'],
            '546': ['a'],
            '547': ['a'],
            '550': ['a'],
            '556': ['a'],
            '561': ['a'],
            '580': ['a'],
            '591': ['a'],
            '592': ['a'],
            '593': ['a'],
            '513': ['ab'],
            '555': ['ad'],
            '505': ['rgt'],
        },
        'author': {
            '111': ['acdgtxyz'],
            '711': ['acdgtxyz'],
            '110': ['abcdgtxyz'],
            '710': ['abcdgtxyz'],
            '100': ['abcdgqtxyz'],
            '700': ['abcdgqtxyz'],
            '130': ['adfgklnp'],
            '730': ['adfgklnp'],
        },
        'related_docs': {
            '993': ['a']
        },
        'prodinf': {
            '930': ['a']
        },
        'bib_creator': {
            '999': ['abc']
        },
        'type': {
            '089': ['b']
        },
        'date': {
            '269': ['a'],
            '992': ['a']
        }
    }
    
    auth_logical_fields = {
        'heading': {
            '100': ['abcdq'],
            '110': ['abcdfgkn'],
            '111': ['acdegknq'],
            '130': ['adfgklnp'],
            '150': ['a'],
            '191': ['abcd']
        },
        'agenda_title': {
            '191': ['c']
        },
        'agenda_subject': {
            '191': ['d']
        },
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
    
    @staticmethod
    def auth_controlled_bib_logical_fields():
        fields = []

        for field, d in Config.bib_logical_fields.items():
            for tag, codes in d.items():
                for codeset in codes:
                    for code in codeset:
                        if flag := Config.is_authority_controlled('bib', tag, code):
                            fields.append(field)

        return list(set(fields))
        
    @staticmethod
    def auth_controlled_auth_logical_fields():
        fields = []

        for field, d in Config.auth_logical_fields.items():
            for tag, codes in d.items():
                for codeset in codes:
                    for code in codeset:
                        if flag := Config.is_authority_controlled('auth', tag, code):
                            fields.append(field)

        return list(set(fields))
