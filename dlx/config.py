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
        '100': {'a': '100'},
        '110': {'a': '110'},
        '111': {'a': '111'},
        '130': {'a': '130'},
        '191': {'b': '190', 'c': '190'},
        '370': {'a': '110'},
        '440': {'a': '140'},
        '600': {'a': '100', 'g': '100'},
        '610': {'a': '110', 'g': '110'},
        '611': {'a': '111', 'g': '111'},
        '630': {'a': '130', 'g': '130'},
        '650': {'a': '150'},
        '651': {'a': '151'},
        '700': {'a': '100', 'g': '100'},
        '710': {'a': '110'},
        '711': {'a': '111'},
        '730': {'a': '130'},
        '791': {'b': '190', 'c' : '190'},
        '830': {'a': '130'},
        '991': {'a': '191', 'b': '191', 'c': '191', 'd': '191'}
    }

    auth_authority_controlled = {
        #'491': {'a': '191', 'b': '191', 'c': '191', 'd': '191'},
        '500': {'a': '100'},
        '510': {'a': '110'},
        '511': {'a': '111'},
        '530': {'a': '130'},
        '550': {'a': '150'},
        '551': {'a': '151'},
        '591': {'a': '191', 'b': '191', 'c': '191', 'd': '191'},
    }
    
    auth_language_tag = {
        '150': {'fr': '993', 'es': '994', 'ar': '995', 'zh': '996', 'ru': '997'},
        '151': {'fr': '993', 'es': '994', 'ar': '995', 'zh': '996', 'ru': '997'},
    }
    
     # auth-controlled fields are automatically indexed
    bib_index = ['089', '191', '245', '246', '249', '269', '500', '520', '546', '930', '991', '989', '998', '999']
    bib_index_case_insensitive = []  
    bib_index_logical_numeric = []
    bib_text_index_weights = {}
    
    # auth-controlled fields are automatically indexed
    auth_index = ['100', '110', '111', '130', '140', '150', '190', '400', '410', '411', '430', '440', '450', '998', '999']
    auth_index_case_insensitive = []
    auth_index_logical_numeric = []
    auth_text_index_weights = {}

    # records with these values will be tagged as such in the special _record_type logical field
    bib_type_map = {
        'speech': ['089', 'b', 'B22'],
        'vote': ['089', 'b', 'B23']
    }

    auth_type_map = {}
    
    bib_logical_fields = {
        'symbol': {
            '079': ['a'],
            '767': ['o'],
            '191': ['a', 'z'],
            '791': ['a']
        },
        'body': {
            '191': ['bc'],
            '791': ['bc']
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
            #'505': ['a'],
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
            '505': ['argt'],
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
        },
        'agenda': {
            '991': ['abcd']
        },
        'series': {
            '440': ['a'],
            '490': ['a'],
            '830': ['a']
        },
        'speaker': {
            '700': 'a'
        },
        'country_org': {
            '710': ['a'],
            '711': ['a']
        },
        'call_number': {
            '099': ['c']
        }
    }
    
    auth_logical_fields = {
        'heading': {
            '100': ['abcdq'],
            '110': ['abcdfgkn'],
            '111': ['acdegknq'],
            '150': ['a'],
            '130': ['adfgklnp'],
            '190': ['bc'],
            '191': ['abcd'],
        },
        'subject': {
            '100': ['abcdq'],
            '400': ['a'],
            '500': ['a'],
            '110': ['abcdfgkn'],
            '410': ['a'],
            '510': ['a'],
            '111': ['acdegknq'],
            '411': ['a'],
            '511': ['a'],
            '130': ['adfgklnp'],
            '430': ['a'],
            '530': ['a'],
            '150': ['a'],
            '191': ['abcd'],
            '190': ['bc'],
            '491': ['d'],
            '591': ['abcd']
        },
        'agenda': {
            '191': ['abcd'],
            '491': ['d'],
            '591': ['abcd']
        },
        'agenda_title': {
            '191': ['c'],
            '591': ['c']
        },
        'agenda_subject': {
            '191': ['d'],
            '491': ['d'],
            '591': ['d']
        },
        'series': {
            '130': ['adfgklnp'],
            '430': ['a'],
            '530': ['a']
        },
        'author': {
            '100': ['abcdgq'],
            '400': ['a'],
            '500': ['a'],
            '110': ['abcdfgkn'],
            '410': ['a'],
            '510': ['a'],
            '111': ['acdegknq'],
            '411': ['a'],
            '511': ['a'],
            '130': ['adfgklnp'],
            '430': ['a'],
            '530': ['a']
        },
        'thesaurus': {
            '150': ['a'],
            '450': ['a'],
            '550': ['a']
        },
        'body': {
            '190': ['bc']
        },
        # disable for now
        #'states': {
        #    '110': ['a'] #todo: {'requires': ['b', 'ms']}]
        #}
    }

    # the collation used in the MDB CE indexes on the bibs and auths collections
    marc_index_default_collation = {'locale': 'en', 'strength': 1, 'numericOrdering': True}

    # control subthreading of database actions on Marc.commit() and Auth.merge() for debugging
    threading = True

    # utility functions
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
    def auth_linked_codes(heading_tag):
        # returns the subfield codes that can be linked to for the given auth heading tag
        codes = []

        for tag, subdict in list(Config.bib_authority_controlled.items()) + list(Config.auth_authority_controlled.items()):
            for code, tag in subdict.items():
                if tag == heading_tag:
                    codes.append(code)

        return [str(x) for x in codes]
            
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
                        flag = Config.is_authority_controlled('bib', tag, code)
                        
                        if flag:
                            fields.append(field)

        return list(set(fields))
        
    @staticmethod
    def auth_controlled_auth_logical_fields():
        fields = []

        for field, d in Config.auth_logical_fields.items():
            for tag, codes in d.items():
                for codeset in codes:
                    for code in codeset:
                        flag = Config.is_authority_controlled('auth', tag, code)
                        
                        if flag:
                            fields.append(field)

        return list(set(fields))

    @staticmethod
    def bib_type_map_tags():
        return list(set([mapping[0] for field, mapping in Config.bib_type_map.items()]))

    @staticmethod
    def auth_type_map_tags():
        return list(set([mapping[0] for field, mapping in Config.auth_type_map.items()]))

