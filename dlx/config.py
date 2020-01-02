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

    # this is used by dlx.query to locate the linked value
    bib_authority_controlled = {
        '191': {'b': '190', 'c': '190'},
        '600': {'a': '100'},
        '610': {'a': '110'},
        '650': {'a': '150'},
        '651': {'a': '151'},
        '700': {'a': '100'},
        '710': {'a': '110'},
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
            raise Exception('{}{} is not configured as authority-controlled'.format(tag, code))

        if code in index[tag]:
            return index[tag][code]

        raise Exception('{}{} is not configured as authority-controlled'.format(tag, code))
