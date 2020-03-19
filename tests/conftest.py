import pytest
from mongomock import MongoClient as MockClient

@pytest.fixture
def bibs():
    return [
        {
            '_id': 1,
            '000': ['leader'],
            '008': ['controlfield'],
            '245': [
                {
                    'indicators' : [' ',' '],
                    'subfields' : [{'code': 'a', 'value': 'This'}, {'code': 'b', 'value': 'is the'}, {'code': 'c', 'value': 'title'}]
                }
            ],
            '520': [
                {
                    'indicators' : [' ' ,' '],
                    'subfields' : [
                        {'code': 'a', 'value': 'Description'}]
                },
                {
                    'indicators': [' ' ,' '],
                    'subfields': [{'code': 'a', 'value': 'Another description'}, {'code' : 'a','value': 'Repeated subfield'}]
                }
            ],
            '650': [
                {
                    'indicators': [' ', ' '],
                    'subfields': [{'code' : 'a', 'xref' : 1}],
                }
            ],
            '710': [
                {
                    'indicators' : [' ',' '],
                    'subfields' : [{'code' : 'a', 'xref' : 2}]
                }
            ]
        },
        {
            '_id': 2,
            '000': ['leader'],
            '245': [
                {
                    'indicators' : [' ',' '],
                    'subfields':[{'code': 'a', 'value': 'Another'}, {'code': 'b', 'value': 'is the'}, {'code': 'c', 'value': 'title'}]
                }
            ],
            '650': [
                {
                    'indicators' : [' ' ,' '],
                    'subfields' : [{'code' : 'a', 'xref' : 1}]
                }
            ]
        }
    ]

@pytest.fixture
def auths():
    return [
        {
            '_id': 1,
            '150': [
                {
                    'indicators': [' ', ' '],
                    'subfields':[{'code': 'a', 'value': 'Header'}]
                }
            ]
        },
        {
            '_id': 2,
            '110': [
                {
                    'indicators' : [' ', ' '],
                    'subfields' : [{'code' : 'a', 'value' : 'Another header'}]
                }
            ]
        }
    ]

@pytest.fixture
def db(bibs, auths) -> MockClient: 
    from dlx import DB
    # Connects to and resets the database
    DB.connect('mongomock://localhost')
    
    DB.bibs.drop
    DB.handle['bib_history'].drop
    DB.bibs.insert_many(bibs)
    DB.auths.drop
    DB.handle['auth_history'].drop
    DB.auths.insert_many(auths)
    
    return DB.client