import pytest, fakeredis
from mongomock import MongoClient as MockClient
from datetime import datetime

@pytest.fixture
def bibs():
    return [
        {
            '_id': 1,
            #'created': datetime.now(),
            #'created_user': 'admin',
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
            #'created': datetime.now(),
            #'created_user': 'admin',
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
            #'created': datetime.now(),
            #'created_user': 'admin',
            '150': [
                {
                    'indicators': [' ', ' '],
                    'subfields':[{'code': 'a', 'value': 'Header'}]
                }
            ]
        },
        {
            '_id': 2,
            #'created': datetime.now(),
            #'created_user': 'admin',
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
    from dlx.marc import Auth

    # Connects to and resets the database
    DB.connect('mongomock://localhost')

    # reset the caches
    Auth._cache = {}
    
    DB.bibs.drop()
    DB.handle['bib_history'].drop()
    DB.handle['bib_id_counter'].drop()
    DB.bibs.insert_many(bibs)
    
    DB.auths.drop()
    DB.handle['auth_history'].drop()
    DB.handle['auth_id_counter'].drop()
    DB.auths.insert_many(auths)
    
    DB.files.drop()

@pytest.fixture
def redis_client(request):
    redis_client = fakeredis.FakeRedis()
    return redis_client