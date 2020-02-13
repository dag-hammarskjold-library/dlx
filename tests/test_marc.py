import pytest
import inspect
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
    DB.bibs.insert_many(bibs)
    DB.auths.drop
    DB.auths.insert_many(auths)
    
    return DB.client

### Tests

# Tests run in order.
# Remember to prefix test function names with "test_".
# Database state is maintained globally. Use the `db` fixture to reset the test database.

def test_init_marc():
    with pytest.raises(Exception):
        Marc()

def test_init_bib(db, bibs):
    from dlx.marc import Marc, Bib, Controlfield, Datafield, Subfield, Literal, Linked
    
    bib = Bib(bibs[0])
    assert isinstance(bib, Marc)
    assert isinstance(bib, Bib)
    
    assert len(bib.controlfields) == 2
    assert len(bib.datafields) == 5
    assert len(bib.fields) == 7
    
    for field in bib.controlfields:
        assert isinstance(field, Controlfield)
        
    for field in bib.datafields:
        assert isinstance(field, Datafield)
        assert field.record_type == 'bib'
        assert field.tag and field.ind1 and field.ind2
        
        for subfield in field.subfields:
            assert isinstance(subfield, Subfield)
            assert isinstance(subfield, (Literal, Linked))
            assert subfield.code and subfield.value
            
def test_init_auth(db, auths):
    from dlx.marc import Marc, Auth, Controlfield, Datafield, Subfield, Literal, Linked
    
    auth = Auth(auths[0])
    assert isinstance(auth, Marc)
    assert isinstance(auth, Auth)
    
    assert len(auth.controlfields) == 0
    assert len(auth.datafields) == 1
    assert len(auth.fields) == 1
    
    for field in auth.controlfields:
        assert isinstance(field, Controlfield)
        
    for field in auth.datafields:
        assert isinstance(field, Datafield)
        assert field.record_type == 'auth'
        assert field.tag and field.ind1 and field.ind2
        
        for subfield in field.subfields:
            assert isinstance(subfield, Subfield)
            assert isinstance(subfield, (Literal, Linked))
            assert subfield.code and subfield.value

def test_commit(db, bibs, auths):
    from dlx import DB
    from dlx.marc import Bib, Auth
    from datetime import datetime
    
    with pytest.raises(Exception):
        Bib({'_id': 'I am invalid'}).commit()
    
    for bib in [Bib(x) for x in bibs]:
        assert bib.commit().acknowledged
        
    for auth in [Auth(x) for x in auths]:
        assert auth.commit().acknowledged
        
    bib = Bib({'_id': 3})
    assert bib.commit().acknowledged
    assert isinstance(bib.updated, datetime)

def test_find_one(db, bibs, auths):
    from dlx.marc import Bib, Auth
    
    bib = Bib.find_one({'_id': 1})
    assert bib.id == 1
    assert isinstance(bib, Bib)
    
    auth = Auth.find_one({'_id': 1})
    assert auth.id == 1
    assert isinstance(auth, Auth)
        
def test_find(db):
    from dlx.marc import Bib, Auth
        
    bibs = Bib.find({})
    assert inspect.isgenerator(bibs)
    assert len(list(bibs)) == 2
    
    for bib in Bib.find({}):
        assert isinstance(bib, Bib)
    
    auths = Auth.find({})
    assert inspect.isgenerator(auths)
    assert len(list(auths)) == 2
    
    for auth in Auth.find({}):
        assert isinstance(auth, Auth)
        
    auths = Auth.find({'_id': 1})
    assert len(list(auths)) == 1
        
    auths = Auth.find({}, limit=1)
    assert len(list(auths)) == 1
    
    auths = Auth.find({}, limit=0, skip=1, projection={})
    assert len(list(auths)) == 1
        
def test_find_id():
    from dlx.marc import Bib, Auth
    # candidate to replace .match_id
    pass
    
def test_querydocument(db):
    from dlx.marc import Bib, QueryDocument, Condition, Or
    from bson import SON
    from json import loads
    
    query = QueryDocument(Condition(tag='245', subfields={'a': 'This'}))
    assert isinstance(query.compile(), SON)
    
    qjson = query.to_json()
    qdict = loads(qjson)
    assert qdict['245']['$elemMatch']['subfields']['$elemMatch']['code'] == 'a'
    assert qdict['245']['$elemMatch']['subfields']['$elemMatch']['value'] == 'This'
    
    query = QueryDocument(
        Condition(tag='245', subfields={'b': 'is the', 'c': 'title'}),
        Condition(tag='650', modifier='exists'),
        Or(
            Condition(tag='710', modifier='exists'),
            Condition(tag='520', modifier='not_exists')
        )
    )
    
    assert len(list(Bib.find(query.compile()))) == 2
    
def test_get_field(bibs):
    from dlx.marc import Bib, Field, Controlfield, Datafield
    
    bib = Bib(bibs[0])
    assert isinstance(bib.get_field('000'), Controlfield)
    assert isinstance(bib.get_field('245'), Datafield)
    assert bib.get_field('245').tag == '245'
    
    fields = bib.get_fields('245', '520')
    assert isinstance(fields, filter)
    for field in fields:    
        assert isinstance(field, Field)
        
    bib = Bib()
    for tag in ('400', '100', '500', '300', '200'):
        bib.set(tag, 'a', 'test')
        
    assert [field.tag for field in bib.get_fields()] == ['100', '200', '300', '400', '500']
    
def test_field_get_value(bibs):
    from dlx.marc import Bib
    
    bib = Bib(bibs[0])
    field = bib.get_field('245')
    assert field.get_value('a') == 'This'
    assert field.get_values('a', 'b') == ['This', 'is the']

def test_get_value(bibs):
    from dlx.marc import Bib
    
    bib = Bib(bibs[0])
    assert bib.get_value('000') == 'leader'
    assert bib.get_value('245', 'a') == 'This'
    assert bib.get_values('245', 'a', 'b') == ['This', 'is the']
    assert bib.get_value('520', 'a', address=[1, 1]) == 'Repeated subfield'
    assert bib.get_values('520', 'a', place=1) == ['Another description', 'Repeated subfield']
    assert bib.get_value('999', 'a') == ''
    assert bib.get_values('999', 'a') == []
    
    assert bib.get_value('245', 'a') == bib.get('245', 'a')
    assert bib.get_values('520', 'a') == bib.gets('520', 'a')
    
def test_get_xref(db, bibs):
    from dlx.marc import Bib
    
    bib = Bib(bibs[0])
    assert bib.get_xref('650', 'a') == 1
    bib.set('710', 'a', 3, address='+')
    assert bib.get_xrefs('710') == [2,3]
    
def test_set():
    from dlx.marc import Bib
    
    bib = Bib()
    bib.set('245', 'a', 'Edited')
    assert bib.get_value('245', 'a') == 'Edited'
    
    bib.set('245', 'a', 'Repeated field', address=['+'])
    assert bib.get_values('245', 'a') == ['Edited', 'Repeated field']
    
    bib.set('245', 'a', 'Repeated field edited', address=[1])
    assert bib.get_value('245', 'a', address=[1, 0]) == 'Repeated field edited'
    
    bib.set('245', 'a', 'Repeated subfield', address=[1, '+'])
    assert bib.get_value('245', 'a', address=[1, 1]) == 'Repeated subfield'
    
    bib.set('651', 'a', 9)
    assert bib.get_xref('651', 'a') == 9
    
    bib = Bib().set_values(
        ('245', 'a', 'yet another'),
        ('245', 'b', 'title'),
        ('500', 'a', 'desc'),
        ('500', 'a', 'desc', {'address': ['+']}),
    )
    
    assert bib.get_values('245', 'a', 'b') == ['yet another', 'title']
    assert bib.get_values('500', 'a') == ['desc', 'desc']

def test_set_008(bibs):
    from dlx.marc import Bib
    from dlx.config import Config
    import time
    
    bib = Bib(bibs[0])
    date_tag, date_code = Config.date_field
    bib.set(date_tag, date_code, '19991231')
    
    with pytest.raises(Exception):
        bib.set('008', None, 'already set')
        bib.set_008()
    
    bib.set('008', None, '')
    bib.set_008();

    assert bib.get_value('008')[0:6] == time.strftime('%y%m%d')
    assert bib.get_value('008')[7:11] == '1999'

def test_auth_lookup(db):
    from dlx.marc import Bib, Auth
    
    bib = Bib.find_one({'_id': 1})
    assert bib.get_xref('650', 'a') == 1
    assert bib.get_value('650', 'a') == 'Header'

    auth = Auth.find_one({'_id': 1})
    auth.set('150', 'a', 'Changed').commit()
    assert bib.get_value('650', 'a') == 'Changed'
    
def test_language(db):
    from dlx.marc import Bib, Auth
    
    Auth({'_id': 3}).set('150', 'a', 'Text').set('994', 'a', 'Texto').commit()
    bib = Bib({'_id': 3}).set('650', 'a', 3)
    assert bib.get('650', 'a', language='es') == 'Texto'
    
def test_to_xml(db):
    from dlx.marc import Bib
    
    control = '<record><controlfield tag="000">leader</controlfield><controlfield tag="008">controlfield</controlfield><datafield ind1=" " ind2=" " tag="245"><subfield code="a">This</subfield><subfield code="b">is the</subfield><subfield code="c">title</subfield></datafield><datafield ind1=" " ind2=" " tag="520"><subfield code="a">Description</subfield></datafield><datafield ind1=" " ind2=" " tag="520"><subfield code="a">Another description</subfield><subfield code="a">Repeated subfield</subfield></datafield><datafield ind1=" " ind2=" " tag="650"><subfield code="a">Header</subfield></datafield><datafield ind1=" " ind2=" " tag="710"><subfield code="a">Another header</subfield></datafield></record>'
    
    bib = Bib.find_one({'_id': 1})
    assert bib.to_xml() == control
    
def test_to_mrc(db):
    from dlx.marc import Bib, Auth
    
    control = '00224r|||a2200097|||4500008001300000245002400013520001600037520004300053650001100096710001900107controlfield  aThisbis thectitle  aDescription  aAnother descriptionaRepeated subfield  aHeader  aAnother header'

    bib = Bib.find_one({'_id': 1})
    assert bib.to_mrc() == control
    
    control = '00049||||a2200037|||4500150001100000  aHeader'
   
    auth = Auth.find_one({'_id': 1})
    assert auth.to_mrc() == control
    
    auth.set('994', 'a', 'Titulo').commit()
    assert bib.to_mrc(language='es') == '00224r|||a2200097|||4500008001300000245002400013520001600037520004300053650001100096710001900107controlfield  aThisbis thectitle  aDescription  aAnother descriptionaRepeated subfield  aTitulo  aAnother header'

def test_to_mrk(bibs):
    from dlx.marc import Bib
    
    control = '000  leader\n008  controlfield\n245  \\\\$aThis$bis the$ctitle\n520  \\\\$aDescription\n520  \\\\$aAnother description$aRepeated subfield\n650  \\\\$aHeader\n710  \\\\$aAnother header\n'

    bib = Bib.find_one({'_id': 1})
    assert bib.to_mrk() == control
