import pytest, inspect

# Fixtures are loaded automatically by pytest from ./conftest.py
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
    from bson import ObjectId
    from jsonschema.exceptions import ValidationError
    
    with pytest.raises(Exception):
        Bib({'_id': 'I am invalid'}).commit()
    
    for bib in [Bib(x) for x in bibs]:
        assert bib.commit().acknowledged
        
    for auth in [Auth(x) for x in auths]:
        assert auth.commit().acknowledged
        
    bib = Bib({'_id': 3})
    assert bib.commit().acknowledged
    assert isinstance(bib.updated, datetime)
    assert bib.user == 'admin'
    assert bib.history()[0].to_dict() == bib.to_dict()
    assert bib.history()[0].user == 'admin'
    assert Bib.max_id() == 3
    
    Bib().commit()
    Bib().commit()
    assert Bib.max_id() == 5
    
    DB.bibs.drop()
    DB.handle['bib_id_counter'].drop()
    Bib().commit()
    assert Bib.max_id() == 1

def test_delete(db):
    from dlx import DB
    from dlx.marc import Bib
    from datetime import datetime
    
    bib = Bib().set('245', 'a', 'This record will self-destruct')
    bib.commit()    
    bib.delete()
    
    assert Bib.match_id(bib.id) == None
    
    history = DB.handle['bib_history'].find_one({'_id': bib.id})
    assert history['deleted']['user'] == 'admin'
    assert isinstance(history['deleted']['time'], datetime)

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
        
def test_from_id(db, bibs, auths):
    from dlx.marc import Bib, Auth
    
    assert Bib.from_id(2).id == 2
    
def test_querydocument(db):
    from dlx.marc import Bib, Auth, QueryDocument, Condition, Or
    from bson import SON
    from json import loads
    import re
    
    query = QueryDocument(Condition(tag='245', subfields={'a': 'This'}))
    assert isinstance(query.compile(), SON)
    
    qjson = query.to_json()
    qdict = loads(qjson)
    assert qdict['245']['$elemMatch']['subfields']['$elemMatch']['code'] == 'a'
    assert qdict['245']['$elemMatch']['subfields']['$elemMatch']['value'] == 'This'
    
    query = QueryDocument(
        Condition(tag='245', subfields={'a': re.compile(r'(This|Another)'), 'b': 'is the', 'c': 'title'}),
        Condition(tag='650', modifier='exists'),
        Or(
            Condition(tag='710', modifier='exists'),
            Condition(tag='520', modifier='not_exists')
        )
    )
    assert len(list(Bib.find(query.compile()))) == 2
    
    query = QueryDocument(
        Condition(tag='110', subfields={'a': 'Another header'}),
    )
    assert len(list(Auth.find(query.compile()))) == 1
    assert Auth.find_one(query.compile()).id == 2
    
def test_querystring(db):
    from dlx.marc import Bib, Auth, QueryDocument
    
    query = QueryDocument.from_string('{"245": {"a": "/^(This|Another)/", "b": "is the", "c": "title"}}')
    assert len(list(Bib.find(query.compile()))) == 2
    
    query = QueryDocument.from_string('{"OR": {"650": 0, "710": 0}}')
    assert len(list(Bib.find(query.compile()))) == 1
    
    query = QueryDocument.from_string('{"110": {"a": "Another header"}}')
    assert Auth.find_one(query.compile()).id == 2

def test_from_query(db):
    from dlx.marc import Bib, Auth, Query, Condition
    
    bib = Bib.from_query(Query(Condition('245', {'a': 'Another'})))
    assert bib.id == 2
    
def test_get_field(bibs):
    from dlx.marc import Bib, Field, Controlfield, Datafield
    
    bib = Bib(bibs[0])
    assert isinstance(bib.get_field('000'), Controlfield)
    assert isinstance(bib.get_field('245'), Datafield)
    assert bib.get_field('245').tag == '245'
    
    fields = bib.get_fields('245', '520')
    
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
    
def test_get_xref(db, bibs):
    from dlx.marc import Bib
    
    bib = Bib(bibs[0])
    assert bib.get_xref('650', 'a') == 1
    bib.set('710', 'a', 2, address='+')
    assert bib.get_xrefs('710') == [2, 2]
    
def test_set():
    from dlx.marc import Bib, InvalidAuthXref, InvalidAuthValue
    
    bib = Bib()
    bib.set('245', 'a', 'Edited')
    assert bib.get_value('245', 'a') == 'Edited'
    
    bib.set('245', 'a', 'Repeated field', address=['+'])
    assert bib.get_values('245', 'a') == ['Edited', 'Repeated field']
    
    bib.set('245', 'a', 'Repeated field edited', address=[1])
    assert bib.get_value('245', 'a', address=[1, 0]) == 'Repeated field edited'
    
    bib.set('245', 'a', 'Repeated subfield', address=[1, '+'])
    assert bib.get_value('245', 'a', address=[1, 1]) == 'Repeated subfield'
    
    # int sets xref
    with pytest.raises(InvalidAuthXref):
        bib.set('650', 'a', 9)
        
    bib.set('650', 'a', 1)
    assert bib.get_xref('650', 'a') == 1
    
    # str is subject to auth control
    with pytest.raises(InvalidAuthValue):
        bib.set('650', 'a', 'invalid auth controlled value')
        
    bib.set('650', 'a', 'Header')
    
    bib = Bib().set_values(
        ('245', 'a', 'yet another'),
        ('245', 'b', 'title'),
        ('500', 'a', 'desc'),
        ('500', 'a', 'desc', {'address': ['+']}),
    )
    
    assert bib.get_values('245', 'a', 'b') == ['yet another', 'title']
    assert bib.get_values('500', 'a') == ['desc', 'desc']

def test_merge():
    from dlx.marc import Bib
    
    bib1 = Bib().set('000', None, 'leader').set('245', 'a', 'Title')
    bib2 = Bib().set('000', None, '|eade|').set('269', 'a', 'Date')
    bib1.merge(bib2)
    assert bib1.get_value('269', 'a') == 'Date'
    assert bib1.get_value('000') ==  'leader'
       
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
    
def test_delete_field(bibs):
    from dlx.marc import Bib
    
    bib = Bib.find_one({'_id': 1})
    bib.delete_field('008')
    assert list(bib.get_fields('008')) == []
    bib.delete_field('500')
    assert list(bib.get_fields('500')) == []
    
    bib.delete_field('520', place=1)
    assert len(list(bib.get_fields('520'))) == 1
    assert bib.get_values('520', 'a') == ['Description']
    
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
    assert bib.get_value('650', 'a', language='es') == 'Texto'
    
def test_to_xml(db):
    from dlx.marc import Bib
    from xmldiff import main
    
    control = '<record><controlfield tag="000">leader</controlfield><controlfield tag="008">controlfield</controlfield><datafield ind1=" " ind2=" " tag="245"><subfield code="a">This</subfield><subfield code="b">is the</subfield><subfield code="c">title</subfield></datafield><datafield ind1=" " ind2=" " tag="520"><subfield code="a">Description</subfield></datafield><datafield ind1=" " ind2=" " tag="520"><subfield code="a">Another description</subfield><subfield code="a">Repeated subfield</subfield></datafield><datafield ind1=" " ind2=" " tag="650"><subfield code="a">Header</subfield><subfield code="0">1</subfield></datafield><datafield ind1=" " ind2=" " tag="710"><subfield code="a">Another header</subfield><subfield code="0">2</subfield></datafield></record>'
    bib = Bib.find_one({'_id': 1})
    assert main.diff_texts(bib.to_xml(), control) == []
    
def test_xml_encoding():
    from dlx.marc import Bib
    from xmldiff import main
    
    control = '<record><datafield ind1=" " ind2=" " tag="245"><subfield code="a">Title with an é</subfield></datafield></record>'
    bib = Bib().set('245', 'a', 'Title with an é')
    assert main.diff_texts(bib.to_xml(), control) == []
    
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
    
    control = '=000  leader\n=008  controlfield\n=245  \\\\$aThis$bis the$ctitle\n=520  \\\\$aDescription\n=520  \\\\$aAnother description$aRepeated subfield\n=650  \\\\$aHeader\n=710  \\\\$aAnother header\n'

    bib = Bib.find_one({'_id': 1})
    assert bib.to_mrk() == control

def test_from_mrk(db):
    from dlx.marc import Bib
    
    mrk = '=000  leader\n=008  controlfield\n=245  \\\\$aThis$bis the$ctitle\n=520  \\\\$aDescription\n=520  \\\\$aAnother description$aRepeated subfield\n=650  \\\\$aHeader\n=710  \\\\$aAnother header\n'

    bib = Bib.from_mrk(mrk)
    assert bib.to_mrk() == mrk
    assert bib.commit()
    
def test_to_jmarcnx(bibs):
    from dlx.marc import Bib
    import json
    
    control = '{"_id": 1, "000": ["leader"], "008": ["controlfield"], "245": [{"indicators": [" ", " "], "subfields": [{"code": "a", "value": "This"}, {"code": "b", "value": "is the"}, {"code": "c", "value": "title"}]}], "520": [{"indicators": [" ", " "], "subfields": [{"code": "a", "value": "Description"}]}, {"indicators": [" ", " "], "subfields": [{"code": "a", "value": "Another description"}, {"code": "a", "value": "Repeated subfield"}]}], "650": [{"indicators": [" ", " "], "subfields": [{"code": "a", "value": "Header"}]}], "710": [{"indicators": [" ", " "], "subfields": [{"code": "a", "value": "Another header"}]}]}'
    
    jnx = Bib.from_id(1).to_jmarcnx()
    bib = Bib(json.loads(jnx))
    assert bib.to_dict() == json.loads(control)

def test_from_jmarcnx(bibs):
    from dlx.marc import Bib

    assert Bib.from_jmarcnx(Bib.from_id(1).to_jmarcnx()).to_dict() == Bib.from_id(1).to_dict()
    
def test_field_from_jmarcnx(bibs):
    from dlx.marc import Datafield
    
    field = Datafield.from_jmarcnx(
        record_type='bib',
        tag='500',
        data='{"indicators": [" ", " "], "subfields": [{"code": "a", "value": "val"}]}'
    )
    
    assert isinstance(field, Datafield)
    assert field.get_value('a') == 'val'
    
    field = Datafield.from_jmarcnx(
        record_type='bib',
        tag='610',
        data='{"indicators": [" ", " "], "subfields": [{"code": "a", "value": "Another header"}]}'
    )
    
    assert field.get_xref('a') == 2
    
    with pytest.raises(Exception):
        field = Datafield.from_jmarcnx(
            record_type='bib',
            tag='610',
            data='{"indicators": [" ", " "], "subfields": [{"code": "a", "value": "Another headerrrr"}]}'
        )
    