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

def test_init_auth_check(db):
    # auth check on init is off
    return 
    
    from dlx import DB
    from dlx.marc import Bib, Auth, InvalidAuthXref, InvalidAuthValue, AmbiguousAuthValue
    
    with pytest.raises(InvalidAuthXref):
        Bib({'650': [{'indicators': [' ', ' '], 'subfields': [{'code': 'a', 'xref': 9}]}]})
   
    with pytest.raises(InvalidAuthValue):
        Bib({'650': [{'indicators': [' ', ' '], 'subfields': [{'code': 'a', 'value': 'invalid'}]}]})
        
    with pytest.raises(AmbiguousAuthValue):
        for idx in (98, 99):
            DB.auths.insert_one({'_id': idx, '150': [{'indicators': [' ', ' '], 'subfields': [{'code': 'a', 'value': 'Ambiguous value'}]}]})
        
        Bib({'650': [{'indicators': [' ', ' '], 'subfields': [{'code': 'a', 'value': 'Ambiguous value'}]}]})
    
def test_commit(db, bibs, auths):
    from dlx import DB
    from dlx.marc import Bib, Auth
    from datetime import datetime
    from bson import ObjectId
    from jsonschema.exceptions import ValidationError

    for linked_bib in [Bib(x) for x in bibs]:
        assert linked_bib.commit() == linked_bib
        
    for auth in [Auth(x) for x in auths]:
        assert auth.commit() == auth
        
    linked_bib = Bib()
    assert linked_bib.commit() == linked_bib
    assert isinstance(linked_bib.updated, datetime)
    assert linked_bib.user == 'admin'
    assert linked_bib.history()[0].to_dict() == linked_bib.to_dict()
    assert linked_bib.history()[0].user == 'admin'
    # there are two bibs before this in the db from conftest
    assert Bib.max_id() == 3

    # new audit attributes
    assert linked_bib.created == linked_bib.updated
    assert linked_bib.created_user == 'admin'
    linked_bib.commit(user='different user')
    assert linked_bib.created != linked_bib.updated
    linked_bib.created = 'string'
    linked_bib.commit()
    assert isinstance(linked_bib.created, datetime) # can't change created
    assert linked_bib.created_user == 'admin'

    # new text attributes
    linked_bib.set('245', 'a', 'TESTING TESTING 1234')
    linked_bib.commit()
    assert linked_bib.text == 'testing testing 1234'
    assert linked_bib.words == ['test', 'test', '1234']

    # id incrementer
    Bib().commit()
    linked_bib = Bib().commit()
    assert linked_bib.id == Bib.max_id() == 5
    
    # don't reuse id
    linked_bib.delete()
    linked_bib = Bib().commit()
    assert linked_bib.id == 6
    
    # max id resets
    DB.bibs.drop()
    DB.handle['bib_history'].drop()
    DB.handle['bib_id_counter'].drop()
    Bib().commit()
    assert Bib.max_id() == 1
    
    # json schema validation
    with pytest.raises(ValidationError):
        linked_bib = Bib({'245': [{'indicators': [' ', ' '], 'subfields': [{'code': ' ', 'value': 'Subfield code is a space'}]}]})
        linked_bib.commit()

    # update attached records
    auth = Auth().set('150', 'a', 'to be updated').commit()
    bibs = [Bib().set('650', 'a', auth.id).commit() for i in range(10)]
    auths = [Auth().set('150', 'a', 'related').set('550', 'a', auth.id).commit() for i in range(10)]
    auth.set('150', 'a', 'updated')
    auth.commit(user='test auth update')

    assert len(auth.list_attached()) == 20
    
    for linked_bib in auth.list_attached(usage_type='bib'):
        assert linked_bib.get_value('650', 'a') == 'updated'
        assert linked_bib.user == 'test auth update'

        # log
        assert DB.handle['auth_linked_update_log'].find_one({'record_type': 'bib', 'record_id': linked_bib.id})['triggered_by'] == auth.id

    for linked_auth in auth.list_attached(usage_type='auth'):
        assert linked_auth.get_value('550', 'a') == 'updated'
        assert linked_auth.user == 'test auth update'

    # tag change
    auth.heading_field.tag = '100'
    auth.commit()

    for linked_bib in auth.list_attached(usage_type='bib'):
        assert linked_bib.get_field('600')
        assert linked_bib.get_field('650') is None

    # subfield deleted
    auth = Auth().set('100', 'a', 'will not be deleted').set('100', 'g', 'subfield to be deleted').commit()
    bib = Bib().set('600', 'a', auth.id).set('600', 'g', auth.id).commit()
    auth2 = Auth().set('100', 'a', 'another heading').set('100', 'g', 'another heading').commit()
    bib.set('600', 'a', auth2.id, address=['+']).set('600', 'g', auth2.id, address=[1]).commit()
    auth.heading_field.subfields = [x for x in auth.heading_field.subfields if x.code != 'g'] # remove subfield g
    auth.commit()
    bib = Bib.from_id(bib.id) # re-retrieve the updated data from the db that was updated in the background
    assert len(bib.get_fields('600')[0].subfields) == 1
    assert len(bib.get_fields('600')[1].subfields) == 2

    # subfield added
    auth.set('100', 'g', "I'm back").commit()
    bib = Bib.from_id(bib.id) # re-retrieve the updated data from the db that was updated in the backgroud
    assert len([x for x in bib.get_fields('600')[0].subfields]) == 2
    assert len([x for x in bib.get_fields('600')[1].subfields]) == 2

def test_delete(db):
    from copy import deepcopy
    from dlx import DB
    from dlx.marc import Bib
    from datetime import datetime
    
    bib = Bib().set('245', 'a', 'This record will self-destruct')
    bib.commit()    
    bib.delete()
    assert Bib.from_id(bib.id) == None
    
    history = DB.handle['bib_history'].find_one({'_id': bib.id})
    assert history['deleted']['user'] == 'admin'
    assert isinstance(history['deleted']['time'], datetime)

    # no history
    bib = Bib().set('245', 'a', 'This record will also self-destruct')
    bib.commit()
    bib.set('245', 'a', 'Updated') # in memory changes, not saved
    DB.handle['bib_history'].update_one({'_id': bib.id}, {'$unset': {'history': None}})
    bib.delete()
    history = DB.handle['bib_history'].find_one({'_id': bib.id})
    assert history['history'][0]['245'][0]['subfields'][0]['value'] == 'This record will also self-destruct'

def test_restore(db):
    from dlx.marc import Bib

    #Create and commit a new bib record
    bib = Bib().set('245', 'a', 'This record will be restored')
    bib.commit()
    bib_id = bib.id

    #Delete the bib record
    bib.delete()
    assert Bib.from_id(bib_id) is None

    #Restore the bib record
    restored = Bib.restore(bib_id)
    assert isinstance(restored, Bib)
    assert restored.id == bib_id
    assert restored.get_value('245', 'a') == 'This record will be restored'

    #Confirm the record is restored in the database
    bib2 = Bib.from_id(bib_id)
    assert bib2 is not None
    assert bib2.get_value('245', 'a') == 'This record will be restored'

def test_revert(db):
    from dlx.marc import Bib

    bib = Bib().set('245', 'a', 'Original title').commit()
    bib.set('245', 'a', 'Edited title').commit()
    assert isinstance(bib.revert(1), Bib)
    assert bib.get_value('245', 'a') == 'Original title'

@pytest.mark.skip(reason='deprecated')
def test_find_one(db, bibs, auths):
    from dlx.marc import Bib, Auth
    
    bib = Bib.from_query({'_id': 1})
    assert bib.id == 1
    assert isinstance(bib, Bib)
    
    auth = Auth.from_query({'_id': 1})
    assert auth.id == 1
    assert isinstance(auth, Auth)
        
@pytest.mark.skip(reason='deprecated')
def test_find(db):
    from dlx.marc import Bib, BibSet, Auth, AuthSet
        
    bibs = BibSet.from_query({})
    assert inspect.isgenerator(bibs)
    assert len(list(bibs)) == 2
    
    for bib in BibSet.from_query({}):
        assert isinstance(bib, Bib)
    
    auths = AuthSet.from_query({})
    assert inspect.isgenerator(auths)
    assert len(list(auths)) == 2
    
    for auth in AuthSet.from_query({}):
        assert isinstance(auth, Auth)
        
    auths = AuthSet.from_query({'_id': 1})
    assert len(list(auths)) == 1
        
    auths = AuthSet.from_query({}, limit=1)
    assert len(list(auths)) == 1
    
    auths = AuthSet.from_query({}, limit=0, skip=1, projection={})
    assert len(list(auths)) == 1
        
def test_from_id(db, bibs, auths):
    from dlx.marc import Bib, Auth
    
    assert Bib.from_id(2).id == 2
    
def test_querydocument(db):
    from dlx.marc import Bib, BibSet, Auth, AuthSet
    from dlx.marc.query import Query, Condition, Or, TagOnly
    from bson import SON
    from json import loads
    import re

    class BibCondition(Condition):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.record_type = 'bib'
    
    query = Query(Condition(tag='245', subfields={'a': 'This'}, record_type='bib'))
    assert isinstance(query.compile(), SON)
    
    qjson = query.to_json()
    qdict = loads(qjson)
    assert qdict['245.subfields']['$elemMatch']['code'] == 'a'
    assert qdict['245.subfields']['$elemMatch']['value'] == 'This'
    
    query = Query(
        Condition(tag='245', subfields={'a': re.compile(r'(This|Another)'), 'b': 'is the', 'c': 'title'}),
        BibCondition(tag='650', modifier='exists'),
        Or(
            BibCondition(tag='710', modifier='exists'),
            Condition(tag='520', modifier='not_exists')
        )
    )
    assert len(list(BibSet.from_query(query.compile()))) == 2
    
    query = Query(
        Condition(tag='110', subfields={'a': 'Another header'}, record_type='auth'),
    )

    assert len(list(AuthSet.from_query(query.compile()))) == 1
    assert Auth.from_query(query.compile()).id == 2

    query = Query(
        TagOnly('245', 'This', record_type='bib')
    )
    assert len(list(BibSet.from_query(query.compile()))) == 1

    query = Query(
        TagOnly('245', 'This', modifier='not', record_type='bib')
    )
    assert len(list(BibSet.from_query(query.compile()))) == 1

def test_from_query(db):
    from dlx.marc import Bib, Auth, Query, Condition
    
    bib = Bib.from_query(Query(Condition('245', {'a': 'Another'})))
    assert bib.id == 2

def test_querystring(db):
    from datetime import datetime, timezone
    from bson import Regex
    from dlx.marc import BibSet, Bib, Auth, Query

    query = Query.from_string('245__c:\'title\'')
    assert len(list(BibSet.from_query(query.compile()))) == 2
    
    query = Query.from_string('245__a:\'This\' AND 650__a:\'Header\'')
    assert len(list(BibSet.from_query(query.compile()))) == 1
    
    query = Query.from_string('245__a:\'This\' OR 245__a:\'Another\'')
    assert len(list(BibSet.from_query(query.compile()))) == 2
    
    # todo: how to handle mixed conjunctions
    #query = Query.from_string('245__a:This OR 245__a:Another AND 650:Header')
    #assert len(list(BibSet.from_query(query.compile()))) == 2
    
    # text indexes don't exist here until commit
    for _ in (1, 2):
        Bib.from_id(_).commit()
        Auth.from_id(_).commit()

    # regex    
    id = Auth().set('110', 'a', 'string').set('110', 'b', 'part').commit().id
    query = Query.from_string('110__a:/string$/', record_type='auth')
    auth = Auth.from_query(query)
    assert auth
    assert auth.id == id
    query = Query.from_string('110__b:/^part/', record_type='auth')
    auth = Auth.from_query(query)
    assert auth.id == id
    
    # regex authority controlled
    query = Query.from_string('650__a:/[Hh]eader/')
    assert len(list(BibSet.from_query(query.compile()))) == 2
    
    query = Query.from_string('650__a:/header/i')
    assert len(list(BibSet.from_query(query.compile()))) == 2

    # double quoted in subfield, field and logical field search
    query = Query.from_string('520__a:"another description"')
    assert BibSet.from_query(query).count == 1
    query = Query.from_string('520__a:"another descrip"') # has to match whole words
    assert BibSet.from_query(query).count == 0

    query = Query.from_string('520:"another description"')
    assert BibSet.from_query(query).count == 1
    query = Query.from_string('520:"another descrip"') # has to match whole words
    assert BibSet.from_query(query).count == 0

    query = Query.from_string('title:"is the"')
    assert BibSet.from_query(query).count == 2
    query = Query.from_string('title:"is t"') # has to match whole words
    assert BibSet.from_query(query).count == 0
    
    # all fields text
   
    #with pytest.raises(NotImplementedError):
    #    # $text operator not implemented in mongomock
    #    #assert len(list(BibSet.from_query(query.compile()))) == 2

    query = Query.from_string('header')
    assert len(list(BibSet.from_query(query.compile()))) == 2
    query = Query.from_string('Another header')
    assert query.compile() == {'words': {'$all': ['anoth', 'header']}}

    # hyphenated word inside double quoted text
    query = Query.from_string('"Another-header"')
    assert query.compile()['words'] == {'$all': ["anoth", "header"]}
    assert query.compile()['text'] == Regex('\\sanother header\\s')
    
    # hyphenated free text word
    query = Query.from_string('Another-header')
    assert query.compile()['words'] == {'$all': ["anoth", "header"]}

    # negation operator
    query = Query.from_string('Another -header')
    assert query.compile() == {'$and': [{'words': {'$all': ['anoth']}}, {'words': {'$nin': ['header']}}]}

    # starred words (truncation)
    query = Query.from_string('anoth*')
    assert query.compile() == {'text': Regex(' anoth.*')}
    query = Query.from_string('anoth* h*der')
    assert query.compile() == {'$and': [{'text': Regex(' anoth.*')}, {'text': Regex(' h.*der')}]}

    # tag no subfield
    query = Query.from_string('245:\'is the\'')
    results = list(BibSet.from_query(query.compile()))
    assert len(results) == 2
    
    query = Query.from_string('650:\'Header\'')
    results = list(BibSet.from_query(query.compile()))
    assert len(results) == 2

    query = Query.from_string('650:/eader/')
    results = list(BibSet.from_query(query.compile()))
    assert len(results) == 2
    
    # id
    query = Query.from_string('id:1')
    assert len(list(BibSet.from_query(query.compile()))) == 1

    # updated
    # currently 3 records have been updated in this test
    Bib().commit()
    query = Query.from_string('updated>1900-01-01')
    assert len(list(BibSet.from_query(query.compile()))) == 3

    query = Query.from_string(f'updated:{(datetime.now(timezone.utc)).strftime("%Y-%m-%d")}')
    assert len(list(BibSet.from_query(query.compile()))) == 3
    
    # xref
    auth = Auth().set('100', 'a', 'x').commit()
    bib = Bib().set('700', 'a', auth.id).commit()
    query = Query.from_string(f'xref:{auth.id}', record_type='bib')
    assert len(list(BibSet.from_query(query.compile()))) == 1
    
    # string with wildcard
    query = Query.from_string(f"245__c:*itl*", record_type='bib')
    assert len(list(BibSet.from_query(query.compile()))) == 2

    query = Query.from_string(f"245:titl*", record_type='bib')
    assert len(list(BibSet.from_query(query.compile()))) == 2

    query = Query.from_string(f"650:head*", record_type='bib')
    assert len(list(BibSet.from_query(query.compile()))) == 2

    query = Query.from_string(f"650__a:head*", record_type='bib')
    assert len(list(BibSet.from_query(query.compile()))) == 2
    
    # logical fields
    bib = Bib().set('246', 'a', 'This title:').set('246', 'b', 'is a title').commit()
    query = Query.from_string(f'title:\'This title: is a title\'', record_type='bib')
    assert len(list(BibSet.from_query(query.compile()))) == 1

    # logical field wildcard
    query = Query.from_string(f"title:*is*", record_type='bib')
    assert len(list(BibSet.from_query(query.compile()))) == 3

    query = Query.from_string(f"title:this*", record_type='bib')
    assert len(list(BibSet.from_query(query.compile()))) == 2

    # NOT
    for bib in BibSet.from_query({}):
        # delete all bibs for further tests
        bib.delete()

    bib = Bib().set('246', 'a', 'New title').commit()
    query = Query.from_string(f'NOT 246:\'New title\'', record_type='bib')
    assert len(list(BibSet.from_query(query.compile()))) == 0

    # NOT with text
    bib.set('246', 'a', 'Edited alt title', address='+').commit()
    query = Query.from_string(f'NOT 246:New title', record_type='bib')
    assert len(list(BibSet.from_query(query.compile()))) == 0

    # NOT xref
    query = Query.from_string(f'NOT xref:1', record_type='bib')
    assert BibSet.from_query(query).count == 1

    # multi field + text
    bib.set('500','a', 'notes').set('520', 'z', 'Some words in a field').commit()
    query = Query.from_string(f"246:'New title' AND 500:'notes' AND some words in a field", record_type='bib')
    assert len(list(BibSet.from_query(query.compile()))) == 1

    # text search order of terms
    query = Query.from_string(f"246:'New title' AND some words in a field AND 500:'notes'", record_type='bib')
    assert len(list(BibSet.from_query(query.compile()))) == 1

    # invalid query strings
    from dlx.marc.query import InvalidQueryString

    with pytest.raises(InvalidQueryString): Query.from_string('invalid_field:value')
    with pytest.raises(InvalidQueryString): Query.from_string('245:title NOT 500:notes')
    with pytest.raises(InvalidQueryString): Query.from_string('245:title "unclosed double quote')
    with pytest.raises(InvalidQueryString): Query.from_string('245:\'title unclosed \' exact match')
    with pytest.raises(InvalidQueryString): Query.from_string('245:/title uncl/osed regex')

def test_from_aggregation(db, bibs):
    from dlx.marc import BibSet, Query

    query = Query.from_string('245__c:\'title\'')
    bibs = BibSet.from_aggregation(
        [
            {'$match': query.compile()}
        ],
        collation={}
    )
    assert isinstance(bibs, BibSet)
    assert bibs.count == 2

def test_atlasquery(db, bibs):
    from dlx.marc.query import AtlasQuery

    ag = AtlasQuery.from_string('245:\'title\' AND notes')
    pipeline = ag.compile()
    assert isinstance(ag.compile(), list)
    assert pipeline[0].get('$search')
    assert pipeline[1].get('$match')

def test_get_field(db, bibs):
    from dlx.marc import Bib, Field, Controlfield, Datafield
    
    bib = Bib(bibs[0])
    assert isinstance(bib.get_field('000'), Controlfield)
    assert isinstance(bib.get_field('245'), Datafield)
    assert bib.get_field('245').tag == '245'
    
    fields = bib.get_fields('245', '520')
    
    for field in fields:    
        assert isinstance(field, Field)
        
    bib = Bib()
    for tag in ('400', '250', '500', '300', '200'):
        bib.set(tag, 'a', 'test')
        
    assert [field.tag for field in bib.get_fields()] == ['200', '250', '300', '400', '500']
    
def test_field_get_value(bibs):
    from dlx.marc import Bib
    
    bib = Bib(bibs[0])
    field = bib.get_field('245')
    assert field.get_value('a') == 'This'
    assert field.get_values('a', 'b') == ['This', 'is the']

def test_set_field(bibs):
    from dlx.marc import Bib
    
    bib = Bib(bibs[0])
    field = bib.get_field('245')
    field.set('a', 'Changed')
    assert field.get_value('a') == 'Changed'
    
    field.set('x', 'Extra subfield').set('x', 'Another extra', place='+')
    assert field.get_value('x', place=1) == 'Another extra'

def test_get_value(db, bibs):
    from dlx.marc import Bib
    
    bib = Bib(bibs[0])
    assert bib.get_value('000') == 'leader'
    assert bib.get_value('245', 'a') == 'This'
    assert bib.get_values('245', 'a', 'b') == ['This', 'is the']
    assert bib.get_value('520', 'a', address=[1, 1]) == 'Repeated subfield'
    assert bib.get_values('520', 'a', place=1) == ['Another description', 'Repeated subfield']
    assert bib.get_value('999', 'a') == ''
    assert bib.get_values('999', 'a') == []

    # issue 487
    bib.set('520', 'z', 'Extra subfield', address=['+'])
    assert bib.get_value('520', 'z') == 'Extra subfield'

def test_get_values(db, bibs):
    from dlx.marc import Bib

    bib = Bib(bibs[0])
    assert bib.get_values('520') == ['Description', 'Another description', 'Repeated subfield']

    # field level
    assert bib.get_field('520', place=1).get_values() == ['Another description', 'Repeated subfield']

def test_get_xref(db, bibs):
    from dlx.marc import Bib
    
    bib = Bib(bibs[0])
    assert bib.get_xref('650', 'a') == 1
    bib.set('710', 'a', 2, address='+')
    assert bib.get_xrefs('710') == [2, 2]
    
def test_set(db):
    from dlx.marc import Bib, Auth, InvalidAuthXref, InvalidAuthValue, AmbiguousAuthValue, InvalidAuthField
    
    bib = Bib()
    bib.set('245', 'a', 'Edited')
    assert bib.get_value('245', 'a') == 'Edited'
    
    bib.set('245', 'a', 'Repeated field', address=['+'])
    assert bib.get_values('245', 'a') == ['Edited', 'Repeated field']
    
    bib.set('245', 'a', 'Repeated field edited', address=[1])
    assert bib.get_value('245', 'a', address=[1, 0]) == 'Repeated field edited'
    
    bib.set('245', 'a', 'Repeated subfield', address=[1, '+'])
    assert bib.get_value('245', 'a', address=[1, 1]) == 'Repeated subfield'
    
    bib.set('245', 'x', 'Other field', address=['+']).set('245', 'x', 'Yet another', address=[2, 0])
    assert bib.get_value('245', 'x', address=[2, 0]) == 'Yet another'
    
    # indicators
    bib.set('520', 'a', 'Field with inds', ind1='0', ind2='9')
    field = bib.get_field('520')
    assert (field.ind1, field.ind2) == ('0', '9')
    
    # int sets xref
    with pytest.raises(InvalidAuthXref):
        bib.set('650', 'a', 9)
        
    bib.set('650', 'a', 1)
    assert bib.get_xref('650', 'a') == 1
    assert bib.get_value('650', 'a') == 'Header'
    
    # str is subject to auth control
    with pytest.raises(InvalidAuthValue):
        bib.set('650', 'a', 'Invalid auth controlled value')
        
    bib.set('650', 'a', 'Invalid auth controlled value', auth_control=False)
    assert bib.get_value('650', 'a') == 'Invalid auth controlled value'
    
    with pytest.raises(InvalidAuthField):
        bib.commit()
        
    bib.set('650', 'a', 'Header')
    assert bib.get_xref('650', 'a') == 1
    
    # ambiguous auth controlled value
    with pytest.raises(AmbiguousAuthValue):
        Auth().set('150', 'a', 'Header').commit()
        bib.set('650', 'a', 'Header')

    # multi values
    bib = Bib().set_values(
        ('245', 'a', 'yet another'),
        ('245', 'b', 'title'),
        ('500', 'a', 'desc'),
        ('500', 'a', 'desc', {'address': ['+']}),
    )
    
    assert bib.get_values('245', 'a', 'b') == ['yet another', 'title']
    assert bib.get_values('500', 'a') == ['desc', 'desc']

def test_zmerge():
    from dlx.marc import Bib
    
    bib1 = Bib().set('000', None, 'leader').set('245', 'a', 'Title')
    bib2 = Bib().set('000', None, '|eade|').set('269', 'a', 'Date')
    bib1.zmerge(bib2)
    assert bib1.get_value('269', 'a') == 'Date'
    assert bib1.get_value('000') ==  'leader'
    
def test_xmerge():
    from dlx.marc import Bib
    
    bib1 = Bib().set('000', None, 'leader').set('245', 'a', 'Title')
    bib2 = Bib().set('000', None, '|eade|').set('269', 'a', 'Date')
    bib1.zmerge(bib2)
    assert bib1.get_value('269', 'a') == 'Date'
    assert bib1.get_value('000') ==  'leader'
    
    bib2.set('269', 'a', 'New date')
    bib1.xmerge(bib2, overwrite=False)
    assert bib1.get_value('269', 'a') == 'Date'
    bib1.xmerge(bib2, overwrite=True)
    assert bib1.get_value('269', 'a') == 'New date'
       
def test_set_008(bibs):
    from dlx.marc import Bib
    from dlx.config import Config
    from datetime import datetime, timezone
    
    bib = Bib()
    date_tag, date_code = Config.date_field
    bib.set(date_tag, date_code, '19991231')
    bib.set_008()
    assert bib.get_value('008')[0:6] == datetime.now(timezone.utc).strftime('%y%m%d')
    assert bib.get_value('008')[7:11] == '1999'
    
def test_delete_field(bibs):
    import copy
    from dlx.marc import Bib
    
    bib = Bib.from_query({'_id': 1})
    original = copy.deepcopy(bib)

    bib.delete_field('008')
    assert len(bib.diff(original).b) == 1
    assert bib.diff(original).b[0].tag == '008'

    bib.delete_field('500')
    assert list(bib.get_fields('500')) == []
    
    bib.delete_field('520', place=1)
    assert len(list(bib.get_fields('520'))) == 1
    assert bib.get_values('520', 'a') == ['Description']

    # delete using field object
    field = bib.get_field('520')
    bib.delete_field(field)
    assert bib.get_fields('520') == []

    # multi fields
    bib.set('999', 'a', 'x').set('999', 'a', 'x', address='+')
    original = copy.deepcopy(bib)
    assert len(bib.get_fields('999')) == 2
    bib.delete_fields('999')
    assert len(bib.diff(original).b) == 2
    assert all([x.tag == '999' for x in bib.diff(original).b])
    
def test_auth_lookup(db):
    from dlx.marc import Bib, Auth
    
    Auth._cache = {}
    Auth._xcache = {}
    
    with pytest.raises(KeyError):
        Auth._cache[1]
    
    bib = Bib.from_query({'_id': 1})
    assert bib.get_xref('650', 'a') == 1
    assert bib.get_value('650', 'a') == 'Header'
    
    assert Auth._cache[1]['a'] == 'Header'
   
    auth = Auth.from_query({'_id': 1})
    auth.set('150', 'a', 'Changed').commit()
    assert bib.get_value('650', 'a') == 'Changed'
    
    Auth().set('150', 'a', 'New').commit()
    bib.set('650', 'a', 'New')
    assert Auth._xcache['New']['150']['a'] == [3]
    
    bib.set('650', 'a', 'New')

def test_xlookup(db):
    from dlx.marc import Bib, Auth, Literal

    assert Auth.xlookup_multi('650', [Literal('a', 'Header')], record_type='bib') == [1]

def test_auth_control(db):
    from dlx.marc import Bib, InvalidAuthValue
    
    with pytest.raises(Exception):
        Bib({'650': [{'indicators': [' ', ' '], 'subfields': [{'code': 'a', 'value': 'Invalid'}]}]}, auth_control=True)
    
    bib = Bib({'650': [{'indicators': [' ', ' '], 'subfields': [{'code': 'a', 'value': 'Header'}]}]}, auth_control=True)
    assert bib.get_xref('650', 'a') == 1
      
def test_language(db):
    from dlx.marc import Bib, Auth
    
    Auth({'_id': 3}).set('150', 'a', 'Text').set('994', 'a', 'Texto').commit()
    bib = Bib({'_id': 3}).set('650', 'a', 3)
    assert bib.get_value('650', 'a', language='es') == 'Texto'
    
def test_to_xml(db):
    from dlx.marc import Bib
    from xmldiff import main
    
    control = '<record><controlfield tag="000">leader</controlfield><controlfield tag="001">1</controlfield><controlfield tag="008">controlfield</controlfield><datafield ind1=" " ind2=" " tag="245"><subfield code="a">This</subfield><subfield code="b">is the</subfield><subfield code="c">title</subfield></datafield><datafield ind1=" " ind2=" " tag="520"><subfield code="a">Description</subfield></datafield><datafield ind1=" " ind2=" " tag="520"><subfield code="a">Another description</subfield><subfield code="a">Repeated subfield</subfield></datafield><datafield ind1=" " ind2=" " tag="650"><subfield code="a">Header</subfield><subfield code="0">1</subfield></datafield><datafield ind1=" " ind2=" " tag="710"><subfield code="a">Another header</subfield><subfield code="0">2</subfield></datafield></record>'
    bib = Bib.from_query({'_id': 1})
    assert main.diff_texts(bib.to_xml(), control) == []

    control = control.replace('<controlfield tag="001">1</controlfield>', '')
    assert main.diff_texts(bib.to_xml(write_id=False), control) == []
    
def test_xml_encoding():
    from dlx.marc import Bib
    from xmldiff import main
    
    bib = Bib().set('245', 'a', 'Title with an é')
    control = f'<record><datafield ind1=" " ind2=" " tag="245"><subfield code="a">Title with an é</subfield></datafield></record>'
    assert main.diff_texts(bib.to_xml(write_id=False), control) == []
    
def test_to_mrc(db):
    from dlx.marc import Bib, Auth
    
    control = '00238r|||a2200109|||4500001000200000008001300002245002400015520001600039520004300055650001100098710001900109\x1e1\x1econtrolfield\x1e  \x1faThis\x1fbis the\x1fctitle\x1e  \x1faDescription\x1e  \x1faAnother description\x1faRepeated subfield\x1e  \x1faHeader\x1e  \x1faAnother header\x1e\x1d'

    bib = Bib.from_query({'_id': 1})
    assert bib.to_mrc() == control
    
    control = '00049||||a2200037|||4500150001100000\x1e  \x1faHeader\x1e\x1d'
   
    auth = Auth.from_query({'_id': 1})
    assert auth.to_mrc(write_id=False) == control
    
    auth.set('994', 'a', 'Titulo').commit()
    assert bib.to_mrc(language='es', write_id=False) == '00224r|||a2200097|||4500008001300000245002400013520001600037520004300053650001100096710001900107\x1econtrolfield\x1e  \x1faThis\x1fbis the\x1fctitle\x1e  \x1faDescription\x1e  \x1faAnother description\x1faRepeated subfield\x1e  \x1faTitulo\x1e  \x1faAnother header\x1e\x1d'

def test_to_mrk(bibs):
    from dlx.marc import Bib
    
    control = '=000  leader\n=001  1\n=008  controlfield\n=245  \\\\$aThis$bis the$ctitle\n=520  \\\\$aDescription\n=520  \\\\$aAnother description$aRepeated subfield\n=650  \\\\$aHeader$01\n=710  \\\\$aAnother header$02\n'
    bib = Bib.from_query({'_id': 1})
    assert bib.to_mrk() == control

    control = '\n'.join([line for line in control.split("\n") if line[:4] != '=001'])
    assert bib.to_mrk(write_id=False) == control

    # add 000 if not exists
    bib = Bib().set('245', 'a', 'title')
    assert bib.to_mrk() == '=000  ****\n=245  \\\\$atitle\n'

def test_from_mrk(db):
    from dlx.marc import Bib, InvalidRecordString
    
    control = '=000  leader\n=001  1\n=008  controlfield\n=245  \\\\$aThis$bis the$ctitle\n=520  \\\\$aDescription\n=520  \\\\$aAnother description$aRepeated subfield\n=650  \\\\$aHeader$01\n=710  \\\\$aAnother header$02\n'

    bib = Bib.from_mrk(control, auth_control=True)
    assert bib.id == 1
    assert bib.to_mrk() == control
    assert bib.commit(auth_check=True)

    # invalid tag order
    with pytest.raises(InvalidRecordString):
        Bib.from_mrk('=001  99999\n=000  leader')

def test_from_csv(db, bibs):
    from dlx.marc import Bib

    bib = Bib.from_csv('1.245$a,1.269$a,1.650$a\nTitle,Date,Not validated')
    assert isinstance(bib, Bib)
    assert Bib.from_csv('1.245$a,1.269$a,1.650$a\nTitle,Date,Header', auth_control=True)

    with pytest.raises(Exception):
        Bib.from_csv('1.245$a,1.269$a,1.650$a\nTitle,Date,Invalid auth controlled value', auth_control=True)

    with pytest.raises(Exception):
        Bib.from_csv('Invalid header value,1.269$a,1.650$a\nTitle,Date,Header')

def test_from_json():
    from dlx.marc import Bib, InvalidAuthValue, InvalidAuthField
    
    json = '{"600": [{"indicators": [" ", " "], "subfields": [{"code": "a", "value": "Not auth controlled"}]}]}'
    bib = Bib.from_json(json)
    assert bib
    
    with pytest.raises(InvalidAuthValue):
        Bib.from_json(json, auth_control=True)
        
    with pytest.raises(InvalidAuthField):
        bib.commit()
    
def test_to_jmarcnx(bibs):
    from dlx.marc import Bib
    import json
  
    jnx = Bib.from_id(1).to_jmarcnx()
    bib = Bib(json.loads(jnx))
    assert bib.to_jmarcnx() == jnx
    
def test_field_from_json(db, bibs):
    from dlx.marc import Datafield

    field = Datafield.from_json(
        record_type='bib',
        tag='500',
        data='{"indicators": [" ", " "], "subfields": [{"code": "a", "value": "val"}]}'
    )
    
    assert isinstance(field, Datafield)
    assert field.get_value('a') == 'val'
    
    field = Datafield.from_json(
        record_type='bib',
        tag='610',
        data='{"indicators": [" ", " "], "subfields": [{"code": "a", "value": "Another header"}]}',
        auth_control=True
    )
    
    assert field.get_xref('a') == 2
    
    with pytest.raises(Exception):
        field = Datafield.from_json(
            record_type='bib',
            tag='610',
            data='{"indicators": [" ", " "], "subfields": [{"code": "a", "value": "Wrong header"}]}',
            auth_control=True
        )

def test_partial_lookup(db):
    from dlx.marc import Auth
    
    results = Auth.partial_lookup('650', 'a', 'Head', record_type='bib')
    assert results[0].heading_value('a') == 'Header'
    
    for i in range(0, 100):
        Auth().set('150', 'a', f'Header{i}').commit()
    
    assert len(Auth.partial_lookup('650', 'a', 'eader', record_type='bib')) == 25
    assert len(Auth._pcache['650']['a']['eader']) == 25
    
    Auth._pcache['650']['a']['eader'] = ['dummy']
    assert len(Auth.partial_lookup('650', 'a', 'eader', record_type='bib')) == 1
    
def test_diff(db):
    from dlx.marc import Bib, Field, Diff
    
    bib1, bib2 = Bib.from_id(1), Bib.from_id(2)
    diff = bib1.diff(bib2)
    
    assert isinstance(diff, Diff)
    assert diff.records == (bib1, bib2)
    assert len(diff.a) == 5
    assert len(diff.b) == 1
    assert len(diff.c) == 2
    assert len(diff.d) == 0

    bib1.set('999', 'a', 'abc', address='+')
    bib1.set('999', 'a', 'xyz', address='+')
    bib2.set('999', 'a', 'xyz', address='+')
    bib2.set('999', 'a', 'abc', address='+')
    
    assert len(Diff(bib1, bib2).d) == 2

    bib2.set('999', 'a', 'abc', address='+')
    assert len(Diff(bib1, bib2).e) == 1
    
    for field in diff.a + diff.b + diff.c + diff.d + diff.e:
        assert isinstance(field, Field)

    assert diff.different

    diff = Diff(Bib().set('500', 'a', 'ok'), Bib().set('500', 'a', 'ok'))
    assert diff.same
        
def test_blank_fields(db):
    from dlx.marc import Bib
    
    bib = Bib().set('245', 'a', 'Title').set('246', 'a', '')

def test_auth_in_use(db, bibs, auths):
    from dlx.marc import Auth

    auth = Auth.from_id(1)
    assert auth.in_use()
    
    auth = Auth()
    auth.set('100', 'a', 'Name')
    assert not auth.in_use()
    auth.commit()
    assert not auth.in_use()
    
def test_catch_delete_auth(db, bibs, auths):
    from dlx.marc import Bib, Auth, AuthInUse
    
    auth = Auth().set('100', 'a', 'x').commit()
    Bib().set('600', 'a', 'x').commit()
    
    with pytest.raises(AuthInUse):
        auth.delete()

    auth = Auth().set('100', 'a', 'y').commit()
    Auth().set('500', 'a', 'y').commit()
    
    with pytest.raises(AuthInUse):
        auth.delete()

def test_from_xml():
    from dlx.marc import Bib
    
    bib = Bib.from_xml('<record><controlfield tag="001">1</controlfield><datafield tag="245" ind1=" " ind2=" "><subfield code="a">Title</subfield><subfield code="a">Repeated</subfield></datafield></record>')        
    assert bib.get_value('001', None) == '1'
    assert bib.id == 1
    assert bib.get_value('245', 'a') == 'Title'
    assert bib.get_value('245', 'a', address=[0, 1]) == 'Repeated'

def test_auth_use_count(db, bibs, auths):
    from dlx.marc import Bib, Auth
    
    auth = Auth.from_id(1)
    assert auth.in_use(usage_type='bib') == 2

    Auth().set("550", "a", 1).commit()
    assert auth.in_use(usage_type='auth') == 1
    
    auth = Auth().set('100', 'a', 't').commit()
    Bib().set('700', 'a', 't').commit()
    
    assert auth.in_use() == 1

def test_auth_merge():
    from dlx import DB
    from dlx.marc import Bib, Auth

    auth1 = Auth().set('100', 'a', 'Will be losing')
    auth1.commit()
    auth2 = Auth().set('100', 'a', 'Will be gaining')
    auth2.commit()

    Bib().set('700', 'a', auth1.id).commit()
    Bib().set('700', 'a', auth1.id).commit()

    assert auth1.in_use() == 2

    auth2.merge(user='test', losing_record=auth1)

    assert auth1.in_use() == 0
    assert auth2.in_use() == 2

    # history
    assert DB.handle['auth_history'].find_one({'_id': auth1.id})['merged']['into'] == auth2.id

    # log
    assert DB.handle['merge_log'].find_one({'record_type': 'auth', 'action': 'losing'})
    assert DB.handle['merge_log'].find_one({'record_type': 'auth', 'action': 'deleted'})
    assert DB.handle['merge_log'].find_one({'record_type': 'auth', 'action': 'gaining'})
    assert DB.handle['merge_log'].find_one({'record_type': 'auth', 'action': 'merge complete'})

def test_logical_fields(db):
    from dlx import DB, Config
    from dlx.marc import Bib

    Config.bib_logical_fields.update({'test_field': {'867': ['a', 'z']}})
    bib = Bib().set('867', 'a', 'logical value 1').set('867', 'z', 'logical value 2').commit()
    assert DB.handle['bibs'].find_one({'_id': bib.id}).get('test_field') == ['logical value 1', 'logical value 2']

    Config.bib_logical_fields.update({'test_field_2': {'868': ['abc']}})
    bib.set('868', 'a', 'part 1,').set('868', 'b', 'part 2 +').set('868', 'c', 'part 3').commit()
    assert DB.handle['bibs'].find_one({'_id': bib.id}).get('test_field_2') == ['part 1, part 2 + part 3']

    # record type special field
    assert set(bib.logical_fields()['_record_type']) == set(['bib', 'default'])

    Config.bib_type_map.update({'my_test_type': ['900', 'a', 'test type']})
    bib.set('900', 'a', 'test type')
    assert set(bib.logical_fields()['_record_type']) == set(['my_test_type', 'bib'])

    # indexes
    bib2 = Bib()
    bib2.set('867', 'a', 'logical value 1')
    bib.commit()
    bib2.commit()
    assert DB.handle['_index_test_field'].find_one({'_id': 'logical value 1'})
    
    bib.delete()
    assert DB.handle['_index_test_field'].find_one({'_id': 'logical value 1'})
    bib2.delete()
    assert DB.handle['_index_test_field'].find_one({'_id': 'logical value 1'}) == None

def test_bib_files(db, bibs):
    from datetime import datetime
    from dlx import DB
    from dlx.file import File
    from dlx.marc import Bib

    DB.files.insert_one({'_id': '1', 'identifiers': [{'type': 'symbol', 'value': 'A/TEST'}], 'languages': ['FR'], 'uri': 'www.test.com/fr', 'mimetype': 'text/plain', 'size': 1, 'source': 'test', 'timestamp': datetime.now()})
    DB.files.insert_one({'_id': '2', 'identifiers': [{'type': 'symbol', 'value': 'A/TEST'}], 'languages': ['ES'], 'uri': 'www.test.com/es', 'mimetype': 'text/plain', 'size': 1, 'source': 'test', 'timestamp': datetime.now()})
    bib = Bib().set('191', 'a', 'A/TEST')
    assert bib.files() == ['www.test.com/fr', 'www.test.com/es']

def test_list_attached(db, bibs, auths):
    from dlx.marc import Bib, Auth

    auth = Auth.from_id(1)
    assert len(auth.list_attached()) == 2
    assert len(auth.list_attached(usage_type='bib')) == 2
    assert auth.list_attached()[0].id == 1
    assert auth.list_attached()[1].id == 2

def test_resolve_ambiguous(db):
    from dlx.marc import Bib, Auth, AmbiguousAuthValue, Literal

    auth = Auth().set('100', 'a', 'ambiguous').commit()
    Auth().set('100', 'a', 'ambiguous').set('100', 'b', 'xyz').commit()

    assert len(Auth.xlookup('700', 'a', 'ambiguous', record_type='bib')) == 2

    xref = Auth.resolve_ambiguous(
        tag='700', 
        subfields=[Literal(code='a', value='ambiguous')],
        record_type='bib'
    )

    assert xref == auth.id

def test_history(db):
    from datetime import datetime
    from dlx.marc import Bib, Auth, BibHistory, AuthHistory, Query

    bib = Bib().set('245', 'a', 'new record').commit()
    query = Query.from_string("245__a:'new record'")
    assert next(BibHistory.from_query(query), None).id == bib.id

    bib.delete()
    hist = next(BibHistory.find_deleted(query), None)
    assert hist == bib.id

    assert next(
        BibHistory.deleted_by_date(
            datetime.fromisoformat('1999-12-31'), 
            datetime.fromisoformat('2999-12-31') # set in the future because the delete is too fast here
        ), 
        None
    ) == bib.id

    # restore
    assert not Bib.from_id(bib.id)
    restored = BibHistory.restore(bib.id)
    assert isinstance(restored, Bib)
    assert Bib.from_id(bib.id)

def test_auth_deleted_subfield(db):
    from dlx.marc import Bib, Auth, Query

    # sequence of events formerly causing an error
    auth = Auth().set('110', 'a', 'will NOT be deleted').set('110', 'z', 'will be deleted').commit()
    bib = Bib().set('710', 'a', auth.id).set('710', 'z', auth.id).commit()
    auth.heading_field.delete_subfield('z')
    auth.commit()
    bib.set('999', 'a', 'updated').commit()

    q = Query.from_string('710:"will not be deleted"')
    assert q.to_json()

