import pytest, os, inspect

# Fixtures are loaded automatically py pytest from ./conftest.py
# Tests run in order.
# Remember to prefix test function names with "test_".
# Database state is maintained globally. Use the `db` fixture to reset the test database.

def test_mocked():
    from dlx import DB
    from mongomock import MongoClient as Mocked
    
    assert isinstance(DB.client, Mocked)
    
def test_init(bibs, auths):
    from dlx.marc import BibSet, Bib, AuthSet, Auth
    
    records = [Bib(x) for x in bibs]
    bibset = BibSet(records)
    assert isinstance(bibset, BibSet)
    assert len(bibset.records) == 2
    assert bibset.count == 2
    
    records = [Auth(x) for x in auths]
    authset = AuthSet(records)
    assert isinstance(authset, AuthSet)
    assert len(authset.records) == 2
    assert authset.count == 2
    
def test_iterate(db):
    from dlx.marc import Bib, BibSet, Auth, AuthSet
    
    for bib in BibSet.from_query({}):
        assert isinstance(bib, Bib)
        
    for auth in AuthSet.from_query({}):
        assert isinstance(auth, Auth)

def test_from_query(db):
    from dlx.marc import MarcSet, BibSet, AuthSet, QueryDocument, Condition
    
    bibset = BibSet.from_query({'_id': {'$in': [1, 2]}})
    assert isinstance(bibset, (MarcSet, BibSet))
    assert bibset.count == 2
    assert isinstance(bibset.records, map)
    bibset.cache()
    assert isinstance(bibset.records, list)
    
    bibset = BibSet.from_query({}, skip=0, limit=1)
    assert bibset.count == 1
    for bib in bibset:
        assert bib.id == 1
    assert bibset.count == 1
    
    conditions = [
        Condition(tag='150', subfields={'a': 'Header'}),
        Condition(tag='200', modifier='not_exists')
    ]
    authset = AuthSet.from_query(conditions)
    assert isinstance(authset, (MarcSet, AuthSet))
    assert authset.count == 1
    assert isinstance(authset.records, map)
    authset.cache()
    assert isinstance(authset.records, list)
    
    query = QueryDocument(
        Condition('245', modifier='exists')
        
    )    
    bibset = BibSet.from_query(query)
    assert isinstance(bibset, BibSet)
    assert bibset.count == 2
        
def test_from_table():
    from dlx.marc import BibSet
    from dlx.util import Table
    
    t = Table(
        [
            ['246a','1.246$b','1.269c','2.269c'],
            ['title','subtitle','1999-12-31','repeated'],
            ['title2','subtitle2','2000-01-01','repeated'],
        ]
    )
    
    bibset = BibSet.from_table(t)
    for bib in bibset.records:
        assert bib.get_value('246','b')[:8] == 'subtitle'
        assert bib.get_values('269','c')[1] == 'repeated'
        
def test_from_excel():
    from dlx.marc import BibSet
    
    path = os.path.join(os.path.dirname(__file__), 'marc.xlsx')        
    bibset = BibSet.from_excel(path, date_format='%Y-%m-%d')
        
    for bib in bibset.records:
        assert bib.get_value('246','b')[:8] == 'subtitle'
        assert bib.get_values('269','c')[1] == 'repeated'
