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
    assert len(list(bibset.records)) == 0
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
    
def test_from_ids(db):
    from dlx.marc import BibSet
    
    bibs = BibSet.from_ids([1, 2])
    assert [x.id for x in bibs] == [1, 2]
        
def test_from_table(db):
    from dlx.marc import BibSet
    from dlx.util import Table
    
    t = Table([
        ['246a',  '1.246$b',  '1.269c',    '2.269c'],
        ['title', 'subtitle', '1999-12-31','repeated'],
        ['title2','subtitle2','2000-01-01','repeated'],
    ])
    
    bibset = BibSet.from_table(t)
    for bib in bibset.records:
        assert bib.get_value('246','b')[:8] == 'subtitle'
        assert bib.get_values('269','c')[1] == 'repeated'
        
    with pytest.raises(Exception):
        bibset = BibSet.from_table(Table([['245a'], ['This']]), field_check='245a')
        
    with pytest.raises(Exception):
        bibset = BibSet.from_table(Table([['650a'], ['Should an int']]), auth_control=True)
        
    with pytest.raises(Exception):
        bibset = BibSet.from_table(Table([['650a'], ['Invalid']]), auth_control=False, auth_flag=True)
        
def test_from_excel():
    from dlx.marc import BibSet
    
    path = os.path.join(os.path.dirname(__file__), 'marc.xlsx')        
    bibset = BibSet.from_excel(path, date_format='%Y-%m-%d')
        
    for bib in bibset.records:
        assert bib.get_value('246','b')[:8] == 'subtitle'
        assert bib.get_values('269','c')[1] == 'repeated'
        
def test_to_mrc(db):
    from dlx.marc import BibSet
    
    control = '00224r|||a2200097|||4500008001300000245002400013520001600037520004300053650001100096710001900107controlfield  aThisbis thectitle  aDescription  aAnother descriptionaRepeated subfield  aHeader  aAnother header00088r|||a2200049|||4500245002700000650001100027  aAnotherbis thectitle  aHeader'
    assert BibSet.from_query({}).to_mrc() == control
    
def test_to_mrk(db):
    from dlx.marc import BibSet
    
    control = '=000  leader\n=008  controlfield\n=245  \\\\$aThis$bis the$ctitle\n=520  \\\\$aDescription\n=520  \\\\$aAnother description$aRepeated subfield\n=650  \\\\$aHeader\n=710  \\\\$aAnother header\n\n=000  leader\n=245  \\\\$aAnother$bis the$ctitle\n=650  \\\\$aHeader\n'
    assert BibSet.from_query({}).to_mrk() == control
    
def test_to_xml(db):
    from dlx.marc import BibSet
    from xmldiff import main
    
    control = '<collection><record><controlfield tag="000">leader</controlfield><controlfield tag="008">controlfield</controlfield><datafield ind1=" " ind2=" " tag="245"><subfield code="a">This</subfield><subfield code="b">is the</subfield><subfield code="c">title</subfield></datafield><datafield ind1=" " ind2=" " tag="520"><subfield code="a">Description</subfield></datafield><datafield ind1=" " ind2=" " tag="520"><subfield code="a">Another description</subfield><subfield code="a">Repeated subfield</subfield></datafield><datafield ind1=" " ind2=" " tag="650"><subfield code="a">Header</subfield><subfield code="0">1</subfield></datafield><datafield ind1=" " ind2=" " tag="710"><subfield code="a">Another header</subfield><subfield code="0">2</subfield></datafield></record><record><controlfield tag="000">leader</controlfield><datafield ind1=" " ind2=" " tag="245"><subfield code="a">Another</subfield><subfield code="b">is the</subfield><subfield code="c">title</subfield></datafield><datafield ind1=" " ind2=" " tag="650"><subfield code="a">Header</subfield><subfield code="0">1</subfield></datafield></record></collection>'
    assert main.diff_texts(BibSet.from_query({}).to_xml(), control) == []
    
def test_xml_encoding():
    from dlx.marc import BibSet, Bib
    from xmldiff import main
    
    control = '<collection><record><datafield ind1=" " ind2=" " tag="245"><subfield code="a">Title with an é</subfield></datafield></record></collection>'
    bibset = BibSet([Bib().set('245', 'a', 'Title with an é')])
    assert main.diff_texts(bibset.to_xml(), control) == []
     
def test_to_str(db):
    from dlx.marc import BibSet
    
    control = '000\n   leader\n008\n   controlfield\n245\n   a: This\n   b: is the\n   c: title\n520\n   a: Description\n520\n   a: Another description\n   a: Repeated subfield\n650\n   a: Header\n710\n   a: Another header\n\n000\n   leader\n245\n   a: Another\n   b: is the\n   c: title\n650\n   a: Header\n'

    assert BibSet.from_query({}).to_str() == control
    
    