import pytest, os, inspect

from dlx.marc import Bib

# Fixtures are loaded automatically py pytest from ./conftest.py
# Tests run in order.
# Remember to prefix test function names with "test_".
# Database state is maintained globally. Use the `db` fixture to reset the test database.

def test_mocked(db):
    from dlx import DB
    from mongomock import MongoClient as Mocked
    
    assert isinstance(DB.client, Mocked)
    
def test_init(bibs, auths):
    from dlx.marc import BibSet, Bib, AuthSet, Auth
    
    records = [Bib(x) for x in bibs]
    bibset = BibSet()
    bibset.records = records
    assert isinstance(bibset, BibSet)
    assert len(bibset.records) == 2
    assert bibset.count == 2
    
    records = [Auth(x) for x in auths]
    authset = AuthSet()
    authset.records = records
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
    from dlx.marc import MarcSet, BibSet, AuthSet, Query, Condition
    
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
    
    query = Query(
        Condition('245', modifier='exists')
        
    )    
    bibset = BibSet.from_query(query)
    assert isinstance(bibset, BibSet)
    assert bibset.count == 2
    
def test_from_ids(db):
    from dlx.marc import BibSet
    
    bibs = BibSet.from_ids([1, 2])
    assert [x.id for x in bibs] == [1, 2]

def test_sort_table_header():
    from dlx.marc import MarcSet

    header = ['1.269$a', '1.246$b', '1.246$a', '1.650$a', '1.650$0']

    assert MarcSet.sort_table_header(header) == ['1.246$a',  '1.246$b',  '1.269$a', '1.650$0', '1.650$a']

def test_from_table(db):
    from dlx.marc import BibSet, Auth, InvalidAuthXref
    from dlx.util import Table
    
    table = Table([
        ['1.001', '1.246$a',  '1.246$b',  '1.269$c', '2.269$c', '1.650$a', '1.650$0', ''],
        ['98', 'title', 'subtitle', '1999-12-31','repeated', 'x', 1, '', ''],
        ['99', 'title2','subtitle2', '2000-01-01','repeated', 'x', 1],
        ['', 'title2', 'subtitle2', '', '', 'x', 1]
    ])
    
    bibset = BibSet.from_table(table)
    assert bibset.records[0].id == 98
    assert bibset.records[1].id == 99

    for bib in bibset.records[:2]:
        assert bib.get_value('246','b')[:8] == 'subtitle'
        assert bib.get_values('269','c')[1] == 'repeated'
        assert bib.get_value('650', 'a') == 'Header'
        assert not bib.get_value('650', '0')

    assert bibset.records[2].get_field('269') is None

    bibset = BibSet.from_table(table, delete_subfield_zero=False)
    for bib in bibset.records:
        assert bib.get_value('650', '0')
                                 
    with pytest.raises(Exception):
        # dupe field check
        bibset = BibSet.from_table(Table([['245a'], ['This']]), field_check='245a')

    with pytest.raises(Exception):
        # auth control
        bibset = BibSet.from_table(Table([['650a'], ['Invalid']]), auth_control=True)

    # issue #533 - tables with ten or more instances of a tag
    table = Table([
        [f'{i}.246$a' for i in range (1, 11)], # header
        ['alt title' for i in range (1, 11)]   # data
    ])

    assert BibSet.from_table(table)

    # data outside column headers
    with pytest.raises(Exception):
        table = Table([
            '100$a|100$b|||'.split('|'),
            'cell1|cell2||extra|'.split('|'),
        ])

    # blank column header at beginning
    with pytest.raises(Exception):
        table = Table([
            '|100$b|||'.split('|'),
            'cell1|cell2||extra|'.split('|'),
        ])

    # invalid xref  
    table = Table([
        ['1.650$a', '1.650$0'],
        ['x', 3]
    ])

    with pytest.raises(Exception):
        BibSet.from_table(table, auth_control=True)

    # ambiguous auth controlled value
    Auth().set('100', 'a', 'ambiguous').set('100', 'g', 'unique 1').commit()
    Auth().set('100', 'a', 'ambiguous').set('100', 'g', 'unique 2').commit()

    table = Table([
        ['1.700$a'],
        ['ambiguous', ]
    ])

    with pytest.raises(Exception):
        BibSet.from_table(table, auth_control=True)

    
    table = Table([
        ['1.700$a', '1.700$g'],
        ['ambiguous', 'unique 1']
    ])

    bibs = BibSet.from_table(table, auth_control=True)
    assert bibs
    assert bibs.records[0].get_field('700').subfields[0].xref

    # indicators
    table = Table([
        ['1.245$a', '1.245__', '1.269__', '1.269$a'],
        ['title', '98', '34', 'date'],
        ['title', '98', '', '']
    ])

    bibs = BibSet.from_table(table)
    assert bibs.records[0].get_field('245').indicators == ['9', '8']
    assert bibs.records[0].get_field('269').indicators == ['3', '4']

@pytest.mark.skip(reason='xlrd is obsolete. needs review')
def test_from_excel():
    from dlx.marc import BibSet
    
    path = os.path.join(os.path.dirname(__file__), 'marc.xlsx')        
    bibset = BibSet.from_excel(path, date_format='%Y-%m-%d')
        
    for bib in bibset.records:
        assert bib.get_value('246','b')[:8] == 'subtitle'
        assert bib.get_values('269','c')[1] == 'repeated'

def test_from_mrk(db):
    from dlx.marc import BibSet

    control = '=000  leader\n=008  controlfield\n=245  \\\\$aThis$bis the$ctitle\n=520  \\\\$aDescription\n=520  \\\\$aAnother description$aRepeated subfield\n=650  \\\\$aWill be replaced because of xref$01\n=710  \\\\$aAnother header\n\n=000  leader\n=245  \\\\$aAnother$bis the$ctitle\n=650  \\\\$aHeader\n'
    bibs = BibSet.from_mrk(control)
    assert(len(bibs.records)) == 2
    assert bibs.records[0].get_value('650', 'a') == 'Header'

def test_from_xml(db):
    from dlx.marc import BibSet
    
    string = '''
        <collection>
            <record>
                <controlfield tag="000">leader</controlfield>
                <controlfield tag="008">controlfield</controlfield>
                <datafield ind1=" " ind2=" " tag="245">
                    <subfield code="a">This</subfield>
                    <subfield code="b">is the</subfield>
                    <subfield code="c">title</subfield>
                </datafield><datafield ind1=" " ind2=" " tag="520">
                    <subfield code="a">Description</subfield>
                </datafield>
                <datafield ind1=" " ind2=" " tag="520">
                    <subfield code="a">Another description</subfield>
                    <subfield code="a">Repeated subfield</subfield>
                </datafield>
                <datafield ind1=" " ind2=" " tag="650">
                    <subfield code="a">Header</subfield>
                    <subfield code="0">1</subfield>
                </datafield>
                <datafield ind1=" " ind2=" " tag="710">
                    <subfield code="a">Another header</subfield>
                    <subfield code="0">2</subfield>
                </datafield>
            </record>
            <record>
                <controlfield tag="000">leader</controlfield>
                <datafield ind1=" " ind2=" " tag="245">
                    <subfield code="a">Another</subfield>
                    <subfield code="b">is the</subfield>
                    <subfield code="c">title</subfield>
                </datafield>
                <datafield ind1=" " ind2=" " tag="650">
                    <subfield code="a">head</subfield>
                    <subfield code="0">1</subfield>
                </datafield>
            </record>
        </collection>
    '''

    bibset = BibSet.from_xml(string, auth_control=True)
    assert len(bibset.records) == 2
    assert bibset.records[1].get_value('650', 'a') == 'Header'
        
def test_to_mrc(db):
    from dlx.marc import BibSet
    
    control = '00238r|||a2200109|||4500001000200000008001300002245002400015520001600039520004300055650001100098710001900109\x1e1\x1econtrolfield\x1e  \x1faThis\x1fbis the\x1fctitle\x1e  \x1faDescription\x1e  \x1faAnother description\x1faRepeated subfield\x1e  \x1faHeader\x1e  \x1faAnother header\x1e\x1d00102r|||a2200061|||4500001000200000245002700002650001100029\x1e2\x1e  \x1faAnother\x1fbis the\x1fctitle\x1e  \x1faHeader\x1e\x1d'
    assert BibSet.from_query({}).to_mrc() == control
    
def test_to_mrk(db):
    from dlx.marc import BibSet
    
    control = '=000  leader\n=001  1\n=008  controlfield\n=245  \\\\$aThis$bis the$ctitle\n=520  \\\\$aDescription\n=520  \\\\$aAnother description$aRepeated subfield\n=650  \\\\$aHeader$01\n=710  \\\\$aAnother header$02\n\n=000  leader\n=001  2\n=245  \\\\$aAnother$bis the$ctitle\n=650  \\\\$aHeader$01\n'
    assert BibSet.from_query({}).to_mrk() == control
    
def test_to_xml(db):
    from dlx.marc import BibSet
    from xmldiff import main
    
    control = '<collection><record><controlfield tag="000">leader</controlfield><controlfield tag="001">1</controlfield><controlfield tag="008">controlfield</controlfield><datafield ind1=" " ind2=" " tag="245"><subfield code="a">This</subfield><subfield code="b">is the</subfield><subfield code="c">title</subfield></datafield><datafield ind1=" " ind2=" " tag="520"><subfield code="a">Description</subfield></datafield><datafield ind1=" " ind2=" " tag="520"><subfield code="a">Another description</subfield><subfield code="a">Repeated subfield</subfield></datafield><datafield ind1=" " ind2=" " tag="650"><subfield code="a">Header</subfield><subfield code="0">1</subfield></datafield><datafield ind1=" " ind2=" " tag="710"><subfield code="a">Another header</subfield><subfield code="0">2</subfield></datafield></record><record><controlfield tag="000">leader</controlfield><controlfield tag="001">2</controlfield><datafield ind1=" " ind2=" " tag="245"><subfield code="a">Another</subfield><subfield code="b">is the</subfield><subfield code="c">title</subfield></datafield><datafield ind1=" " ind2=" " tag="650"><subfield code="a">Header</subfield><subfield code="0">1</subfield></datafield></record></collection>'
    assert main.diff_texts(BibSet.from_query({}).to_xml(), control) == []
    
def test_xml_encoding():
    from dlx.marc import BibSet, Bib
    from xmldiff import main
    
    bib = Bib().set('245', 'a', 'Title with an é')
    control = f'<collection><record><datafield ind1=" " ind2=" " tag="245"><subfield code="a">Title with an é</subfield></datafield></record></collection>'
    bibset = BibSet()
    bibset.records = [bib]
    assert main.diff_texts(bibset.to_xml(write_id=False), control) == []
     
def test_to_str(db):
    from dlx.marc import BibSet
    
    control = '000\n   leader\n008\n   controlfield\n245\n   a: This\n   b: is the\n   c: title\n520\n   a: Description\n520\n   a: Another description\n   a: Repeated subfield\n650\n   a: Header\n710\n   a: Another header\n\n000\n   leader\n245\n   a: Another\n   b: is the\n   c: title\n650\n   a: Header\n'
    assert BibSet.from_query({}).to_str() == control

def test_to_csv(db):
    from dlx.marc import BibSet
    from dlx.marc import Bib

    bibset = BibSet.from_query({})
    bibset.records = list(bibset.records)
    control = '1.001,1.245__,1.245$a,1.245$b,1.245$c,1.520__,1.520$a,2.520__,2.520$a,1.650$0,1.650__,1.650$a,1.710$0,1.710__,1.710$a\n1,__,This,is the,title,__,Description,__,Another description||Repeated subfield,1,__,Header,2,__,Another header\n2,__,Another,is the,title,,,,,1,__,Header,,,'
    assert bibset.to_csv(write_id=True) == control
    
    # comma and quote handling
    bibs = BibSet()
    bibs.records += [Bib().set('245', 'a', 'A title, with a comma').set('245', 'b', 'subtitle'), Bib().set('245', 'a', 'A "title, or name" with double quotes in the middle').set('245', 'b', 'subtitle')]
    assert bibs.to_csv(write_id=False) == '1.245__,1.245$a,1.245$b\n__,"A title, with a comma",subtitle\n__,"A ""title, or name"" with double quotes in the middle",subtitle'

    # bug issue 507: fields with more 10+ instances
    from dlx.marc import Bib
    bib = Bib()
    [bib.set('999', 'a', str(i), address=['+']) for i in range(0, 11)]
    bibs = BibSet()
    bibs.records.append(bib)
    assert bibs.to_csv(write_id=False) == '1.999__,1.999$a,2.999__,2.999$a,3.999__,3.999$a,4.999__,4.999$a,5.999__,5.999$a,6.999__,6.999$a,7.999__,7.999$a,8.999__,8.999$a,9.999__,9.999$a,10.999__,10.999$a,11.999__,11.999$a\n__,0,__,1,__,2,__,3,__,4,__,5,__,6,__,7,__,8,__,9,__,10'

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
