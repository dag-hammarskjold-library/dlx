import sys, os, pytest

def test_init_db(db):
    from dlx import DB
    from scripts import init_dlx
    
    sys.argv[1:] = ['--connect=mongomock://localhost']
    
    init_dlx.main()
    
    assert list(DB.bibs.list_indexes()) == [
        {'key': {'_id': 1},
         'name': '_id_',
         'ns': 'dummy.bibs',
         'v': 2},
        {'key': {'191': 1},
         'name': '191_1',
         'ns': 'dummy.bibs',
         'v': 2},
        {'key': {'191.subfields.code': 1},
         'name': '191.subfields.code_1',
         'ns': 'dummy.bibs',
         'v': 2},
        {'key': {'191.subfields.value': 1},
         'name': '191.subfields.value_1',
         'ns': 'dummy.bibs',
         'v': 2},
        {'key': {'191.subfields.xref': 1},
         'name': '191.subfields.xref_1',
         'ns': 'dummy.bibs',
         'v': 2},
        {'key': {'269': 1},
         'name': '269_1',
         'ns': 'dummy.bibs',
         'v': 2},
        {'key': {'269.subfields.code': 1},
         'name': '269.subfields.code_1',
         'ns': 'dummy.bibs',
         'v': 2},
        {'key': {'269.subfields.value': 1},
         'name': '269.subfields.value_1',
         'ns': 'dummy.bibs',
         'v': 2},
        {'key': {'791': 1},
         'name': '791_1',
         'ns': 'dummy.bibs',
         'v': 2},
        {'key': {'791.subfields.code': 1},
         'name': '791.subfields.code_1',
         'ns': 'dummy.bibs',
         'v': 2},
        {'key': {'791.subfields.value': 1},
         'name': '791.subfields.value_1',
         'ns': 'dummy.bibs',
         'v': 2},
        {'key': {'791.subfields.xref': 1},
         'name': '791.subfields.xref_1',
         'ns': 'dummy.bibs',
         'v': 2},
        {'key': {'930': 1},
         'name': '930_1',
         'ns': 'dummy.bibs',
         'v': 2},
        {'key': {'930.subfields.code': 1},
         'name': '930.subfields.code_1',
         'ns': 'dummy.bibs',
         'v': 2},
        {'key': {'930.subfields.value': 1},
         'name': '930.subfields.value_1',
         'ns': 'dummy.bibs',
         'v': 2},
        {'key': {'998': 1},
         'name': '998_1',
         'ns': 'dummy.bibs',
         'v': 2},
        {'key': {'998.subfields.code': 1},
         'name': '998.subfields.code_1',
         'ns': 'dummy.bibs',
         'v': 2},
        {'key': {'998.subfields.value': 1},
         'name': '998.subfields.value_1',
         'ns': 'dummy.bibs',
         'v': 2}
    ]
    
    assert list(DB.auths.list_indexes()) == [
        {'key': {'_id': 1},
         'name': '_id_',
         'ns': 'dummy.auths',
         'v': 2},
        {'key': {'100': 1},
         'name': '100_1',
         'ns': 'dummy.auths',
         'v': 2},
        {'key': {'100.subfields.code': 1},
         'name': '100.subfields.code_1',
         'ns': 'dummy.auths',
         'v': 2},
        {'key': {'100.subfields.value': 1},
         'name': '100.subfields.value_1',
         'ns': 'dummy.auths',
         'v': 2},
        {'key': {'110': 1},
         'name': '110_1',
         'ns': 'dummy.auths',
         'v': 2},
        {'key': {'110.subfields.code': 1},
         'name': '110.subfields.code_1',
         'ns': 'dummy.auths',
         'v': 2},
        {'key': {'110.subfields.value': 1},
         'name': '110.subfields.value_1',
         'ns': 'dummy.auths',
         'v': 2},
        {'key': {'111': 1},
         'name': '111_1',
         'ns': 'dummy.auths',
         'v': 2},
        {'key': {'111.subfields.code': 1},
         'name': '111.subfields.code_1',
         'ns': 'dummy.auths',
         'v': 2},
        {'key': {'111.subfields.value': 1},
         'name': '111.subfields.value_1',
         'ns': 'dummy.auths',
         'v': 2},
        {'key': {'150': 1},
         'name': '150_1',
         'ns': 'dummy.auths',
         'v': 2},
        {'key': {'150.subfields.code': 1},
         'name': '150.subfields.code_1',
         'ns': 'dummy.auths',
         'v': 2},
        {'key': {'150.subfields.value': 1},
         'name': '150.subfields.value_1',
         'ns': 'dummy.auths',
         'v': 2},
        {'key': {'190': 1},
         'name': '190_1',
         'ns': 'dummy.auths',
         'v': 2},
        {'key': {'190.subfields.code': 1},
         'name': '190.subfields.code_1',
         'ns': 'dummy.auths',
         'v': 2},
        {'key': {'190.subfields.value': 1},
         'name': '190.subfields.value_1',
         'ns': 'dummy.auths',
         'v': 2}
    ]
    
def test_excel_marc():
    from scripts import excel_marc
    
    excel = os.path.dirname(__file__) + '/marc.xlsx'
    out = os.path.dirname(__file__) + '/out.mrc'
    defaults = os.path.dirname(__file__) + '/defaults.xlsx'
    sys.argv[1:] = ['--connect=mongomock://localhost', '--file={}'.format(excel), '--type=bib', '--format=mrc', '--out={}'.format(out), '--defaults={}'.format(defaults), '--check=245a']
    
    excel_marc.main()
    assert os.path.exists(out)
    
    os.remove(os.path.dirname(__file__) + '/out.mrc')
