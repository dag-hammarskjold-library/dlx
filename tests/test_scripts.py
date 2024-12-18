import sys, os, pytest

def test_init_db(db):
    from dlx import DB, Config
    
    sys.argv[1:] = ['--connect=mongomock://localhost']
    
    from dlx.scripts import clear_incrementers
    
    assert clear_incrementers.run() is None # runs the function, no return value
    
    from dlx.scripts import init_indexes
    
    assert init_indexes.run() is None # runs the function, no return value

@pytest.mark.skip(reason='xlrd is obsolete. needs review')
def test_excel_marc():
    from dlx.scripts import excel_marc
    
    excel = os.path.dirname(__file__) + '/marc.xlsx'
    out = os.path.dirname(__file__) + '/out.mrc'
    defaults = os.path.dirname(__file__) + '/defaults.xlsx'
    sys.argv[1:] = ['--connect=mongomock://localhost', '--file={}'.format(excel), '--type=bib', '--format=mrc', '--out={}'.format(out), '--defaults={}'.format(defaults), '--check=245a']
    
    excel_marc.run()
    assert os.path.exists(out)
    
    os.remove(os.path.dirname(__file__) + '/out.mrc')
    
def test_build_logical_fields(db):
    from dlx.marc import Bib
    from dlx.scripts import build_logical_fields
    
    sys.argv[1:] = ['--connect=mongomock://localhost', '--type=bib']
    
    bib = Bib().set('245', 'a', 'Title:') \
        .set('245', 'b', 'subtitle') \
        .set('246', 'a', 'Alt title') \
        .set('650', 'a', 1) \
        .commit()

    # delete the logical field that was created on commit to test that the script adds it
    bib.handle().update_one({'_id': bib.id}, {'$unset': {'title': 1}})
    assert bib.handle().find_one({'_id': bib.id}).get('title') is None
    assert build_logical_fields.run() == True
    assert bib.handle().find_one({'_id': bib.id}).get('title') == ['Title: subtitle', 'Alt title']

    # test fields arg
    bib.handle().update_one({'_id': bib.id}, {'$unset': {'title': 1}})
    sys.argv[1:] = ['--connect=mongomock://localhost', '--type=bib', '--fields=dummy1 dummy2']
    assert build_logical_fields.run() == True
    assert bib.handle().find_one({'_id': bib.id}).get('title') is None

def test_build_text_collections(db):
    from dlx.marc import Bib
    from dlx.scripts import build_text_collections
    
    sys.argv[1:] = ['--connect=mongomock://localhost', '--type=bib']

    # interim
    assert build_text_collections.run() is None

def test_auth_merge(db):
    from dlx.marc import Auth
    from dlx.scripts import auth_merge

    auth_merge.run(connect='mongomock://localhost', gaining_id=1, losing_id=2, user='test', skip_prompt=True)
    assert Auth.from_id(1).in_use(usage_type='bib') == 2
    assert Auth.from_id(2) is None

def test_import_marc(db, bibs, auths):
    from dlx import DB
    from dlx.marc import Auth
    from dlx.scripts import marc_import

    control = os.path.dirname(__file__) + '/marc.mrk'

    # these functions do not hve access to the fixture data for auth validation

    assert marc_import.run(
        connect='mongomock://localhost', 
        type='bib', 
        format='mrk', 
        file=control, 
        skip_auth_check=True,
        skip_prompt=True
    ) == None

    control = os.path.dirname(__file__) + '/marc.xml'

    assert marc_import.run(
        connect='mongomock://localhost', 
        type='bib', 
        format='xml', 
        file=control, 
        skip_auth_check=True,
        skip_prompt=True
    ) == None
