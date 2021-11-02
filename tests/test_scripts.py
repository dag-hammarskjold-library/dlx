import sys, os, pytest

def test_init_db(db):
    from dlx import DB, Config
    from dlx.scripts import init_indexes
    
    sys.argv[1:] = ['--connect=mongomock://localhost']
    
    assert init_indexes.run() is None # runs the function, no return value

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
        .commit()
    
    # interim
    assert build_logical_fields.run() is None
