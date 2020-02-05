import sys, os
from unittest import TestCase
from dlx import DB
from scripts import excel_marc

class ExcelMarc(TestCase):
    def setUp(self):
        #DB.connect('mongomock://localhost')
        pass
    
    def test_run(self):
        file = os.path.dirname(__file__) + '/marc.xlsx'
        out = os.path.dirname(__file__) + '/out.mrc'
        defaults = os.path.dirname(__file__) + '/defaults.xlsx'
        sys.argv[1:] = ['--connect=mongomock://localhost', '--file={}'.format(file), '--type=bib', '--format=mrc', '--out={}'.format(out), '--defaults={}'.format(defaults), '--check=245a']
        excel_marc.main()
        self.assertTrue(os.path.exists(out))
    
    def tearDown(self):
        os.remove(os.path.dirname(__file__) + '/out.mrc')
