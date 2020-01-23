import sys, os
from unittest import TestCase
from dlx import DB
from scripts import excel_marc

class ExcelMarc(TestCase):
    def setUp(self):
        #DB.connect('mongomock://localhost')
        pass
    
    def test_run(self):
        path = os.path.join(os.path.dirname(__file__), 'marc.xlsx')
        sys.argv += ['--connect=mongomock://localhost', '--file={}'.format(path), '--type=bib', '--format=xml']
        
        with open(os.devnull, 'w') as nul:
            sys.stdout = nul
            excel_marc.main()
