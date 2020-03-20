"""
Tests for dlx.file
"""

import os, time, re
from unittest import TestCase
from jsonschema import exceptions as X
from dlx import DB, file

class Import(TestCase):
    def setUp(self):
        # note: this runs before every test method and clears the mock database
        DB.connect('mongodb://.../?authSource=dummy',mock=True)
        
    def test_instantiation(self):
        #for kv in os.environ: print(kv)
        try:
            tempdir = (os.environ['HOMEPATH']) + '/temp'
        except:
            tempdir = (os.environ['HOME']) + '/temp'
            
        if not os.path.exists(tempdir):
            os.mkdir(tempdir)
        
        path = tempdir + '/test.txt'
        with open(path,'w') as fh:
            print('testing @ ' + str(time.time()), file=fh)
        
        result = file.Import(path,[file.Identifier('symbol','A/TEST')],['EN'])
        
        os.remove(path)
        
    def test_validation(self):
        pass
        

