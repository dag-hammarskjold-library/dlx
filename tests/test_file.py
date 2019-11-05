"""
Tests for dlx.file
"""

import os, time, re
from unittest import TestCase
from collections import Generator
from jsonschema import exceptions as X
from dlx import DB
from dlx.file import get_md5, File, Identifier, FileExists

class Import(TestCase):
    testfile = ''
    testfile2 = ''
    
    def setUp(self):
        DB.connect('mongodb://.../?authSource=dummy',mock=True)
        
        try:
            tempdir = (os.environ['HOMEPATH']) + '/temp'
        except:
            tempdir = (os.environ['HOME']) + '/temp'
                   
        if not os.path.exists(tempdir):
            os.mkdir(tempdir)
        
        path = tempdir + '/test.txt'
        path2 = path + '2'
        
        with open(path,'w') as fh:
            print('testing @ ' + str(time.time()), file=fh)
            
        with open(path2,'w') as fh:
            print('testing 2 @ ' + str(time.time()), file=fh)
            
        Import.testfile = path
        Import.testfile2 = path2
            
    def tearDown(self):
        os.remove(Import.testfile)
        os.remove(Import.testfile2)
        
    def test_ingestion(self):
        file = File.ingest(
            Import.testfile,
            [Identifier('symbol','A/TEST'), Identifier('isbn','12312341234')],
            ['EN']
        )
        
        self.assertEqual(file.id,get_md5(Import.testfile))
        self.assertTrue(file.exists())
        
    def test_reject_duplicate(self):
        file1 = File.ingest(
            Import.testfile,
            [Identifier('symbol','A/TEST')],
            ['EN']
        )
        
        with self.assertRaises(FileExists):
            file2 = File.ingest(
                Import.testfile,
                [Identifier('symbol','A/TEST')],
                ['EN']
            )
            
    def test_supercede(self):
        file1 = File.ingest(
            Import.testfile,
            [Identifier('symbol','A/TEST')],
            ['EN']
        )
        
        file2 = File.ingest(
            Import.testfile2,
            [Identifier('symbol','A/TEST')],
            ['EN']
        )
        
        superceded = next(File.match_id_lang('symbol','A/TEST','EN'))
        
        self.assertEqual(superceded.superceded_by, get_md5(Import.testfile2))
    
    def test_validation(self):
        pass
        
class Query(TestCase):
    pass
    
