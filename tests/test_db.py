"""
Tests for dlx.DB
"""

from unittest import TestCase
import mongomock 
from dlx import DB

class Test(TestCase):
    def test_connect(self):
        DB.connect('mongodb://u:pw@dummy.com/?authSource=dummy', mock=True)
        self.assertIsInstance(DB.client, mongomock.MongoClient)
        assert DB.database_name == 'dummy'
        
        DB.connect('mongomock://localhost')
        self.assertIsInstance(DB.client, mongomock.MongoClient)
        assert DB.database_name == 'testing'

        # new connection option
        DB.connect('mongomock://localhost', database='new_mock_database')
        assert DB.database_name == 'new_mock_database'
        
    def test_db(self):
        self.assertIsInstance(DB.handle, mongomock.Database)
        self.assertIsInstance(DB.bibs, mongomock.Collection)
        self.assertIsInstance(DB.auths, mongomock.Collection)
        self.assertIsInstance(DB.files, mongomock.Collection)
        self.assertIsInstance(DB.config, dict)
