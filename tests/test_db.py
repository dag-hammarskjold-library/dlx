"""
Tests for dlx.DB
"""

from unittest import TestCase
import mongomock 
from dlx import DB

class Test(TestCase):
	def test_connect(self):
		db = DB.connect('mongodb://u:pw@dummy.com/?authSource=dummy',mock=True)
		self.assertIsInstance(db,mongomock.Database)
		self.assertTrue(DB.check_connection())
		
	def test_db(self):
		self.assertIsInstance(DB.handle,mongomock.Database)
		self.assertIsInstance(DB.bibs,mongomock.Collection)
		self.assertIsInstance(DB.auths,mongomock.Collection)
		self.assertIsInstance(DB.files,mongomock.Collection)
		self.assertIsInstance(DB.config,dict)
