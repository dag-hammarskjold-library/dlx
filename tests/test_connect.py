
from unittest import TestCase
import mongomock 
from dlx import DB

class Test(TestCase):
	def test_connect(self):
		db = DB.connect('mongodb://u:pw@dummy.com/?authSource=dummy',mock=True)
		
		self.assertIsInstance(db, mongomock.Database)
		
		self.assertTrue(DB.check_connection())
	

	
		
