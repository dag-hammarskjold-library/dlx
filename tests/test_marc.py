"""
Tests for dlx.marc
"""

from unittest import TestCase
from dlx import DB, marc, MARC, Bib, Auth

class Test(TestCase):	
	bib_data = {
		'_id' : 999,
		'000' : ['leader'],
		'245' : [
			{
				'indicators' : [' ',' '],
				'subfields' : [
					{
						'code' : 'a',
						'value' : 'This'
					},
					{
						'code' : 'b',
						'value' : 'is the'
					},
					{
						'code' : 'c',
						'value' : 'title'
					}
				]
			}
		],
		'520' :[
			{
				'indicators' : [' ' ,' '],
				'subfields' : [
					{
						'code' : 'a',
						'value' : 'description'
					}
				]
			},
			{
				'indicators' : [' ' ,' '],
				'subfields' : [
					{
						'code' : 'a',
						'value' : 'another description'
					}
				]
			}
		],
		'650' : [
			{
				'indicators' : [' ', ' '],
				'subfields' : [
					{
						'code' : 'a', 
						'xref' : 777
					}
				]
			}
		],
		'710' : [
			{
				'indicators' : [' ',' '],
				'subfields' : [
					{
						'code' : 'a',
						'xref' : 333
					}
				]
			}
		]
	}
	
	auth_data = {
		'_id' : 777,
		'100' : [
			{
				'indicators' : [' ', ' '],
				'subfields' : [
					{
						'code' : 'a',
						'value' : 'header text'
					}
				]
			}
		]
	}
	
	def setUp(self):
		# note: this runs before every test method and clears the mock database
		DB.connect('mongodb://.../?authSource=dummy',mock=True)
		
	def test_record(self):
		# test instantiation
		
		record = MARC(Test.bib_data)
		self.assertIsInstance(record,MARC)
		
		record = MARC(Test.auth_data)
		self.assertIsInstance(record,MARC)
		
		bib = Bib(Test.bib_data)
		self.assertIsInstance(bib,Bib)
		
		for f in bib.controlfields:
			self.assertIsInstance(f,marc.field.Controlfield)
			
		for f in bib.datafields:
			self.assertIsInstance(f,marc.field.Datafield)
			
			for s in f.subfields:
				self.assertIsInstance(s,marc.subfield.Subfield)

		auth = Auth(Test.auth_data)
		self.assertIsInstance(auth,Auth)
		
		# test commit
		
		self.assertTrue(bib.commit().acknowledged)
		self.assertTrue(auth.commit().acknowledged)
		
		# test queries
		
		self.assertIsInstance(Bib.match_id(999),Bib)		
		self.assertIsInstance(Auth.match_id(777),Auth)
		
		# test get methods
		
		self.assertIsInstance(bib.get_field('245'),marc.field.Field)
		
		for f in record.get_fields(): 
			self.assertIsInstance(f,marc.field.Field)
			
		self.assertEqual(bib.get_value('000'),'leader')
		self.assertEqual(bib.get_value('245','a'), 'This')
		self.assertEqual(' '.join(bib.get_values('245','a','b','c')), 'This is the title')
		self.assertEqual(['description','another description'],list(bib.get_values('520','a')))

		self.assertEqual(bib.get_value('650','a'),'header text')
		self.assertEqual(bib.get_value('710','a'),'N/A')
		
		self.assertEqual(bib.get_tags(),['000','245','520','520','650','710'])
		self.assertEqual(bib.get_xrefs(),[777,333])
		
		# test set methods
		#todo
		
		# test utility methods
		#todo
		
		# test serializations
		#todo
		
		# test de-serializations
		#todo
		
	def test_bib(self):
		pass
	
	def test_auth(self):
		pass
		
