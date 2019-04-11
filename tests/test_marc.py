"""
Tests for dlx.marc
"""

from unittest import TestCase
from dlx import DB, marc, MARC, Bib, Auth

class Test(TestCase):
	DB.connect('mongodb://.../?authSource=dummy',mock=True)
		
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
				'subfields' : [{'code' : 'a', 'xref' : 777}]
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
		
	def test_record(self):
		record = MARC(Test.bib_data)
		self.assertIsInstance(record,MARC)
		
		for f in record.controlfields:
			self.assertIsInstance(f,marc.field.Controlfield)
			
		for f in record.datafields:
			self.assertIsInstance(f,marc.field.Datafield)
			
			for s in f.subfields:
				self.assertIsInstance(s,marc.subfield.Subfield)
			
	def test_bib(self):
		Test.bib = Bib(Test.bib_data)
		self.assertIsInstance(Test.bib,Bib)
		
	def test_auth(self):
		Test.auth = Auth(Test.auth_data)
		self.assertIsInstance(Test.auth,Auth)
		
	def test_commit(self):
		Test.bib.commit()
		
	def test_get_methods(self):
		record = Test.bib
		
		self.assertIsInstance(record.get_field('245'),marc.field.Field)
		
		for x in [f for f in record.get_fields()]: 
			self.assertIsInstance(x,marc.field.Field)
			
		self.assertEqual(record.get_value('000'),'leader')	
		
		self.assertEqual(record.get_value('245','a'), 'This')
		
		self.assertEqual(' '.join(record.get_values('245','a','b','c')), 'This is the title')
		
		self.assertEqual(['description','another description'],list(record.get_values('520','a')))
		
		# auth lookup returns 'N/A' because there is no auth data in the mock DB yet
		self.assertEqual(record.get_value('650','a'),'N/A')	

		Test.auth.commit()
		
		self.assertEqual(record.get_value('650','a'),'header text')
		
		self.assertEqual(record.tags(),['000','245','520','520','650'])
		
	def test_set_methods(self):
		pass
		
	def test_queries(self):
		pass
		
		
