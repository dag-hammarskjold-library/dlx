"""
Tests for dlx.marc
"""

import re
from unittest import TestCase
from collections import Generator
from jsonschema import exceptions as X
from dlx import DB, marc, MARC, Bib, Auth

class Data(object):
	jbib = {
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
		'520' : [
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
				],
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
	
	jbib2 = {
		'_id' : 555,
		'000' : ['leader'],
		'245' : [
			{
				'indicators' : [' ',' '],
				'subfields' : [
					{
						'code' : 'a',
						'value' : 'Another'
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
		'650' : [
			{
				'indicators' : [' ' ,' '],
				'subfields' : [
					{
						'code' : 'a',
						'xref' : 777
					}
				]
			}
		]
	}
	
	jauth = {
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
	
	invalid = {
		'_id' : 'string invalid',
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

class Instantiation(TestCase):
	def setUp(self):
		# note: this runs before every test method and clears the mock database
		DB.connect('mongodb://.../?authSource=dummy',mock=True)
				
	def test_instantiation(self):
		# test instantiation
		
		record = MARC(Data.jbib)
		self.assertIsInstance(record,MARC)

		record = MARC(Data.jauth)
		self.assertIsInstance(record,MARC)
		
		bib = Bib(Data.jauth)
		self.assertIsInstance(bib,Bib)
		
		auth = Auth(Data.jauth)
		self.assertIsInstance(auth,Auth)
		
		for f in bib.controlfields + auth.controlfields:
			self.assertIsInstance(f,marc.field.Controlfield)
			
		for f in bib.datafields + auth.controlfields:
			self.assertIsInstance(f,marc.field.Datafield)
			
			for s in f.subfields + auth.controlfields:
				self.assertIsInstance(s,marc.subfield.Subfield)
		
	def test_validation(self):
		# test validation
		
		self.assertRaises(X.ValidationError, MARC.validate, Data.invalid)
		self.assertIsNone(MARC.validate(Data.jbib))

class Commit(TestCase):
	def test_commit(self):
		# test commit
		
		bib = Bib(Data.jbib)
		auth = Auth(Data.jauth)
		
		self.assertTrue(bib.commit().acknowledged)
		self.assertTrue(auth.commit().acknowledged)

class Query(TestCase):
	def setUp(self):
		DB.connect('mongodb://.../?authSource=dummy',mock=True)
		
		Bib(Data.jbib).commit()
		Bib(Data.jbib2).commit()
		Auth(Data.jauth).commit()
	
	def test_find(self):
		# test queries

		bib = Bib.find_one({'_id' : 999})
		self.assertIsInstance(bib,Bib)
		
		bibs = Bib.find({})
		self.assertIsInstance(bibs,Generator)
		bibs = list(bibs)
		for bib in bibs: self.assertIsInstance(bib,Bib)
		self.assertEqual(len(bibs),2)
	
	def test_match_id(self):
		bib = Bib.match_id(999)	
		self.assertIsInstance(bib,Bib)
		self.assertEqual(bib.id,999)
		
		auth = Auth.match_id(777)		
		self.assertIsInstance(auth,Auth)
		self.assertEqual(auth.id,777)
	
	def test_match_controlfield_value(self):
		bibs = Bib.match_value('000',None,'leader')
		self.assertIsInstance(bibs,Generator)
		bibs = list(bibs)
		self.assertEqual(len(bibs),2)
		for bib in bibs: self.assertIsInstance(bib,Bib)
		
		bib = Bib.match_value_one('000',None,'leader')
		self.assertIsInstance(bib,Bib)
		self.assertEqual(bib.id,999)
		
	def test_match_datafield_value(self):
		bibs = Bib.match_value('245','c','title')
		self.assertIsInstance(bibs,Generator)
		bibs = list(bibs)
		for bib in bibs: self.assertIsInstance(bib,Bib)
		self.assertEqual(len(bibs),2)
		
		bib = Bib.match_value_one('245','c','title')
		self.assertIsInstance(bib,Bib)
		self.assertEqual(bib.id,999)
		
	def test_match_values(self):
		bibs = Bib.match_values(('000',None,'leader'),('245','c','title'))
		self.assertIsInstance(bibs,Generator)
		bibs = list(bibs)
		for bib in bibs: self.assertIsInstance(bib,Bib)
		self.assertEqual(len(bibs),2)
		
	def test_match_values_or(self):
		bibs = Bib.match_values_or(('245','a','This'),('245','a','Another'))
		self.assertIsInstance(bibs,Generator)
		bibs = list(bibs)
		for bib in bibs: self.assertIsInstance(bib,Bib)
		self.assertEqual(len(bibs),2)
		
	def test_match_field(self):
		bibs = Bib.match_field('245',('b','is the'),('c','title'))
		self.assertIsInstance(bibs,Generator)
		bibs = list(bibs)
		for bib in bibs: self.assertIsInstance(bib,Bib)
		self.assertEqual(len(bibs),2)
		
		bib = Bib.match_field_one('245',('b','is the'),('c','title'))
		self.assertIsInstance(bib,Bib)
		self.assertEqual(bib.id,999)
		
	def test_match_fields(self):
		#todo
		pass
		
	def test_match_xrefs(self):
		bibs = Bib.match_xrefs('650','a',777)
		self.assertIsInstance(bibs,Generator)
		bibs = list(bibs)
		for bib in bibs: self.assertIsInstance(bib,Bib)
		self.assertEqual(len(bibs),2)
		
class Get(TestCase):
	def setUp(self):
		DB.connect('mongodb://.../?authSource=dummy',mock=True)
		
		Bib(Data.jbib).commit()
	
	def test_get_bib(self):
		# test get methods and lookup
		
		bib = Bib.match_id(999)
		
		self.assertIsInstance(bib.get_field('245'),marc.field.Field)
		
		for f in bib.get_fields(): self.assertIsInstance(f,marc.field.Field)
			
		self.assertEqual(bib.get_value('000'),'leader')
		self.assertEqual(bib.get_value('245','a'), 'This')
		self.assertEqual(' '.join(bib.get_values('245','a','b','c')), 'This is the title')
		self.assertEqual(['description','another description'],list(bib.get_values('520','a')))
	
	def test_get_auth(self):
		Auth(Data.jauth).commit()
		
		auth = Auth.match_id(777)
		
		self.assertEqual(auth.get_value('100','a'), 'header text')
		
	def test_lookup(self):
		# test auth lookup
		
		bib = Bib.match_id(999)
		
		self.assertEqual(bib.get_value('650','a'),'N/A')
		
		Auth(Data.jauth).commit()
		
		self.assertEqual(bib.get_value('650','a'),'header text')
		
		self.assertEqual(bib.get_tags(),['000','245','520','520','650','710'])
		self.assertEqual(bib.get_xrefs(),[333,777])

class Todo(TestCase):
			
	def test_d(self):
		# test set methods
		#todo
		pass
		
	def test_e(self):
		# test utility methods
		#todo
		pass
		
	def test_f(self):
		# test serializations
		#todo
		pass
		
	def test_g(self):
		# test de-serializations
		#todo
		pass
		
