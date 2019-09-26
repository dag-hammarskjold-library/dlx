"""
Tests for dlx.marc
"""

import re
from unittest import TestCase
from collections import Generator
from jsonschema import exceptions as X
from dlx import DB, marc, MARC, Bib, Auth
from dlx.query import jmarc as Q

from bson import SON

class Data(object):
    jbib = {
        '_id' : 999,
        '000' : ['leader'],
        '008' : ['controlfield'],
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
                    },
                    {
                        'code' : 'a',
                        'value' : 'repeated subfield'
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
        '150' : [
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
    
    jauth2 = {
        '_id' : 333,
        '150' : [
            {
                'indicators' : [' ', ' '],
                'subfields' : [
                    {
                        'code' : 'a',
                        'value' : 'another header'
                    }
                ]
            }
        ]
    }
    
    invalid = {
        '_id' : 'string invalid',
        '150' : [
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
        
    def test_match_datafield_value(self):
        bibs = Bib.match_value('650','a','header text')
        self.assertIsInstance(bibs,Generator)
        bibs = list(bibs)
        for bib in bibs: self.assertIsInstance(bib,Bib)
        self.assertEqual(len(bibs),2)
        
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
        
        self.assertIsInstance(bib,Bib)
        self.assertEqual(bib.id,999)
        
    def test_match_fields(self):
        bibs = Bib.match_fields (
            ('245',('b','is the'),('c','title')),
            ('650',('a','header text'))
        )
        self.assertIsInstance(bibs,Generator)
        bibs = list(bibs)
        for bib in bibs: self.assertIsInstance(bib,Bib)
        self.assertEqual(len(bibs),2)
        
    def test_match_fields(self):
        bibs = Bib.match_fields_or (
            ('245',('a','This'),('c','title')),
            ('245',('a','Another'))
        )
        self.assertIsInstance(bibs,Generator)
        bibs = list(bibs)
        for bib in bibs: self.assertIsInstance(bib,Bib)
        self.assertEqual(len(bibs),2)
        
        auth = Auth.match_id(777)        
        self.assertIsInstance(auth,Auth)
        self.assertEqual(auth.id,777)
    
    def test_match_controlfield_value(self):
        bibs = Bib.match_value('000',None,'leader')
        self.assertIsInstance(bibs,Generator)
        bibs = list(bibs)
        self.assertEqual(len(bibs),2)
        for bib in bibs: self.assertIsInstance(bib,Bib)
        
    def test_match_datafield_value(self):
        bibs = Bib.match_value('245','c','title')
        self.assertIsInstance(bibs,Generator)
        bibs = list(bibs)
        for bib in bibs: self.assertIsInstance(bib,Bib)
        self.assertEqual(len(bibs),2)
        
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
        
    def test_match_fields(self):
        bibs = Bib.match_fields (
            ('245',('b','is the'),('c','title')),
            ('650',('a','header text'))
        )
        self.assertIsInstance(bibs,Generator)
        bibs = list(bibs)
        for bib in bibs: self.assertIsInstance(bib,Bib)
        self.assertEqual(len(bibs),2)
        
    def test_match_fields_or(self):
        bibs = Bib.match_fields_or (
            ('245',('b','is the'),('c','title')),
            ('245',('a','Another'))
        )
        self.assertIsInstance(bibs,Generator)
        bibs = list(bibs)
        for bib in bibs: self.assertIsInstance(bib,Bib)    
        self.assertEqual(len(bibs),2)
            
    def test_match_xrefs(self):
        bibs = Bib.match_xrefs('650','a',777)
        self.assertIsInstance(bibs,Generator)
        bibs = list(bibs)
        for bib in bibs: self.assertIsInstance(bib,Bib)
        self.assertEqual(len(bibs),2)
        
    def test_match_multi(self):
        take = [
            Bib.match_value('245','c','title'),
            Bib.match_value('650','a','header text'),
        ]
        exclude = [
            Bib.match_value('245','a','This')
        ]
        
        bibs = list(Bib.match_multi(take,exclude))
        
        for bib in bibs: self.assertIsInstance(bib,Bib)
        for bib in bibs: self.assertEqual(bib.id,555)
        
        self.assertEqual(len(bibs),1)
       
    def test_match(self):
        m = marc.record.Matcher('245',('a','This'),('b','is the'))
        bibs = list(Bib.match(m))
        self.assertEqual(len(bibs),1)
        self.assertEqual(bibs[0].id,999)
    
    def test_match_not(self):
        m = marc.record.Matcher('245',('a','This'),('b','is the'),modifier='not')
        bibs = list(Bib.match(m))
        self.assertEqual(len(bibs),1)
        self.assertEqual(bibs[0].id,555)
        
        m = marc.record.Matcher('245',('c',re.compile('title')),modifier='not')
        bibs = list(Bib.match(m))
        self.assertEqual(len(bibs),0)
        
    def test_match_multiple_matchers(self):
        match1 = marc.record.Matcher('245',('b','is the'))
        match2 = marc.record.Matcher('650',('a','a fake subject'),modifier='not')
        
        bibs = list(Bib.match(match1,match2))
        self.assertEqual(len(bibs),2)
              
class Index(TestCase):
    def setUp(self):
        DB.connect('mongodb://.../?authSource=dummy',mock=True)
        
        Bib(Data.jbib).commit()
        Auth(Data.jauth).commit()
    
    def test_controlfield_index(self):
        Bib.literal_index('000')
    
    def test_literal_index(self):
        Bib.literal_index('245')
        
    def test_linked_index(self):
        Bib.linked_index('650')
        
    def test_hybrid_index(self):
        Bib.hybrid_index('710')
    
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
        self.assertEqual(
            ' '.join(bib.get_values('245','a','b','c')),
            'This is the title'
        )
        self.assertEqual(
            ['description','another description','repeated subfield'],
            list(bib.get_values('520','a'))
        )
        
        self.assertEqual(bib.get_value('520','a',address=[1,0]),'another description')
        self.assertEqual(bib.get_value('520','a',address=[1,1]),'repeated subfield')
    
    def test_get_auth(self):
        Auth(Data.jauth).commit()
        auth = Auth.match_id(777)

        self.assertEqual(auth.get_value('150','a'), 'header text')
        self.assertEqual(auth.header_value('a'), 'header text')
        
    def test_get_util(self):
        bib = Bib.match_id(999)
    
        self.assertEqual(bib.get_tags(),['000','008','245','520','520','650','710'])
        self.assertEqual(bib.get_xrefs(),[333,777])
        
    def test_lookup(self):
        # test auth lookup
        
        bib = Bib.match_id(999)
        
        self.assertEqual(bib.get_value('650','a'),'N/A')
        
        Auth(Data.jauth).commit()
        
        self.assertEqual(bib.get_value('650','a'),'header text')

class Set(TestCase):
    def setUp(self):
        DB.connect('mongodb://.../?authSource=dummy',mock=True)
        
        Bib(Data.jbib).commit()
                      
    def test_set_new(self):
        bib = Bib.match_id(999)
        
        bib.set('520','b','added subfield').set('520','c','another new one')
        self.assertEqual(bib.get_value('520','b'),'added subfield')
        self.assertEqual(bib.get_value('520','c'),'another new one')
        
    def test_set_new_field(self):
        bib = Bib.match_id(999)
        
        bib.set('521','a','new field and value')
        self.assertEqual(bib.get_value('521','a'),'new field and value')
        
        bib.set('007',None,'new value')
        self.assertEqual(bib.get_value('007',None),'new value')
        
        bib.set('008',None,'new controlfield',address=['+',None])
        self.assertEqual(bib.get_value('008',None,address=[1,None]),'new controlfield')
        
    def test_set_existing(self):
        bib = Bib.match_id(999)
        
        bib.set('520','a','changed subfield')
        self.assertEqual(bib.get_value('520','a'),'changed subfield')
        
        bib.set('520','a','another changed one',address=[1,0])
        self.assertEqual(bib.get_value('520','a',address=[1,0]),'another changed one')
        
        bib.set('008',None,'changed controlfield')
        self.assertEqual(bib.get_value('008',None),'changed controlfield')
        
    def test_set_existing_all(self):
        bib = Bib.match_id(999)
    
        bib.set('520','a','changed all',address=['*','*'])
        for val in bib.get_values('520','a'):
            self.assertEqual(val,'changed all')
            
        for i in range(0,3):
            bib.set('005',None,'set all',address=['+',None])
             
        bib.set('005',None,'changed all',address=['*',None])
        for val in bib.get_values('005',None):
            self.assertEqual(val,'changed all')
                
    def test_set_existing_linked(self):
        Auth(Data.jauth).commit()
        Auth(Data.jauth2).commit()
        bib = Bib.match_id(999)
        
        bib.set('650','a',333)
        self.assertEqual(bib.get_value('650','a'),'another header')
        
        bib.set('650','a',777)
        self.assertEqual(bib.get_value('650','a'),'header text')
        
    def test_set_match_literal(self):
        bib = Bib.match_id(999)
        
        bib.set('520','a','changed',matcher=re.compile('desc'))
        self.assertEqual(bib.get_value('520','a'),'changed')
        self.assertEqual(bib.get_values('520','a')[1],'another description')
        
        bib.set('520','a','this shouldn\'t match',matcher=re.compile('x'))
        self.assertEqual(bib.get_value('520','a'),'changed')
        
        bib.set('008',None,'changed',matcher=re.compile('trolfiel'))
        self.assertEqual(bib.get_value('008',None),'changed')
         
    def test_set_match_literal_all(self):
        bib = Bib.match_id(999)
        
        bib.set('520','a','changed',matcher=re.compile('.*desc'),place='*')
        for val in bib.get_values('520','a'):     
            self.assertEqual(bib.get_value('520','a'),'changed')
    
    def test_set_match_linked(self):
        Auth(Data.jauth).commit()
        Auth(Data.jauth2).commit()
        bib = Bib.match_id(999)
        
        bib.set('650','a',333,matcher=[777])
        self.assertEqual(bib.get_value('650','a'),'another header')
        
class Todo(TestCase):
   
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
        
