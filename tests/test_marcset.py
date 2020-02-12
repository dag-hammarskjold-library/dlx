import re, os
from unittest import TestCase

from dlx import DB, marc
from dlx.marc import Marc, Bib, Auth, BibSet, AuthSet
from dlx.marc import QueryDocument, Condition

### test data

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

### tests  
    
class Batch(TestCase):
    def setUp(self):
        DB.connect('mongodb://.../?authSource=dummy',mock=True)
        Bib(Data.jbib).commit()
        Bib(Data.jbib2).commit()
        
    def test_from_query(self):
        bibset = BibSet.from_query({'_id': {'$in': [555,999]}})
        self.assertEqual(len(list(bibset.records)),2)
        
        query = QueryDocument(
            Condition(tag='245',subfields={'a': 'Another'})
        )
        bibset = BibSet.from_query(query)
        self.assertEqual(len(list(bibset.records)),1)
        
    def test_count(self):
        query = QueryDocument(Condition('245',modifier='exists'))
        self.assertEqual(BibSet.from_query(query).count,2)
        
    def test_cache(self):
        query = QueryDocument(Condition('245',{'a': 'Another'}))
        bibset = BibSet.from_query(query).cache()
        self.assertEqual(len(list(bibset.records)),1)
        self.assertEqual(len(list(bibset.records)),1)
        
    def test_from_table(self):
        from dlx.util import Table
        
        t = Table(
            [
                ['246a','1.246$b','1.269c','2.269c'],
                ['title','subtitle','1999-12-31','repeated'],
                ['title2','subtitle2','2000-01-01','repeated'],
            ]
        )
        
        bibset = BibSet.from_table(t)
        for bib in bibset.records:
            self.assertEqual(bib.get_value('246','b')[:8],'subtitle')
            self.assertEqual(bib.get_values('269','c')[1],'repeated')
            
    def test_from_excel(self):
        path = os.path.join(os.path.dirname(__file__), 'marc.xlsx')        

        bibset = BibSet.from_excel(path, date_format='%Y-%m-%d')
        for bib in bibset.records:
            self.assertEqual(bib.get_value('246','b')[:8],'subtitle')
            self.assertEqual(bib.get_values('269','c')[1],'repeated')

class BatchSerialization(TestCase):
    def test_xml(self):
        bibset = BibSet.from_query({'_id': {'$in': [555,999]}})
        Auth(Data.jauth).commit()
        Auth(Data.jauth2).commit()
        
        self.assertEqual(bibset.to_xml(),'<collection><record><controlfield tag="000">leader</controlfield><controlfield tag="008">controlfield</controlfield><datafield ind1=" " ind2=" " tag="245"><subfield code="a">This</subfield><subfield code="b">is the</subfield><subfield code="c">title</subfield></datafield><datafield ind1=" " ind2=" " tag="520"><subfield code="a">description</subfield></datafield><datafield ind1=" " ind2=" " tag="520"><subfield code="a">another description</subfield><subfield code="a">repeated subfield</subfield></datafield><datafield ind1=" " ind2=" " tag="650"><subfield code="a">header text</subfield></datafield><datafield ind1=" " ind2=" " tag="710"><subfield code="a">another header</subfield></datafield></record><record><controlfield tag="000">leader</controlfield><datafield ind1=" " ind2=" " tag="245"><subfield code="a">Another</subfield><subfield code="b">is the</subfield><subfield code="c">title</subfield></datafield><datafield ind1=" " ind2=" " tag="650"><subfield code="a">header text</subfield></datafield></record></collection>')
        