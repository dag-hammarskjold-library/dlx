import os
from unittest import TestCase
from dlx.util import Table

class Data(object):
    data = [
        ['246a','246b','1.269c','2.269c'],
        ['title','subtitle','1999-12-31','repeated'],
        ['title2','subtitle2','2000-01-01','repeated'],
    ]
    
class TestTable(TestCase):
    def setUp(self):
	    pass
        
    def test_init(self):
        d = Data.data
        self.assertEqual(Table(d).to_list(),d)
        
    def test_from_excel(self):
        path = os.path.join(os.path.dirname(__file__), 'test.xlsx')
        table = Table.from_excel(path)
        self.assertEqual(table.to_list(),Data.data)
    
    def test_set_get(self):
        t = Table(Data.data)
        t.set(1,'246a','changed')
        self.assertEqual(t.get(1,'246a'),'changed')
        
    def test_to_html(self):
        data = '<table><tr><td>246a</td><td>246b</td><td>1.269c</td><td>2.269c</td></tr><tr><td>title</td><td>subtitle</td><td>1999-12-31</td><td>repeated</td></tr><tr><td>title2</td><td>subtitle2</td><td>2000-01-01</td><td>repeated</td></tr></table>'
        self.assertEqual(Table(Data.data).to_html(),data)
