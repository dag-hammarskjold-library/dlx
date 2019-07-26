"""
Tests for dlx.file
"""

import re
from unittest import TestCase
from collections import Generator
from jsonschema import exceptions as X
from dlx import DB #, marc, MARC, Bib, Auth

class Instantiation(TestCase):
    def setUp(self):
        # note: this runs before every test method and clears the mock database
        DB.connect('mongodb://.../?authSource=dummy',mock=True)
        
    def test_instantiation(self):
        pass
        
    def test_validation(self):
        pass
        
class Import(TestCase):
    def setUp(self):
        pass
        

    
