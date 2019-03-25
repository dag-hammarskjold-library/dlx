#!/usr/bin/env python

'''
This script will be used to create JMARC 2.0
'''

import sys
sys.path[0] = sys.path[0] + '\..'

from dlx.jmarc import JMARC
from dlx.db import DB
from dlx.query import *

def run():
	connect_str = sys.argv[1]
	#tag = sys.argv[2]
	#code = sys.argv[3]
	#val = sys.argv[4]
	
	#print(connect_str)
	
	db = DB(connect_str)
	
	cursor = db.bibs.find({})
	
	for dict in cursor:
		jmarc = JMARC(dict)
		
		print(jmarc.to_bson2())
		
		#db.insert_one('bibs',{})
		
		print('-' * 100);
		
		
####
	
run()