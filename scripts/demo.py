#!/usr/bin/env python

'''
Usage: 

python jmarc.py <MongoDB connection string> <tag> <subfield code> <value to match>
i.e. -> python jmarc.py "mongodb://..." 191 a S/2011/10
'''

import sys, os, json, re

# add project root to include paths for use wihtout installation
sys.path.append(os.path.dirname(__file__) + '/..')

from bson import BSON
#from dlx import DB, JBIB, JAUTH, match

import dlx

def MAIN():
	connection_string = sys.argv[1]
	tag = sys.argv[2]
	code = sys.argv[3]
	val = sys.argv[4]
	
	# connect to DB
	dlx.DB(connection_string)
	
	for jmarc in dlx.JBIB.find(tag,code,val):
	
		print('symbols: ' + '; '.join(jmarc.symbols()))
		print('title: ' + jmarc.title())
		print('date: ' + jmarc.get_value('269','a'))
		
		# slower because auth-controlled values need to be looked up
		print('authors: ' + '; '.join(jmarc.get_values('710','a')))
		print('subjects: ' + '; '.join(jmarc.get_values('650','a')))
		
		for lang in ('EN','FR'):
			print(lang + ': ' + jmarc.file(lang))
		
		print('-' * 100)
		

	
MAIN()
