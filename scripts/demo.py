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
from dlx import DB, JBIB, match

def MAIN():
	connection_string = sys.argv[1]
	tag = sys.argv[2]
	code = sys.argv[3]
	val = sys.argv[4]

	db = DB(connection_string)
	
	query = match(tag,code,val)
	cursor = db.bibs.find(query)

	i = 0
	size = 0
	
	for dict in cursor:
		i += 1
		size += len(BSON.encode(dict))
		
		jmarc = JBIB(dict)

		print('symbols: ' + '; '.join(jmarc.symbols()))
		print('title: ' + jmarc.title())
		print('date: ' + jmarc.get_value('269','a'))
		
		# slower because auth-controlled values need to be looked up
		print('authors: ' + '; '.join(jmarc.get_values('710','a')))
		print('subjects: ' + '; '.join(jmarc.get_values('650','a')))
		
		for lang in ('EN','FR'):
			print(lang + ': ' + jmarc.file(lang))
		
		print('-' * 100)
		
	if cursor.retrieved == 0:
		print('no results :(')
		exit()

	print(str(i) + ' results')
	print(str(size) + ' bytes')

MAIN()
