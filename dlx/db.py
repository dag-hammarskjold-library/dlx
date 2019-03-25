'''
'''
import re
from pymongo import MongoClient

class DB(object):
	handle = None
	config = {
		'bibs_collection_name' : 'bibs',
		'auths_collection_name' : 'auths',
		'files_collection_name' : 'files',
	}
	
	def __init__(self,connection_string,**kwargs):
		client = MongoClient()
		
		try:
			client = MongoClient(connection_string)
		except:
			print('Database connection failed')
			exit()
			
		DB.config['connection_string'] = connection_string
			
		match = re.search('\?authSource=([\w]+)',connection_string)
		DB.config['database_name'] = match.group(1)
			
		DB.handle = client[DB.config['database_name']]
		self.bibs = DB.handle[DB.config['bibs_collection_name']]
		self.auths = DB.handle[DB.config['auths_collection_name']]
		self.files = DB.handle[DB.config['files_collection_name']]
	
	def literal_index(self,tag):
		field = tag + '.subfields'
		self.bibs.create_index({field : 1})
		self.bibs.create_index({field + '.code' : 1, field + '.value' : 1})
		
	def linked_index(self,tag):
		field = tag + '.subfields'
		self.bibs.create_index({field : 1})
		self.bibs.create_index({field + '.code' : 1, field + '.xref' : 1})
		
	def hybrid_index(self,tag):
		field = tag + '.subfields'
		self.bibs.create_index({field : 1})
		self.bibs.create_index({field + '.code' : 1, field + '.value' : 1, field + '.xref' : 1})