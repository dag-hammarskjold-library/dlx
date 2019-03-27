'''
'''
import re
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

class DB(object):
	connected = False
	handle = None
	config = {
		'bibs_collection_name' : 'bibs',
		'auths_collection_name' : 'auths',
		'files_collection_name' : 'files',
	}
	
	def __init__(self,connection_string,**kwargs):
		client = MongoClient(connection_string,serverSelectionTimeoutMS=2)
		
		try:
			client.admin.command('ismaster')
		except:
			print('Database connection failed')
			exit()
		
		DB.connected = True		
		DB.config['connection_string'] = connection_string
			
		match = re.search('\?authSource=([\w]+)',connection_string)
		
		if match:
			DB.config['database_name'] = match.group(1)
		else:
			print('Could not parse database name from connection string')
			exit()
			
		DB.handle = client[DB.config['database_name']]
		DB.bibs = DB.handle[DB.config['bibs_collection_name']]
		DB.auths = DB.handle[DB.config['auths_collection_name']]
		DB.files = DB.handle[DB.config['files_collection_name']]
	
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