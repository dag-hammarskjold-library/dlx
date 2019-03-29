"""
Provides the DB class for connecting to and accessing the database.
"""

import re
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from .exceptions import NotConnected

class DB(object):
	"""A class providing a global database connection.
	
	The DB class does not get instantiated. When class method DB.connect() 
	is called and a succesful connection is made, all relevant database 
	handles are stored as class attributes.
	
	Class attributes
	-----------
	connected : bool
	handle : pymongo.database.Database
	bibs : pymongo.collection.Collection
	auths : pymongo.collection.Collection
	file : pymongo.collection.Collection
	config : dict
	"""
		
	connected = False
	handle = None
	bibs = None
	auths = None
	files = None
	config = {
		'bibs_collection_name' : 'bibs',
		'auths_collection_name' : 'auths',
		'files_collection_name' : 'files',
	}
	
	## class 
	
	@classmethod
	def connect(cls,connection_string):
		"""Connects to the database and stores database and collection handles
		as class variables.
		
		Parameters
		----------
		param1 : str
			MongoDB connection string.
		
		Returns
		-------
		pymongo.database.Database
			The database handle automatically gets stored as class attribute 'handle'.
		
		Raises
		------
		pymongo.errors.ConnectionFailure
			If connection fails.
		"""
		
		client = MongoClient(connection_string,serverSelectionTimeoutMS=2)
		
		try:
			client.admin.command('ismaster')
		except ConnectionFailure:
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
		
		return DB.handle
	
	## static
	
	@staticmethod
	def check_connection():
		"""Raises an exception if the database has not been connected to.
		
		This is used to prevent attempts at database operations without
		being connected, which can create hard-to-trace errors.
		
		Returns
		-------
		None
		
		Raises
		------
		dlx.exceptions.NotConnected
			If the database has not been connected to yet.
		
		"""
		if DB.connected == False:
			raise NotConnected
		
	
	
	
	
	
	
