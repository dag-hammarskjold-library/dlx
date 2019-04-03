"""
Functions for building PyMongo queries for the `files` collection. 

All functions return BSON objects suitable for use in PyMongo queries. 
The returned objects can be embedded in dicts in order to compose more 
complex queries.
"""

from bson import SON

def by_id(type,id):
	"""Builds a query document for matching identifiers.
	
	Parameters
	----------
	param1 : str
		The type of idenitifier to match.
	param2 : str
		The identifier value to watch.
	
	Returns
	-------
	bson.son.SON
	
	
	Examples
	-------
	>>> query = by_id('ISBN','9789999999999')
	"""
	
	return SON (
		data = {
			'identifiers' : {
				'type' : type, 
				'value' : id 
			} 
		}
	)
	
def latest_by_id(type,id):
	"""Builds a query document for matching the latest file by identifiers.
	
	Parameters
	----------
	param1 : str
		The type of idenitifier to match.
	param2 : str
		The identifier value to watch.
	
	Returns
	-------
	bson.son.SON
	
	
	Examples
	-------
	>>> query = latest_by_id('ISBN','9789999999999')
	"""
	
	return SON (
		data = {
			'$and' : [
				{
					'identifiers' : {
						'type' : type, 
						'value' : id 
					}
				},
				{
					'superceded_by' : {
						'$exists' : False
					}
				}
			]
		}
	)
	
def by_id_lang(type,id,lang):
	return SON (
		data = {
			'$and' : [
				{
					'identifiers' : {
						'type' : type, 
						'value' : id  
					}
				},
				{
					'languages' : lang
				}
			]
		}
	)
	
def latest_by_id_lang(type,id,lang):
	return SON (
		data = {
			'$and' : [
				{
					'identifiers' : {
						'type' : type, 
						'value' : id  
					}
				},
				{
					'languages' : lang
				},
				{
					'superceded_by' : {
						'$exists' : False
					}
				}
			]
		}
	)
