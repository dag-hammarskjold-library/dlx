"""
Functions for building PyMongo queries. 

All functions return BSON objects suitable for use in PyMongo queries. The returned objects can be embedded in dicts in order to compose more complex queries.
"""

import re
from bson import SON
from .db import DB
from .config import Configs

# JMARC queries

def match_value(tag,code,val):
	
	"""Builds a query document for matching single subfield values. 
	
	parameters
	----------
	param1 : str
		The field tag to match.
	param2 : str / None
		The subfield code to match. Use None as the code if matching a controlfield value.
	param3 : str / Pattern
		Value to match against. Exact string or a compiled Pattern.
	
	returns
	-------
	bson.son.BSON 
	
	examples
	-------
	>>> query = match_value('269', 'a', '1999')
	>>> complex_query = { '$and' : [ match_value('008', None, re.compile('1999')), match_value('245', 'a', re.compile('United Nations')) ] }
	"""
	
	valtype = val.__class__.__name__
	
	auth_tag = __auth_controlled(tag,code)
	
	if auth_tag is not None:
		xrefs = __get_xrefs(auth_tag,code,val)
		
		return in_xrefs(tag,code,*xrefs)

	if valtype == 'str':
		return __exact_value(tag,code,val)
	elif valtype == 'Pattern':
		return __re_value(tag,code,val)
		
def __exact_value(tag,code,val):
	return SON (
		data = {
			tag + '.subfields' : {
				'code' : code,
				'value' : val
			}
		}
	)
	
def __re_value(tag,code,re):
	return SON (
		data = {
			tag + '.subfields' : {
				'$elemMatch' : {		
					'code' : code,
					'value' : re
				}
			}
		}
	)
	
def exact_xref(tag,code,xref):
	
	"""Builds a query document for matching a single subfield xref.
	
	parameters
	---------
	param1 : str
		The field tag to match.
	param2 : str
		The subfield code to match.
	param3 : int
		The xref to match.
	
	returns
	-------
	bson.son.BSON
	
	examples
	--------
	>>> query = exact_xref('650','a',268584)
	"""
	
	return SON (
		data = {
			tag + '.subfields' : {
				'code' : code,
				'xref' : int(xref)
			}
		}
	)

def in_xrefs(tag,code,*xrefs):
	
	"""Builds a query document for matching single subfield xrefs against a list of xrefs.
	
	parameters
	----------
	param1 : str
		The field tag to match.
	param2 : str
		The subfield code to match.
	*args : int
		List of xrefs to match against.
			
	returns
	-------
	bson.son.BSON
		
	examples
	--------
	>>> query = in_xrefs('650','a',268584,274431)
	"""
	
	return SON (
		data = {
			tag + '.subfields' : {
				'$elemMatch' : {
					'code' : code,
					'xref' : {
						'$in' : [*xrefs]
					}
				}
			}
		}
	)
	
def __get_xrefs(tag,code,val):
	cur = DB.auths.find(match_value(tag,code,val),{'_id':1})
	
	ret_vals = []
	
	for doc in cur:
		ret_vals.append(doc['_id'])
		
	return ret_vals
	
def __auth_controlled(tag,code):
	try:
		return Configs.authority_controlled[tag][code]
	except:
		return None


# File queries

def files_by_id(type,id):
		return SON (
		data = {
			'identifiers' : {
				'type' : type, 
				'value' : id 
			} 
		}
	)

def files_by_symbol(symbol):
	return files_by_id('symbol',symbol)
	
def file_by_id_lang(type,id,lang):
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
	
def file_by_symbol_lang(symbol,lang):
	return file_by_id_lang('symbol',symbol,lang)
