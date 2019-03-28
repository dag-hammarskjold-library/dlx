'''

'''

import re
from bson import SON
from .db import DB
from .config import Configs

# JMARC queries

def match_value(tag,code,val):
	valtype = val.__class__.__name__
	
	auth_tag = auth_controlled(tag,code)
	
	if auth_tag is not None:
		xrefs = get_xrefs(auth_tag,code,val)
		
		return in_xrefs(tag,code,*xrefs)

	if valtype == 'str':
		return exact_value(tag,code,val)
	elif valtype == 'Pattern':
		return re_value(tag,code,val)
		
def exact_value(tag,code,val):
	return SON (
		data = {
			tag + '.subfields' : {
				'code' : code,
				'value' : val
			}
		}
	)
	
def re_value(tag,code,re):
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
	return SON (
		data = {
			tag + '.subfields' : {
				'code' : code,
				'xref' : int(xref)
			}
		}
	)

def in_xrefs(tag,code,*xrefs):
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
	
def get_xrefs(tag,code,val):
	cur = DB.auths.find(match_value(tag,code,val),{'_id':1})
	
	ret_vals = []
	
	for doc in cur:
		ret_vals.append(doc['_id'])
		
	return ret_vals
	
def auth_controlled(tag,code):
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
