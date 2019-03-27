'''

'''

import json
from bson import SON
from .db import DB
from .query import *

### subfield classes

class Subfield(object):
	def __init__(self):
		pass
		
	def to_bson(self):
		dict = self.__dict__
		bson = SON()
		
		bson['code'] = self.code
		
		if self.__class__.__name__ == 'Literal':
			bson['value'] = self.value
		elif self.__class__.__name__ == 'Linked':
			bson['xref'] = self.xref
			
		return bson

class Literal(Subfield):
	def __init__(self,code,value):
		self.code = code
		self.value = value
	
class Linked(Subfield):	
	_cache = {}
	
	def __init__(self,code,xref):
		self.code = code
		self.xref = int(xref)
		self._value = None
	
	@property
	def value(self):
		try:
			self._value = Linked._cache[self.xref][self.code]
			return self._value
		except:
			pass
			
		if self._value is None:
			doc = DB.auths.find_one({'_id' : self.xref})
			auth = JAUTH(doc)
			
			self._value = auth.header.get_value(self.code)
			
			Linked._cache[self.xref] = {}
			Linked._cache[self.xref][self.code] = self._value
		
		return self._value

### field classes

class Controlfield(object):
	def __init__(self,tag,value):
		self.tag = tag
		self.value = value
	
	def to_bson(self):
		return self.value
	
class Datafield(object):
	def __init__(self,tag,ind1,ind2,subfields):
		self.tag = tag
		self.ind1 = ind1
		self.ind2 = ind2
		self.subfields = subfields
	
	def get_value(self,code):
		for sub in self.subfields:
			if sub.code == code:
				return sub.value
		
		return None

	def get_values(self,*codes):
		ret_vals = []
		
		for sub in self.subfields:
			if sub.code in codes:
				ret_vals.append(sub.value)
		
		return ret_vals
		
	def to_bson(self):
		return SON (
			data = {
				'indicators' : [self.ind1, self.ind2],
				'subfields' : [sub.to_bson() for sub in self.subfields]
			}
		)

### record classes
		
class JMARC(object):
	
	def __init__(self,dict={}):
		self.controlfields = []
		self.datafields = []
		
		for tag in filter(lambda x: False if x == '_id' else True, dict.keys()):
			if tag == '000':
				self.leader = dict['000'][0]
				
			if tag[:2] == '00':
				for value in dict[tag]:
					self.controlfields.append(Controlfield(tag,value))
			else:
				for field in dict[tag]:
					ind1 = field['indicators'][0]
					ind2 = field['indicators'][1]
					subfields = []
					
					for sub in field['subfields']:
						if 'value' in sub.keys():
							subfields.append(Literal(sub['code'], sub['value']))	
						elif 'xref' in sub.keys():
							subfields.append(Linked(sub['code'], sub['xref']))
						
					self.datafields.append(Datafield(tag,ind1,ind2,subfields))
	
	def get_fields(self,tag=None):
		if tag is None:
			return self.controlfields + self.datafields
			
		return filter(lambda x: True if x.tag == tag else False, self.controlfields + self.datafields)
							
	def get_field(self,tag):
		return next(self.get_fields(tag), None)

	def get_value(self,tag,code = None):
		# returns the first value found
		
		field = self.get_field(tag)
		
		if field is None:
			return None
		
		if field.__class__.__name__ == 'Controlfield':
			return field.value
		
		return field.get_value(code)

	def get_values(self,tag,*codes):
		# returns list of values
		
		ret_vals = []
		
		for field in self.get_fields(tag):
			vals = field.get_values(*codes)
			
			if not vals:
				pass
			else:
				ret_vals.append(field.get_values(*codes))
		
		# this is the only way to flatten a list in python?? ðŸ˜•
		return [x for y in ret_vals for x in y]
		
	def tags(self):
		# trying list comprehension instead of map
		return sorted([x.tag for x in self.get_fields()])

	
	# utlities 
	
	def diff(self,jmarc):
		pass
	
	# serializations
	
	def to_bson(self):
		bson = SON()
		
		for tag in self.tags():
			bson[tag] = [field.to_bson() for field in self.get_fields(tag)]
		
		return bson
	
	def to_dict(self):
		return self.to_bson().to_dict()
		
	def to_json(self):
		return json.dumps(self.to_dict())
	
	def to_mij(self):
		mij = {}
		mij['leader'] = self.leader	
		fields = []
		
		for f in self.controlfields:
			fields.append({f.tag : f.value})
		
		for f in self.datafields:
			fields.append(
				{
					f.tag : {
						'subfields' : list(map(lambda x: {x.code : x.value}, f.subfields)),
						'ind1' : f.ind1,
						'ind2' : f.ind2,
					}
				}
			)
		
		mij['fields'] = fields
		
		return json.dumps(mij)

class JBIB(JMARC):
	@staticmethod
	def find_id(id):
		return JBIB(DB.bibs.find_one({'_id' : id}))
	
	@staticmethod
	def find_value(tag,code,val):
		return JBIB(DB.bibs.find_one(match(tag,code,val)))
	
	@staticmethod
	def find_values(tag,code,val):
		cursor = DB.bibs.find(match(tag,code,val))
		
		for dict in cursor:
			yield JBIB(dict)
	
	@staticmethod
	def query_one(doc):
		return JBIB(DB.bibs.find_one(doc))
		
	@staticmethod
	def query(doc):
		cursor = DB.bibs.find(doc)
		
		for dict in cursor:
			yield JBIB(dict)
	
	# constructor
	
	def __init__(self,dict={}):
		super().__init__(dict)
		
	# shorctuts
	
	def symbol(self):
		return self.get_value('191','a')
		
	def symbols(self):
		return self.get_values('191','a')
		
	def title(self):
		return ' '.join(self.get_values('245','a','b','c'))
	
	def date(self):
		return self.get_value('269','a')
		
	# files 
		
	def files(self,*langs):
		symbol = self.symbol()
		cursor = DB.files.find(files_by_symbol(symbol))
		
		ret_vals = []
		
		for doc in cursor:
			for lang in langs:
				if lang in doc['languages']:
					ret_vals.append(doc['uri'])
			
		return ret_vals
	
	def file(self,lang):
		symbol = self.symbol()
		
		try:
			return DB.files.find_one(file_by_symbol_lang(symbol,lang))['uri']
		except:
			return ''
		
class JAUTH(JMARC):
	@staticmethod
	def find_id(id):
		return JAUTH(DB.auths.find_one({'_id' : id}))
		
	@staticmethod
	def find_value(tag,code,val):
		return JAUTH(DB.auths.find_one(match(tag,code,val)))
	
	@staticmethod
	def find_values(tag,code,val):
		cursor = DB.auths.find(match(tag,code,val))
		
		for dict in cursor:
			yield JAUTH(dict)
	
	def __init__(self,dict={}):
		super().__init__(dict)
		
		self.header = next(filter(lambda field: field.tag[0:1] == '1', self.get_fields()))

	
