'''
'''

import string, json
from bson import SON
from dlx.db import DB
from dlx.query import jmarc as Q
from .subfield import Literal, Linked
from .field import Controlfield, Datafield
		
class JMARC(object):
	_cache = {}
	
	## static 
	
	@staticmethod
	def lookup(xref,code):
		try:
			return JMARC._cache[xref][code]
		except:
			auth = JAUTH.match_id(xref)
			value = auth.header_value(code)
			
			if xref not in JMARC._cache.keys():
				JMARC._cache[xref] = {}
				
			JMARC._cache[xref][code] = value
				
			return value
		
	## class
		
	#### database query handlers
	
	# decorator
	def check_connection(f):
		def wrapper(*args):
			DB.check_connection()
			
			return f(*args)
		
		return wrapper

	@classmethod
	@check_connection
	def handle(cls):
		if cls.__name__ == 'JBIB':
			col = 'bibs'
		elif cls.__name__ == 'JAUTH':
			col = 'auths'
		else:
			raise Exception('Must call `handle()` from JBIB or JAUTH')
			
		return getattr(DB,col)
		
	@classmethod
	@check_connection
	def match_id(cls,id):
		return cls.find_one({'_id' : id})
		
	@classmethod
	@check_connection
	def match_ids(cls,*ids):
		return cls.find({'_id' : {'$in' : [*ids]}})
	
	@classmethod
	@check_connection
	def match_value(cls,tag,code,val):
		cursor = cls.handle().find(Q.match_value(tag,code,val))
		
		for dict in cursor:
			yield cls(dict)
	
	@classmethod
	@check_connection
	def match_value_one(cls,tag,code,val):
		return cls(cls.handle().find_one(Q.match_value(tag,code,val)))
	
	@classmethod	
	@check_connection
	def match_values(cls,*tuples):
		cursor = cls.handle().find(Q.and_values(*tuples))
		
		for dict in cursor:
			yield cls(dict)
			
	@classmethod	
	@check_connection
	def match_values_or(cls,*tuples):
		cursor = cls.handle().find(Q.or_values(*tuples))
		
		for dict in cursor:
			yield cls(dict)
	
	@classmethod	
	@check_connection
	def match_values_one(cls,*tuples):
		return cls.handle().find_one(Q.and_values(*tuples))
	
	@classmethod
	@check_connection
	def match_field(cls,tag,*tuples):
		cursor = cls.handle().find(Q.match_field(tag,*tuples))
		
		for dict in cursor:
			yield cls(dict)
			
	@classmethod
	@check_connection	
	def match_field_one(cls,tag,*tuples):
		return cls.handle().find_one(Q.match_field(tag,*tuples))
		
	@classmethod
	@check_connection	
	def match_xrefs(cls,tag,*xrefs):
		cursor = cls.handle().find(Q.match_xrefs(tag,*xrefs))
		
		for dict in cursor:
			yield cls(dict)
	
	@classmethod
	@check_connection
	def find(cls,doc):
		cursor = cls.handle().find(doc)
		
		for dict in cursor:
			yield cls(dict)
	
	@classmethod
	@check_connection
	def find_one(cls,doc):
		return cls(cls.handle().find_one(doc))
		
	#### database index creation
	
	@classmethod
	@check_connection
	def controlfield_index(cls,tag):
		cls.handle().create_index({tag : 1})
	
	@classmethod
	@check_connection
	def literal_index(cls,tag):
		field = tag + '.subfields'
		cls.handle().create_index({field : 1})
		cls.handle().create_index({field + '.code' : 1, field + '.value' : 1})
	
	@classmethod
	@check_connection
	def linked_index(cls,tag):
		field = tag + '.subfields'
		cls.handle().create_index({field : 1})
		cls.handle().create_index({field + '.code' : 1, field + '.xref' : 1})
	
	@classmethod
	@check_connection
	def hybrid_index(cls,tag):
		field = tag + '.subfields'
		cls.handle().create_index({field : 1})
		cls.handle().create_index({field + '.code' : 1, field + '.value' : 1, field + '.xref' : 1})
	
	## instance 
	
	def __init__(self,dict={}):
		self.controlfields = []
		self.datafields = []
		
		if '_id' in dict.keys():
			self.id = str(dict['_id'])
		
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
	
	def get_fields(self,*tags):
		if len(tags) == 0:
			return self.controlfields + self.datafields
			
		return filter(lambda x: True if x.tag in tags else False, self.controlfields + self.datafields)
							
	def get_field(self,tag):
		return next(self.get_fields(tag), None)
			
	def get_values(self,tag,*codes):
		if len(codes) == 0:
			codes = list(string.ascii_lowercase + string.digits)
					
		for field in self.get_fields(tag):
			if field.__class__.__name__ == 'Controlfield':
				yield field.value
				raise StopIteration
			
			for sub in filter(lambda sub: sub.code in codes, field.subfields):
				if sub.__class__.__name__ == 'Literal':
					yield sub.value
				elif sub.__class__.__name__ == 'Linked':
					yield JMARC.lookup(sub.xref,sub.code)
	
	def get_value(self,tag,code=None):
		return next(self.get_values(tag,code), None)
	
	def tags(self):
		return sorted([x.tag for x in self.get_fields()])
		
	def xrefs(self,*tags):
		ret_vals = []
		
		for f in self.datafields:
			ret_vals = ret_vals + f.xrefs()
		
		return ret_vals

	#### utlities 
	
	def diff(self,jmarc):
		pass
	
	#### serializations
	
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
	def __init__(self,dict={}):
		super().__init__(dict)
		
	#### shorctuts
	
	def symbol(self):
		return self.get_value('191','a')
		
	def symbols(self):
		return self.get_values('191','a')
		
	def title(self):
		return ' '.join(self.get_values('245','a','b','c'))
	
	def date(self):
		return self.get_value('269','a')
		
	#### files 
		
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
	def __init__(self,dict={}):
		super().__init__(dict)
		
		self.header = next(filter(lambda field: field.tag[0:1] == '1', self.get_fields()))
		
	def header_value(self,code):
		for sub in filter(lambda sub: sub.code == code, self.header.subfields):
			return sub.value
