'''
'''

import string, json
from jsonschema import validate, exceptions as X
from pymongo import ASCENDING, DESCENDING
from bson import SON
from dlx.config import Configs
from dlx.db import DB
from dlx.query import jmarc as Q
from .subfield import Literal, Linked
from .field import Controlfield, Datafield

# decorator
def check_connection(f):
	def wrapper(*args,**kwargs):
		DB.check_connection()
		
		return f(*args,**kwargs)
	
	return wrapper
		
class MARC(object):
	_cache = {}
	
	## static 
	
	@staticmethod
	def lookup(xref,code):
		DB.check_connection()
		
		try:
			return MARC._cache[xref][code]
		except:
			auth = Auth.match_id(xref)
			
			if auth is None:
				value = 'N/A'
			else:	
				value = auth.header_value(code)
				
			if xref not in MARC._cache.keys():
				MARC._cache[xref] = {}
				
			MARC._cache[xref][code] = value
				
			return value
			
	@staticmethod
	def serialize_subfield(sub):
		if isinstance(sub,Linked):
			return {sub.code : MARC.lookup(sub.xref,sub.code)}
		else:
			return {sub.code : sub.value}
			
	@staticmethod
	def field_text(f):
		delim = u'\u001f'
		field_terminator = u'\u001e'
		text = ''

		if isinstance(f,Controlfield):
			text = f.value
		else:
			text += f.ind1 + f.ind2
			
			for sub in (f.subfields):
				if hasattr(sub,'value'):
					text += delim + sub.code + sub.value
				else:
					text += delim + sub.code + MARC.lookup(sub.xref,sub.code)
		
		text += field_terminator
		
		return text
	
	@staticmethod
	def validate(doc):
		try:
			validate(instance=doc,schema=Configs.jmarc_schema)
		except X.ValidationError as e:
			msg = '{} in {} : {}'.format(e.message, str(list(e.path)), json.dumps(doc,indent=4))
			raise X.ValidationError(msg) 
		
	#### database query handlers

	@classmethod
	def handle(cls):
		DB.check_connection()
		
		if cls.__name__ in ('Bib', 'JBIB'):
			col = 'bibs'
		elif cls.__name__ in ('Auth', 'JAUTH'):
			col = 'auths'
		else:
			raise Exception('Must call `handle()` from subclass `Bib` or `Auth`, or `JBIB` or `JAUTH` (deprecated)')
			
		return getattr(DB,col)
		
	@classmethod
	def match_id(cls,id):
		return cls.find_one({'_id' : id})
		
	@classmethod
	def match_ids(cls,*ids):
		return cls.find({'_id' : {'$in' : [*ids]}})
	
	@classmethod
	def match_value(cls,tag,code,val):
		"""Performs a query for a single MARC value in the database and returns a generator object which yields the results.
		Must be called from a subclass of `dlx.Marc` (`dlx.Bib` or `dlx.Auth`).
		
	
		Parameters
		----------
		tag : str
			The field tag to match.
		code : str / None
			The subfield code to match. Use `None` as the code if matching a controlfield value.
		val : str / Pattern
			Exact string value or compiled pattern to match. 
	
		Returns
		-------
		type.GeneratorType
			Yields instances of `dlx.Bib` or `dlx.Auth` depending on which subclass it was called on.   
	
		Examples
		-------
		>>> for bib in dlx.Bib.match_value('269', 'a', '1999'): print(bib.id)
		>>> for auth in dlx.Auth.match_value('100','a',re.compile('Hammarskjöld')): print(auth.get_value('100','a'))
		"""
		
		cursor = cls.handle().find(Q.match_value(tag,code,val))
		
		for doc in cursor:
			yield cls(doc)
	
	@classmethod
	def match_value_one(cls,tag,code,val):
		"""Performs a query for a single MARC value in the database and returns the first result.
		Must be called from a subclass of `dlx.Marc` (`dlx.Bib` or `dlx.Auth`).
		
	
		Parameters
		----------
		tag : str
			The field tag to match.
		code : str / None
			The subfield code to match. Use `None` as the code if matching a controlfield value.
		val : str / Pattern
			Exact string value or compiled pattern to match. 
	
		Returns
		-------
		dlx.Bib / dlx.Auth
			Depending on which subclass it was called on.   
	
		Examples
		-------
		>>> bib =  dlx.Bib.match_value_one('269', 'a', '1999')
		>>> auth = dlx.Auth.match_value_one('100','a',re.compile('Hammarskjöld'))
		"""
		
		return cls(cls.handle().find_one(Q.match_value(tag,code,val)))
	
	@classmethod	
	def match_values(cls,*tuples):
		"""Performs a query for a multiple MARC values in the database and returns a generator object which yields the results.
		Must be called from a subclass of `dlx.Marc` (`dlx.Bib` or `dlx.Auth`).
		
		The query will be a boolean `and` search.
	
		Parameters
		----------
		*tuples : tag [str], code [str], val [str / Pattern] 
			Accepts arbitrary number of tuples composed of the code and value to 
			match against. Value can be a str or Pattern.
	
			Use `None` as the subfield code if the field is a controlfield.
	
		Returns
		-------
		type.GeneratorType
			Yields instances of `dlx.Bib` or `dlx.Auth` depending on which subclass it was called on.   
	
		Examples
		-------
		>>> for bib in dlx.Bib.match_values(('269', 'a', '1999'), ('269','a','2000')): print(bib.get_value('269','a'))
		>>> for auth in dlx.Auth.match_values(('100','a',re.compile('Hammarskjöld')), ('100','a',re.compile('Dag'))): print(auth.get_value('100','a'))
		"""
		cursor = cls.handle().find(Q.and_values(*tuples))
		
		for doc in cursor:
			yield cls(doc)
			
	@classmethod	
	def match_values_or(cls,*tuples):
		"""Performs a query for a multiple MARC values in the database and returns a generator object which yields the results.
		Must be called from a subclass of `dlx.Marc` (`dlx.Bib` or `dlx.Auth`).
		
		The query will be a boolean `or` search.
	
		Parameters
		----------
		*tuples : tag [str], code [str], val [str / Pattern] 
			Accepts arbitrary number of tuples composed of the code and value to 
			match against. Value can be a str or Pattern.
	
			Use `None` as the subfield code if the field is a controlfield.
	
		Returns
		-------
		type.GeneratorType
			Yields instances of `dlx.Bib` or `dlx.Auth` depending on which subclass it was called on.   
	
		Examples
		-------
		>>> for bib in dlx.Bib.match_values(('269', 'a', '1999'), ('269','a','2000')): print(bib.get_value('269','a'))
		>>> for auth in dlx.Auth.match_values(('100','a',re.compile('Hammarskjöld')), ('100','a',re.compile('Dag'))): print(auth.get_value('100','a'))
		"""		
		
		cursor = cls.handle().find(Q.or_values(*tuples))
		
		for doc in cursor:
			yield cls(doc)
	
	@classmethod	
	def match_values_one(cls,*tuples):
		"""Performs a query for a multiple MARC values in the database and returns the first result.
		Must be called from a subclass of `dlx.Marc` (`dlx.Bib` or `dlx.Auth`).
		
		The query will be a boolean `and` search.
	
		Parameters
		----------
		*tuples : tag [str], code [str], val [str / Pattern] 
			Accepts arbitrary number of tuples composed of the code and value to 
			match against. Value can be a str or Pattern.
	
			Use `None` as the subfield code if the field is a controlfield.
	
		Returns
		-------
		dlx.Bib / dlx.Auth
			Depending on which subclass it was called on.   
	
		Examples
		-------
		>>> bib = dlx.Bib.match_values_one(('269', 'a', '1999'), ('269','a','2000'))
		>>> auth = dlx.Auth.match_values(('100','a',re.compile('Hammarskjöld')), ('100','a',re.compile('Dag')))
		"""

		return cls(cls.handle().find_one(Q.and_values(*tuples)))
	
	@classmethod
	def match_field(cls,tag,*tuples):
		"""Performs a query for a multiple subfield values in the database within the same MARC field.
		Must be called from a subclass of `dlx.Marc` (`dlx.Bib` or `dlx.Auth`).
		
		Parameters
		----------
		tag : str
			The field tag to match.
		*tuples : (code [str], val [str / Pattern])
			Accepts arbitrary number of tuples composed of the code and value to 
			match against. Value can be a str or Pattern.
		
			Use `None` as the subfield code if the field is a controlfield.
		
		Returns
		-------
		type.GeneratorType
			Yields instances of `dlx.Bib` or `dlx.Auth` depending on which subclass it was called on.
		
		Examples
		-------
		>>> for bib in match_field('191', ('b','A/'), ('c','73')): print(bib.get_value('191','a'))
		"""

		cursor = cls.handle().find(Q.match_field(tag,*tuples))
		
		for doc in cursor:
			yield cls(doc)
			
	@classmethod
	def match_field_one(cls,tag,*tuples):
		"""Performs a query for a multiple subfield values in the database within the same MARC field.
		Must be called from a subclass of `dlx.Marc` (`dlx.Bib` or `dlx.Auth`).
		
		Parameters
		----------
		tag : str
			The field tag to match.
		*tuples : (code [str], val [str / Pattern])
			Accepts arbitrary number of tuples composed of the code and value to 
			match against. Value can be a str or Pattern.
		
			Use `None` as the subfield code if the field is a controlfield.
		
		Returns
		-------
		dlx.Bib / dlx.Auth
			Depending on which subclass it was called on.
		
		Examples
		-------
		>>> bib = match_field('191', ('b','A/'), ('c','73'))
		"""
		
		return cls(cls.handle().find_one(Q.match_field(tag,*tuples)))
	
	@classmethod
	def match_fields(cls,*tuples):
		cursor = cls.handle().find(Q.and_fields(*tuples))

		for doc in cursor:
			yield cls(doc)
			
	@classmethod	
	def match_fields_or(cls,*tuples):
		cursor = cls.handle().find(Q.or_fields(*tuples))

		for doc in cursor:
			yield cls(doc)
	
	@classmethod	
	def match_fields_one(cls,*tuples):
		return next(cls.match_fields(*tuples),None)
		
	@classmethod	
	def match_xrefs(cls,tag,code,*xrefs):
		cursor = cls.handle().find(Q.match_xrefs(tag,code,*xrefs))
		
		for doc in cursor:
			yield cls(doc)
	
	@classmethod
	def find(cls,doc):
		cursor = cls.handle().find(doc)
		
		for doc in cursor:
			yield cls(doc)
	
	@classmethod
	def find_one(cls,doc):
		found = cls.handle().find_one(doc)
		
		if found is not None:
			return cls(found)
		
	#### database index creation
	
	@classmethod
	@check_connection
	def controlfield_index(cls,tag):
		cls.handle().create_index(tag)
	
	@classmethod
	def literal_index(cls,tag):
		field = tag + '.subfields'
		cls.handle().create_index(field)
		cls.handle().create_index([(field + '.code', ASCENDING), (field + '.value', ASCENDING)])
	
	@classmethod
	def linked_index(cls,tag):
		field = tag + '.subfields'
		cls.handle().create_index(field)
		cls.handle().create_index([(field + '.code', ASCENDING), (field + '.xref', ASCENDING)])
	
	@classmethod
	def hybrid_index(cls,tag):
		field = tag + '.subfields'
		cls.handle().create_index(field)
		cls.handle().create_index([(field + '.code', ASCENDING), (field + '.value', ASCENDING), (field + '.xref', ASCENDING)])
	
	## instance 
	
	def __init__(self,doc={}):
		self.controlfields = []
		self.datafields = []
		
		if doc is None: 
			doc = {}
		else:
			MARC.validate(doc)
		
		if '_id' in doc.keys():
			self.id = int(doc['_id'])
		
		for tag in filter(lambda x: False if x == '_id' else True, doc.keys()):
			
			if tag == '000':
				self.leader = doc['000'][0]
				
			if tag[:2] == '00':
				for value in doc[tag]:
					self.controlfields.append(Controlfield(tag,value))
			else:
				for field in doc[tag]:
					ind1 = field['indicators'][0]
					ind2 = field['indicators'][1]
					subfields = []
					
					for sub in field['subfields']:
						if 'value' in sub.keys():
							subfields.append(Literal(sub['code'], sub['value']))	
						elif 'xref' in sub.keys():
							subfields.append(Linked(sub['code'], sub['xref']))
						
					self.datafields.append(Datafield(tag,ind1,ind2,subfields))
	
	#### "get"-type methods
	
	def get_fields(self,*tags):
		if len(tags) == 0:
			return self.controlfields + self.datafields
			
		return filter(lambda x: True if x.tag in tags else False, self.controlfields + self.datafields)
		
		#todo: return sorted by tag
			
	def get_field(self,tag):
		return next(self.get_fields(tag), None)
			
	def get_values(self,tag,*codes):
		if len(codes) == 0:
			codes = list(string.ascii_lowercase + string.digits)
			
		vals = []
					
		for field in self.get_fields(tag):
			if isinstance(field,Controlfield):
				return [field.value]
				
			for sub in filter(lambda sub: sub.code in codes, field.subfields):
				if isinstance(sub,Literal):
					vals.append(sub.value)
				elif isinstance(sub,Linked):
					vals.append(MARC.lookup(sub.xref,sub.code))
					
		return vals
	
	def get_value(self,tag,code=None):
		#return next(self.get_values(tag,code), None)
		try: 
			return self.get_values(tag,code)[0]
		except:
			return None
	
	def get_tags(self):
		return sorted([x.tag for x in self.get_fields()])
		
	def get_xrefs(self,*tags):
		xrefs = []
		
		for f in self.datafields:
			xrefs = xrefs + f.xrefs()
		
		return sorted(xrefs)
		
	#### "set"-type methods
	
	def set_value(self,match_tag,match_code,match_val,new_val):
		pass
	
	def change_tag(self,old_tag,new_tag):
		pass
		
	def delete_tag(self,tag):
		pass
		
	### store
	
	def commit(self):
		# clear the cache so the new value is available
		if isinstance(self,Auth): MARC._cache = {}
		
		MARC.validate(self.to_dict())
		
		# upsert (replace if exists, else new)
		return self.collection().replace_one({'_id' : int(self.id)}, self.to_bson(), True)
	
	#### utlities 
		
	def collection(self):
		if isinstance(self,Bib):
			return DB.bibs
		elif isinstance(self,Auth):
			return DB.auths
	
	def check(self,tag,val):
		pass
	
	def diff(self,marc):
		pass
	
	#### serializations
	
	def to_bson(self):
		bson = SON()
		bson['_id'] = int(self.id)
		
		for tag in self.get_tags():
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
						'subfields' : [MARC.serialize_subfield(sub) for sub in f.subfields],
						'ind1' : f.ind1,
						'ind2' : f.ind2,
					}
				}
			)
		
		mij['fields'] = fields
		
		return json.dumps(mij)
		
	def to_mrc(self):
		directory = ''
		data = ''
		next_start = 0
		field_terminator = u'\u001e'
		record_terminator = u'\u001d'
		
		for f in filter(lambda x: x.tag != '000', self.get_fields()):
			text = MARC.field_text(f)
			data += text
			field_length = len(text.encode('utf-8'))
			directory += f.tag + str(field_length).zfill(4) + str(next_start).zfill(5)
			next_start += field_length
			
		directory += field_terminator
		data += record_terminator
		leader_dir_len = len(directory.encode('utf-8')) + 24 
		base_address = str(leader_dir_len).zfill(5)
		total_len = str(leader_dir_len + len(data.encode('utf-8'))).zfill(5)
		
		new_leader = total_len \
			+ self.leader[5:9] \
			+ 'a' \
			+ self.leader[10:11] \
			+ base_address \
			+ self.leader[17:21] \
			+ '4500'

		return new_leader + directory + data
	
	def to_mrk(self):
		pass	
		
	def to_xml(self):
		pass
		
	#### de-serializations
	# these formats don't fully support linked values.
	
	# todo: data coming from these formats should be somehow flagged as 
	# "externally sourced" and not committed to the DB without revision.
	#
	# alternatively, we can try to match string values from known DLX auth-
	# controlled fields with DLX authority strings and automatically assign
	# the xref (basically, the Horizon approach)
		
	def from_mij(self,string):
		pass
		
	def from_mrc(self,string):
		pass
		
	def from_mrk(self,string):
		pass
		
	def from_xml(self,string):
		pass

### deprecated class name (now a subclass of dlx.MARC)
class JMARC(MARC):
	def __init__(self,doc={}):
		super().__init__(doc)


class Bib(MARC):
	def __init__(self,doc={}):
		super().__init__(doc)
		
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
	
### deprecated class name (now a subclass of dlx.Bib)
class JBIB(Bib):
	def __init__(self,doc={}):
		super().__init__(doc)
		
					
class Auth(MARC):
	def __init__(self,doc={}):
		super().__init__(doc)
		
		self.header = next(filter(lambda field: field.tag[0:1] == '1', self.get_fields()), None)
		
	def header_value(self,code):
		if self.header is None:
			return
			
		for sub in filter(lambda sub: sub.code == code, self.header.subfields):
			return sub.value
			
### deprecated class name (now a subclass of dlx.Bib)
class JAUTH(Auth):
	def __init__(self,doc={}):
		super().__init__(doc)

