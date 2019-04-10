'''
'''

from bson import SON

class Subfield(object):
	def __init__(self):
		raise Exception('Cannot instantiate fom base class')
		
	def to_bson(self):
		raise Exception('This is a stub')
		
	@classmethod
	def is_linked(cls):
		if cls.__name__ == 'Linked':
			return True
		else:
			return False
	
class Literal(Subfield):
	def __init__(self,code,value):
		self.code = code
		self.value = value
		
	def to_bson(self):
		return SON(data = {'code' : self.code, 'value' : self.value})
	
class Linked(Subfield):	
	def __init__(self,code,xref):
		self.code = code
		self.xref = int(xref)
		
	def to_bson(self):
		return SON(data = {'code' : self.xref, 'xref' : self.xref})