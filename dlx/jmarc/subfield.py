'''
'''

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
	def __init__(self,code,xref):
		self.code = code
		self.xref = int(xref)