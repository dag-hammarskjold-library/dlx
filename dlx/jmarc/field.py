'''
'''

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
			
	def to_bson(self):
		return SON (
			data = {
				'indicators' : [self.ind1, self.ind2],
				'subfields' : [sub.to_bson() for sub in self.subfields]
			}
		)