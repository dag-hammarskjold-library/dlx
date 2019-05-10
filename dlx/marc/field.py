'''
'''

from bson import SON

class Field(object):
    def __init__(self):
        raise Exception('Cannot instantiate fom base class')
        
    def to_bson(self):
        raise Exception('This is a stub')
        
class Controlfield(Field):
    def __init__(self,tag,value):
        self.tag = tag
        self.value = value
    
    def to_bson(self):
        return self.value
    
class Datafield(Field):
    def __init__(self,tag,ind1,ind2,subfields):
        self.tag = tag
        self.ind1 = ind1
        self.ind2 = ind2
        self.subfields = subfields
        
    def xrefs(self):
        return [sub.xref for sub in filter(lambda x: hasattr(x,'xref'), self.subfields)]
            
    def to_bson(self):
        return SON (
            data = {
                'indicators' : [self.ind1, self.ind2],
                'subfields' : [sub.to_bson() for sub in self.subfields]
            }
        )