from dlx import DB
from dlx.marc.query import QueryDocument

class MARCSet(object):
    # constructors
    
    @classmethod
    def from_query(cls,*args,**kwargs):
        if isinstance(args[0],QueryDocument):
            query = args[0].compile()
            args = [query]
        
        self = cls()
        self.records = self.handle.find(*args,**kwargs)
        
        return self
    
    @classmethod    
    def from_dataframe(cls,df):
        pass
    
    def __init__(self):
        self.records = None # can be any type iterable
        
    def cache(self):
        self.records = list(self.records)
        return self

    # serializations
    
    def to_mrc(self):
        str = ''
        
        for record in self.records:
            str += record.to_mrc()
            
        return str
    
    def to_xml(self):
        xml = ''
        
        for record in self.records:
            str += record.to_xml()
            
        return xml
    
class BibSet(MARCSet):
    def __init__(self):
        self.handle = DB.bibs
        super().__init__()
        
class AuthSet(MARCSet):
    def __init__(self):
        self.handle = DB.auths
        super().__init__()
