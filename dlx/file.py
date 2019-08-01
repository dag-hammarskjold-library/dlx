
from dlx import DB
from dlx.query import jfile

def get_md5(path):
    return 1
    
class Identifier(object):
    def __init__(self,type,value):
        self.type = type
        self.value = value

class File(object): 
    @classmethod
    def match_id(cls,id):
        return File(DB.files.find_one({'_id':id}))
    
    @classmethod    
    def match_id_lang(cls,type,id,lang):
        for doc in DB.files.find(jfile.by_id_lang(type,id,lang)):
            yield File(doc)
            
    def __init__(self,doc={}):
        pass
    
    def exists(self):
        pass
    
    def commit(self):
        pass
    
class Import(object):
    def __init__(self,path,ids,langs):
        md5 = get_md5(path)    
        
        file = File(
            {
                '_id': md5, 
                'identifiers': [{'type': x.type, 'value': x.value} for x in ids], 
                'languages': langs
            }
        )
        
        # check if file exists
        
        if file.exists():
            # check if ids and langs clash
            pass
                #Y: exception
                #N: return
        else:
            # supercede if identifier and lang exists
            for id in ids:
                if not isinstance(id,Identifier): raise Exception
                for lang in langs:
                    for match in File.match_id_lang(id.type,id.value,lang):
                        print('superceding')
                        # supercede, return
                
            # copy to s3, insert jfile record to db, return
            print('importing')
    
