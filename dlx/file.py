
from dlx import DB
from dlx.query import jfile

def get_md5(path):
    return 1
    
class Identifier(object):
    def __init__(self,type,value):
        self.type = type
        self.value = value

class File(object):
    '''Interface to the DLX `db.files` collection ans S3 filestore.'''
    
    @classmethod
    def match_id(cls,id):
        return File(DB.files.find_one({'_id':id}))
    
    @classmethod    
    def match_id_lang(cls,type,id,lang):
        for doc in DB.files.find(jfile.by_id_lang(type,id,lang)):
            yield File(doc)
    
    @classmethod
    def ingest(self,path,ids,langs):
        md5 = get_md5(path)    
        
        incoming = File(
            {
                '_id': md5, 
                'identifiers': [{'type': x.type, 'value': x.value} for x in ids], 
                'languages': langs
            }
        )
        
        # check if incoming exists
        
        if incoming.exists():
            raise FileExists('File already exists.')

        # mark old version superceded if identifier and lang exists
        for id in ids:
            if not isinstance(id,Identifier): 
                raise Exception
            
            for lang in langs:
                for match in File.match_id_lang(id.type,id.value,lang):
                    if match.superceded_by:
                        continue
                        
                    print('superceding ' + match.uri)
                    match.superceded_by = incoming.id                         
                    
        # upload to s3 
        # commit to db insert jfile record to db, return
        # print('importing')
    
    def __init__(self,doc={}):
        pass
    
    def exists(self):
        pass
    
    def commit(self):
        pass

class S3(object):
    pass
    
### excetpions

class FileExists(Exception):
    pass
