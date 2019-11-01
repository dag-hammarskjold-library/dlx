
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
            
        # upload to s3 
        # commit to db insert jfile record to db, return

        # mark old version superceded if identifier and lang exists
        for id in ids:    
            for lang in langs:
                for match in File.match_id_lang(id.type,id.value,lang):
                    if not match.superceded_by:
                        print('superceded ' + match.uri)
                        match.superceded_by = incoming.id
                     
        return 
    
    def __init__(self,doc={}):
        self.id = doc['id']
        for idx in doc['identifiers']:
            if not isinstance(idx,Identifier):
                raise Exception
        self.identifiers = doc['identifiers']
        self.languages = doc['languages']

    def exists(self):
        pass
    
    def commit(self):
        pass

class S3(object):
    pass
    
### exceptions

class FileExists(Exception):
    pass
