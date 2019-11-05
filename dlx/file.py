'''
'''

import hashlib, json
from bson import SON
from dlx import DB
from dlx.query import jfile
    
class Identifier(object):
    def __init__(self,type,value):
        self.type = type
        self.value = value

class File(object):
    '''Interface to the DLX `db.files` collection and S3 filestore.'''
    
    @classmethod
    def match_id(cls,id):
        return File(DB.files.find_one({'_id':id}))
    
    @classmethod    
    def match_id_lang(cls,type,id,lang):
        for doc in DB.files.find(jfile.by_id_lang(type,id,lang)):
            yield File(doc)
    
    @classmethod
    def ingest(cls,path,ids,langs):
        incoming = File(
            {
                '_id': get_md5(path), 
                'identifiers': [{'type': idx.type, 'value': idx.value} for idx in ids],
                'languages': langs
            }
        )
        
        if incoming.exists():
            raise FileExists('File already exists.')
            
        # upload to s3
        incoming.uri = 'www.file.com'         
        # commit to db 
        incoming.commit()
        
        for id in ids:    
            for lang in langs:
                for match in File.match_id_lang(id.type,id.value,lang):
                    if match.id == incoming.id:
                        continue
                        
                    if not match.superceded_by:
                        match.supercede(incoming.id)
                        match.commit()

        return incoming
    
    def __init__(self,doc={}):
        if len(doc) > 1:
            self.id = doc['_id']
            
            self.identifiers = []
            for idx in doc['identifiers']:
                self.identifiers.append({'type': idx['type'], 'value': idx['value']})
                        
            fields = ('uri','size','mimetype','source','timestamp','languages','superceded_by')
            
            for field in fields:
                if field in doc.keys():
                    setattr(self,field,doc[field])
                else:
                    setattr(self,field,None)
                
    def exists(self):
        if DB.files.find_one(jfile.by_md5(self.id)):
            return True
        else: 
            return False
            
    def upload(self):
        pass
    
    def commit(self):
        DB.files.replace_one({'_id': self.id}, self.to_bson(), True)
        
    def supercede(self,superceding_md5):
        self.superceded_by = superceding_md5
        self.commit()
        
    def to_bson(self):
        return SON(
            data = {
               '_id': self.id,
               'identifiers': [ {'type': idx['type'], 'value': idx['value']} for idx in self.identifiers ],
               'languages': self.languages,
               'mimetype': self.mimetype,
               'size': self.size,
               'source': self.source,
               'superceded_by': self.superceded_by,
               'timestamp': self.timestamp,
               'uri': self.uri,
            }
        )

class S3(object):
    pass
    
### exceptions

class FileExists(Exception):
    pass
    
### util

def get_md5(path):
    with open(path,'rb') as fh:
        checksum = hashlib.md5()
        
        while True:
            data = fh.read(8192)
            
            if data: 
                checksum.update(data)
            else:
                break
                
        return checksum.hexdigest()
