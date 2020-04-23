

import os, requests, hashlib
from datetime import datetime, timezone
from io import BytesIO
from tempfile import TemporaryFile, SpooledTemporaryFile
from bson import SON
from dlx import Config, DB
from dlx.file.s3 import S3

###

class FileExists(Exception):
    pass
    
class FileExistsIdentifierConflict(FileExists):
    def __init__(self, existing_identifiers):
        super().__init__('File already exists but with different identifiers: {}'.format(existing_identifiers))
        
        self.existing_identifiers = existing_identifiers
    
class FileExistsLanguageConflict(FileExists):
    def __init__(self, existing_languages):
        super().__init__('File already exists but with different languages: {}'.format(existing_languages))
        
        self.existing_languages = existing_languages
        
###
    
class Identifier(object):
    def __init__(self, identifier_type, value):
        self.type = identifier_type
        self.value = value

class File(object):    
    @classmethod    
    def import_from_path(cls, path, identifiers, filename, languages, mimetype, source):
        fh = open(path, 'rb')
        
        return cls.import_from_handle(fh, identifiers, filename, languages, mimetype, source)

    @classmethod
    def import_from_url(cls, url, identifiers, filename, languages, mimetype, source):
        hasher = hashlib.md5()
        fh = TemporaryFile('wb+')
        response = requests.get(url, stream=True)
        
        if response.status_code == 200:
            for chunk in response.iter_content(8192):
                fh.write(chunk)
                
            fh.seek(0)
            
            return cls.import_from_handle(fh, identifiers, filename, languages, mimetype, source)
        
    @classmethod
    def import_from_binary(cls, data, identifiers, filename, languages, mimetype, source):
        return cls.import_from_handle(BytesIO(data), identifiers, filename, languages, mimetype, source)
        
    @classmethod
    def import_from_handle(cls, handle, identifiers, filename, languages, mimetype, source):
        # handle can be any type of file-like object in binary mode
        
        hasher = hashlib.md5()
        
        while True:
            chunk = handle.read(8192)
            
            if chunk:
                hasher.update(chunk)
            else:
                break
                
        size = handle.tell()
        
        if size == 0:
            raise Exception('File-like object "{}" has no content'.format(handle))
        
        checksum = hasher.hexdigest()    
        File._check_file_exists(checksum, identifiers, languages)
        handle.seek(0)
        
        if S3.upload(handle, Config.files_bucket, checksum, mimetype):
            db_result = DB.files.insert_one(SON({
                '_id': checksum,
                'identifiers': [SON({'type': idx.type, 'value': idx.value}) for idx in identifiers],
                'filename': filename,
                'languages': languages,
                'mimetype': mimetype,
                'size': size,
                'source': source,
                'timestamp': datetime.now(timezone.utc),
                'uri': '{}.s3.amazonaws.com/{}'.format(Config.files_bucket, checksum),
            }))
                
            if db_result.acknowledged:
                return checksum
        
        return False

    @staticmethod
    def _check_file_exists(checksum, identifiers, languages):
        existing_record = DB.files.find_one({'_id': checksum})
        
        if existing_record:
            for idx in identifiers:
                raw_idx = {'type': idx.type, 'value': str(idx.value)}
                
                if raw_idx not in existing_record['identifiers']:
                    raise FileExistsIdentifierConflict(existing_record['identifiers'])
                    
            for raw_idx in existing_record['identifiers']:
                if {'type': raw_idx['type'], 'value': raw_idx['value']} not in [{'type': x.type, 'value': x.value} for x in identifiers]:
                    raise FileExistsIdentifierConflict(existing_record['identifiers'])
  
            for lang in languages:
                if sorted(languages) != existing_record['languages']:
                    raise FileExistsLanguageConflict(existing_record['languages'])
                    
            raise FileExists()
