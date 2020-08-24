import os, requests, hashlib
from datetime import datetime, timezone
from io import BytesIO
from json import dumps
from tempfile import TemporaryFile, SpooledTemporaryFile
import jsonschema
from bson import SON
from dlx import Config, DB
from dlx.util import ISO6391
from dlx.file.s3 import S3

class FileExists(Exception):
    def __init__(self, checksum, identifiers, languages, existing_record, message=None):
        self.checksum = checksum
        self.identifiers = identifiers
        self.languages = languages
        self.message = message or 'File {} - {} - {} already exists'.format(checksum, [x.to_str() for x in identifiers], languages)
        
        super().__init__(self, self.message)
        
class FileExistsConflict(FileExists):
    def __init__(self, checksum, identifiers, languages, existing_record):
        self.existing_identifiers = existing_record.identifiers
        self.existing_languages = existing_record.languages

        super().__init__(
            checksum,
            identifiers,
            languages,
            existing_record,
            'File {} - {} - {} already exists but with different {}: {}'.format(
                checksum,
                [x.to_str() for x in identifiers],
                languages,
                self.descriptor,
                [x.to_str() for x in existing_record.identifiers] if self.descriptor == 'identifiers' else existing_record.languages
            )
        )
    
class FileExistsIdentifierConflict(FileExistsConflict):
    def __init__(self, checksum, identifiers, languages, existing_record):
        self.descriptor = 'identifiers'
        
        super().__init__(checksum, identifiers, languages, existing_record)

class FileExistsLanguageConflict(FileExistsConflict):
    def __init__(self, checksum, identifiers, languages, existing_record):
        self.descriptor = 'languages'
        
        super().__init__(checksum, identifiers, languages, existing_record)
        
###
    
class Identifier(object):
    '''Class for standardizing content identifiers'''
    
    def __init__(self, identifier_type, value):
        self.type = identifier_type
        self.value = str(value)
        
    def __eq__(self, other):
        if self.type == other.type and self.value == other.value:
            return True
        else:
            return False
        
    def to_dict(self):
        return {'type': self.type, 'value': self.value}
        
    def to_str(self):
        return str({'type': self.type, 'value': self.value})

class File(object):    
    @classmethod    
    def import_from_path(cls, path, filename, identifiers, languages, mimetype, source, overwrite=False):
        fh = open(path, 'rb')
        
        return cls.import_from_handle(fh, filename, identifiers, languages, mimetype, source)

    @classmethod
    def import_from_url(cls, url, filename, identifiers, languages, mimetype, source, overwrite=False):
        hasher = hashlib.md5()
        fh = TemporaryFile('wb+')
        response = requests.get(url, stream=True)
        
        if response.status_code == 200:
            for chunk in response.iter_content(8192):
                fh.write(chunk)
                
            fh.seek(0)
            
            return cls.import_from_handle(fh, filename, identifiers, languages, mimetype, source, overwrite=False)
        
    @classmethod
    def import_from_binary(cls, data, filename, identifiers, languages, mimetype, source, overwrite=False):
        return cls.import_from_handle(BytesIO(data), filename, identifiers, languages, mimetype, source)
        
    @classmethod
    def import_from_handle(cls, handle, filename, identifiers, languages, mimetype, source, overwrite=False):
        '''Import a file using a file-like object. The file is 
        uploaded to the s3 bucket specified in `dlx.Config`. The
        metadata is stored in the database.
        
        All paramaters are required.
        
        Parameters
        ----------
        handle : Any file-like object
        filename : str
            The destination filename. Files with common identifiers, 
            languages, and filename are considered versions of each 
            other
        identifiers : list(dlx.file.Identifier)
        languages : list(str)
            The ISO 639-1 codes of the languages of the content.
            Codes will be stored in uppercase.
        mimetype : str
            Must be a value recognized by s3, otherwise the upload
            will fail
        source : str
            Name of the process that called the import for auditing
        overwrite : bool
            Ignore exisiting file exceptions and overwrite the data and file
        
        Returns
        -------
        If succesful, the md5 checksum as a hex string (also used as
        the database record ID) of the imported file, otherwise 
        `False`
        
        Raises
        ------
        FileExists : The file is already in the system
        FileExistsIdentifierConflict : The file is already in the
            system but with different identifiers
        FileExistsLanguageConflict : The file is already in the 
            system different languages
        '''
        
        ###
        
        if len(identifiers) == 0 or len(languages) == 0:
            raise ValueError('Params `identifiers` and `languages` cannot be an empty list')
        
        for idx in identifiers:
            if not isinstance(idx, Identifier):
                raise TypeError('Identifier must be of type `dlx.file.Identifier`')
                
        for lang in languages:
            lang = lang.upper()
            
            if lang.lower() not in ISO6391.codes:
                raise ValueError('Invalid ISO 639-1 language code')
        
        ###
        
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
        
        if overwrite == False:
            File._check_file_exists(checksum, identifiers, languages)

        handle.seek(0)
        
        if S3.upload(handle, checksum, mimetype):
            f = File(
                {
                    '_id': checksum,
                    'filename': filename,
                    'identifiers': [SON([('type', idx.type), ('value', idx.value)]) for idx in identifiers],
                    'languages': languages,
                    'mimetype': mimetype,
                    'size': size,
                    'source': source,
                    'timestamp': datetime.now(timezone.utc),
                    'uri': '{}.s3.amazonaws.com/{}'.format(S3.bucket, checksum),
                }
            )
            
            f.validate()
            data = f.to_bson()
             
            if overwrite == True:
                db_result = DB.files.replace_one({'_id': checksum}, data, upsert=True)
            else:
                db_result = DB.files.insert_one(data)
                    
            if db_result.acknowledged:
                return checksum
        
        return False

    @staticmethod
    def _check_file_exists(checksum, identifiers, languages):
        existing_record = File.find_one({'_id': checksum})
        
        if existing_record:
            for idx in identifiers:
                if not list(filter(lambda x: x == idx, existing_record.identifiers)):
                    raise FileExistsIdentifierConflict(checksum, identifiers, languages, existing_record)
            
            if sorted(languages) != sorted(existing_record.languages):
                raise FileExistsLanguageConflict(checksum, identifiers, languages, existing_record)
                    
            raise FileExists(checksum, identifiers, languages, existing_record)
            
    ###
    
    @classmethod
    def find(cls, query, *args, **kwargs):
        for doc in DB.files.find(query, *args, **kwargs):
            yield(File(doc))
            
    @classmethod
    def find_one(cls, query, *args, **kwargs):
        doc = DB.files.find_one(query, *args, **kwargs)
        
        if doc:
            return File(doc)
        
    @classmethod
    def from_id(cls, idx):
        return cls.find_one({'_id': idx})
    
    @classmethod
    def find_by_identifier(cls, identifier):
        assert isinstance(identifier, Identifier)
        
        return cls.find({'identifiers': {'type': identifier.type, 'value': identifier.value}})
        
    @classmethod
    def find_by_date(cls, date_from, date_to=None):
        assert isinstance(date_from, datetime)
        
        if date_to:
            assert isinstance(date_to, datetime)
        else:
            date_to = datetime.now(timezone.utc)    
        
        query = {
            '$or': [
                {
                    '$and': [
                        {'timestamp': {'$gte': date_from}},
                        {'timestamp': {'$lt': date_to}}
                    ]
                },
                {
                    '$and': [
                        {'updated': {'$gte': date_from}},
                        {'updated': {'$lt': date_to}}
                    ]
                },
            ]
        }
        
        return cls.find(query)
        
    ###
    
    def __init__(self, doc):
        for lang in doc['languages']:
            if lang.lower() not in ISO6391.codes:
                raise Exception('Invalid language code: {}'.format(lang))
        
        self.id = doc['_id']
        self.filename = doc['filename']
        self.identifiers = [Identifier(i['type'], i['value']) for i in doc['identifiers']]
        self.languages = doc['languages']
        self.mimetype = doc['mimetype']
        self.size = doc['size']
        self.source = doc['source']
        self.timestamp = doc['timestamp']
        self.uri = doc['uri']
        self.updated = doc.get('updated')
        
    @property
    def checksum(self):
        return self.id
        
    def validate(self):
        jsonschema.validate(instance=self.to_dict(), schema=Config.jfile_schema, format_checker=jsonschema.FormatChecker())
        
    def commit(self):
        self.updated = datetime.now(timezone.utc)
        self.validate()

        return DB.files.replace_one({'_id': self.id}, self.to_bson(), upsert=True)
        
    def to_bson(self):
        data = SON(
            [
                ('_id', self.id),
                ('filename', self.filename),
                ('identifiers', [SON([('type', idx.type), ('value', idx.value)]) for idx in self.identifiers]),
                ('languages', self.languages),
                ('mimetype', self.mimetype),
                ('size', self.size),
                ('source', self.source),
                ('timestamp', self.timestamp),
                ('uri', self.uri)
            ]
        )
        
        if self.updated:
            data['updated'] = self.updated
            
        return data 
    
    def to_dict(self):
        return self.to_bson().to_dict()
