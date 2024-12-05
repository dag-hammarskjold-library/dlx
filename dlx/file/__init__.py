import os, requests, hashlib
from warnings import warn 
from datetime import datetime, timezone
from io import BytesIO
from json import dumps
from tempfile import TemporaryFile, SpooledTemporaryFile
import jsonschema
from bson import SON
from pymongo import ASCENDING as ASC, DESCENDING as DESC
from pymongo.collation import Collation
from dlx import Config, DB
from dlx.util import ISO6391
from dlx.file.s3 import S3

###

class FileExists(Exception):
    def __init__(self, message=None):
        super().__init__(message or 'File already exists')
    
class FileExistsConflict(FileExists):
    def __init__(self):
        self.message = 'File {} already exists but with different {}: {} - {}'.format(
            self.checksum,
            self.desc,
            [x.to_str() for x in self.existing_identifiers],
            self.existing_languages
        )
        
        super().__init__(self.message)
    
class FileExistsIdentifierConflict(FileExistsConflict):    
    def __init__(self, checksum, existing_identifiers, existing_languages):
        self.checksum = checksum
        self.desc = 'identifiers'
        self.existing_identifiers = existing_identifiers
        self.existing_languages = existing_languages
        
        super().__init__()

class FileExistsLanguageConflict(FileExistsConflict):    
    def __init__(self, checksum, existing_identifiers, existing_languages):
        self.checksum = checksum
        self.desc = 'languages'
        self.existing_identifiers = existing_identifiers
        self.existing_languages = existing_languages
        
        super().__init__()

###
    
class Identifier(object):
    '''Class for standardizing content identifiers'''
    
    def __init__(self, identifier_type, value):
        self.type = identifier_type
        self.value = str(value)
        
    def __eq__(self, other):
        if self.type == other.type and self.value.lower() == other.value.lower():
            return True
        else:
            return False
        
    def to_dict(self):
        return {'type': self.type, 'value': self.value}
        
    def to_str(self):
        return str({'type': self.type, 'value': self.value})

class File(object):
    @classmethod    
    def import_from_path(cls, path, *, identifiers, languages, mimetype, source, filename=None, overwrite=False, user=None):
        fh = open(path, 'rb')
        
        return cls.import_from_handle(
            fh, 
            filename=filename, 
            identifiers=identifiers, 
            languages=languages, 
            mimetype=mimetype, 
            source=source,
            overwrite=overwrite,
            user=None
        )

    @classmethod
    def import_from_url(cls, url, *, identifiers, languages, mimetype, source, filename=None, overwrite=False, user=None):
        hasher = hashlib.md5()
        fh = TemporaryFile('wb+')
        response = requests.get(url, stream=True)
        
        if response.status_code == 200:
            for chunk in response.iter_content(8192):
                fh.write(chunk)
                
            fh.seek(0)
            
        return cls.import_from_handle(
            fh, 
            filename=filename, 
            identifiers=identifiers, 
            languages=languages, 
            mimetype=mimetype, 
            source=source,
            overwrite=overwrite,
            user=None
        )
        
    @classmethod
    def import_from_binary(cls, data, *, identifiers, languages, mimetype, source, filename=None, overwrite=False, user=None):
        return cls.import_from_handle(
            BytesIO(data), 
            filename=filename, 
            identifiers=identifiers, 
            languages=languages, 
            mimetype=mimetype, 
            source=source,
            overwrite=overwrite,
            user=user
    )
        
    @classmethod
    def import_from_handle(cls, handle, *, identifiers, languages, mimetype, source, filename=None, overwrite=False, user=None):
        '''Import a file using a file-like object (handle). The file is uploaded to 
        the s3 bucket specified in `dlx.Config`. The metadata is stored in the
        database
        
        
        Positional arguments
        --------------------
        handle : Any file-like object (required)
        
        Keyword arguments
        ------------------
        identifiers : list(dlx.file.Identifier)
        languages : list(str)
            The ISO 639-1 code(s) of the content language(s)
        mimetype : str
            Must be a value recognized by s3, otherwise the upload will fail
        source : str
            Name of the process that called the import
        filename : str
            Filename recomended for use on download (optional)
        overwrite : bool
            Ignore exisiting file exceptions and overwrite the data and file
        
        Returns
        -------
        If succesful, the md5 checksum as a hex string, which is also used as
        the database record ID of the imported file. If unsuccesful, an
        exception is thrown
        
        Raises
        ------
        Imports are subject to all relevant Pymongo and Boto3 exeptions, as
        well as
        
        FileExists : The file is already in the system
        FileExistsIdentifierConflict : The file is already in the
            system but with different identifiers
        FileExistsLanguageConflict : The file is already in the 
            system but with different languages
        '''
        
        if user is None:
            warn('Uploading a file without the `user` param is deprecated', DeprecationWarning  )
        
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
                    'user': user
                }
            )
            
            f.validate()
            data = f.to_bson()
             
            if overwrite == True:
                db_result = DB.files.replace_one({'_id': checksum}, data, upsert=True)
            else:
                db_result = DB.files.insert_one(data)
                    
            if db_result.acknowledged:
                return f
            else:
                raise Exception('This should be impossible')
        
        return False

    @staticmethod
    def _check_file_exists(checksum, identifiers, languages):
        existing_record = File.find_one({'_id': checksum})
        
        if existing_record:
            for idx in identifiers:
                if not list(filter(lambda x: x == idx, existing_record.identifiers)):
                    raise FileExistsIdentifierConflict(checksum, existing_record.identifiers, existing_record.languages)
            
            if sorted(languages) != sorted(existing_record.languages):
                raise FileExistsLanguageConflict(checksum, existing_record.identifiers, existing_record.languages)
                    
            raise FileExists()
            
    @staticmethod
    def encode_fn(identifiers, languages, extension):
        ids = [identifiers] if isinstance(identifiers, str) else identifiers
        langs = [languages] if isinstance(languages, str) else languages
        
        for lang in langs:
            assert ISO6391.codes[lang.lower()]
        
        return '{}-{}.{}'.format(
            '&'.join([idx.translate(str.maketrans(' /[]*:;', '__^^!#%')) for idx in ids]),
            '-'.join([x.upper() for x in langs]),
            extension
        )
            
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
    def find_by_identifier(cls, identifier, language=None, *, case_insensitive=True):
        assert isinstance(identifier, Identifier)
        
        query = {'identifiers': {'$elemMatch': {'type': identifier.type, 'value': identifier.value}}}
        
        if language:
            query['languages'] = language
        
        cln = Collation(locale='en', strength=2) if case_insensitive else None
        
        for f in DB.files.find(query, collation=cln, sort=[('timestamp', DESC)]):
            yield File(f)
        
    @classmethod
    def find_by_identifier_language(cls, identifier, language):
        return cls.find_by_identifier(identifier, language)
        
    @classmethod
    def latest_by_identifier_language(cls, identifier, language):
        return next(cls.find_by_identifier(identifier, language), None)
        
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
        self.filename = doc.get('filename')
        self.identifiers = [Identifier(i['type'], i['value']) for i in doc['identifiers']]
        self.languages = doc['languages']
        self.mimetype = doc['mimetype']
        self.size = doc['size']
        self.source = doc['source']
        self.timestamp = doc['timestamp']
        self.uri = doc['uri']
        self.updated = doc.get('updated')
        self.user = doc.get('user')
        
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
                ('uri', self.uri),
                ('user', self.user)
            ]
        )
        
        if self.updated:
            data['updated'] = self.updated
            
        return data 
    
    def to_dict(self):
        return self.to_bson().to_dict()
