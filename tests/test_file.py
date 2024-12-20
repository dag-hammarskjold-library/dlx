"""
Tests for dlx.file
"""

import os, time, pytest, responses
from moto import mock_aws

@pytest.fixture
@mock_aws
def s3():
    from dlx import Config
    from dlx.file import S3
       
    S3.connect(bucket='mock_bucket')
    S3.client.create_bucket(Bucket=S3.bucket) # has no effect outside this function?
    
    return S3.client
    
@pytest.fixture
def tempfile():
    from tempfile import TemporaryFile
    
    handle = TemporaryFile()
    handle.write(b'test data')
    handle.seek(0)
    
    return handle

@mock_aws
def test_import_from_handle(db, s3, tempfile):   
    from tempfile import TemporaryFile 
    from dlx import Config, DB
    from dlx.file import S3, File, Identifier, FileExists, FileExistsIdentifierConflict, FileExistsLanguageConflict
    
    S3.client.create_bucket(Bucket=S3.bucket) # this should be only necessary for testing
    handle = TemporaryFile()
    handle.write(b'some data')
    handle.seek(0)
    File.import_from_handle(
        handle, 
        identifiers=[Identifier('isbn', '1')], 
        filename='fn.ext', 
        languages=['EN'], 
        mimetype='application/dlx', 
        source='test',
        user='test'
    )
    
    results = list(DB.files.find({'identifiers': {'type': 'isbn', 'value': '1'}}))
    assert(len(results)) == 1
    assert results[0]['filename'] == 'fn.ext'
    assert results[0]['languages'] == ['EN']
    assert results[0]['mimetype'] == 'application/dlx'
    assert results[0]['source'] == 'test'
    assert results[0]['uri'] == '{}.s3.amazonaws.com/{}'.format(S3.bucket, results[0]['_id'])
    assert results[0]['user'] == 'test'
    
    with TemporaryFile() as fh:
        S3.client.download_fileobj(S3.bucket, results[0]['_id'], fh)
        fh.seek(0)
        assert fh.read() == b'some data'
    
    with pytest.raises(FileExists):
        handle = TemporaryFile()
        handle.write(b'some data')
        handle.seek(0)
        File.import_from_handle(handle, identifiers=[Identifier('isbn', '1')], filename='fn.ext', languages=['EN'], mimetype='application/dlx', source='test')    
        
    with pytest.raises(FileExistsIdentifierConflict) as xe:
        handle = TemporaryFile()
        handle.write(b'some data')
        handle.seek(0)
        File.import_from_handle(handle, identifiers=[Identifier('isbn', '2')], filename='test', languages=['FR'], mimetype='test', source=None)
    
    assert xe.value.existing_identifiers == [Identifier('isbn', '1')]
    assert xe.value.existing_languages == ['EN']
    
    with pytest.raises(FileExistsLanguageConflict) as xe:
        handle = TemporaryFile()
        handle.write(b'some data')
        handle.seek(0)
        File.import_from_handle(handle, identifiers=[Identifier('isbn', '1')], filename='test', languages=['FR'], mimetype='test', source=None)
        
    assert xe.value.existing_identifiers == [Identifier('isbn', '1')]
    assert xe.value.existing_languages == ['EN']

    handle.seek(0)
    File.import_from_handle(
        handle, identifiers=[Identifier('isbn', 'updated')], filename='fn.ext', languages=['AR'], mimetype='application/dlx', source='test', overwrite=True
    )
    
    results = list(DB.files.find({'identifiers': {'type': 'isbn', 'value': 'updated'}}))
    assert(len(results)) == 1
    assert results[0]['languages'] == ['AR']

@mock_aws
def test_import_from_path(db, s3):
    from dlx.file import S3, File, Identifier
    
    S3.client.create_bucket(Bucket=S3.bucket) # this should be only necessary for testing
    fh = open('temp', 'wb') # can't use NamedTemoraryFile here because the file goes awwy when the handle is closed
    fh.write(b'test data')
    fh.close()
    control = 'eb733a00c0c9d336e65691a37ab54293'
    f = File.import_from_path('temp', identifiers=[Identifier('isbn', '1')], filename='fn.ext', languages=['EN'], mimetype='application/dlx', source='test')
    assert f.id == control

@mock_aws
@responses.activate
def test_import_from_url(db, s3):
    import requests
    from http.server import HTTPServer, BaseHTTPRequestHandler
    from io import BytesIO
    from dlx.file import File, Identifier, S3
    
    S3.client.create_bucket(Bucket=S3.bucket) # this should be only necessary for testing
    server = HTTPServer(('127.0.0.1', 9090), None)
    responses.add(responses.GET, 'http://127.0.0.1:9090', body=BytesIO(b'test data').read())
    control = 'eb733a00c0c9d336e65691a37ab54293'
    f = File.import_from_url(url='http://127.0.0.1:9090', identifiers=[Identifier('isbn', '3')], filename='test', languages=['EN'], mimetype='test', source='test')
    assert f.id == control

@mock_aws   
def test_import_from_binary(db, s3):
    from io import BytesIO
    from dlx.file import File, Identifier, S3
    
    S3.client.create_bucket(Bucket=S3.bucket) # this should be only necessary for testing 
    control = 'eb733a00c0c9d336e65691a37ab54293'
    f = File.import_from_binary(b'test data', identifiers=[Identifier('isbn', '1')], filename='fn.ext', languages=['EN'], mimetype='application/dlx', source='test')
    assert f.id == control

@mock_aws    
def test_find(db, s3, tempfile):
    from dlx import Config, DB
    from dlx.file import S3, File, Identifier
    
    S3.client.create_bucket(Bucket=S3.bucket) # this should be only necessary for testing
    File.import_from_handle(tempfile, identifiers=[Identifier('isbn', '1')], filename='fn.ext', languages=['EN'], mimetype='application/dlx', source='test')
    
    for f in File.find({}):
        assert isinstance(f, File)
        assert f.id == 'eb733a00c0c9d336e65691a37ab54293'
        
    for f in File.find({'identifiers': [{'type': 'isbn', 'value': '1'}]}):
        assert f.id == 'eb733a00c0c9d336e65691a37ab54293'
        
    f = File.find_one({'_id': 'eb733a00c0c9d336e65691a37ab54293'})
    assert f.id == 'eb733a00c0c9d336e65691a37ab54293'

@mock_aws
def test_find_special(db, s3, tempfile):
    from datetime import datetime
    from dlx import Config, DB
    from dlx.file import S3, File, Identifier as ID
    
    S3.client.create_bucket(Bucket=S3.bucket) # this should be only necessary for testing
    File.import_from_handle(tempfile, identifiers=[ID('isbn', 'X')], filename='fn.ext', languages=['EN'], mimetype='application/dlx', source='test')

    results = list(File.find_by_identifier(ID('isbn', 'X')))
    assert len(results) == 1
    
    for f in results:    
        assert isinstance(f, File)
        
    results = list(File.find_by_identifier(ID('isbn', 'X'), 'EN'))
    assert len(results) == 1
    
    for f in results:    
        assert isinstance(f, File)
    
    time.sleep(1) # needed to get test to pass in Windows?
    results = list(File.find_by_date(datetime.strptime('1900-01-01', '%Y-%m-%d')))
    assert len(results) == 1
    
    for f in results:    
        assert isinstance(f, File)

    result = File.latest_by_identifier_language(ID('isbn', 'X'), 'EN')
    assert isinstance(result, File)
    
    result = File.latest_by_identifier_language(ID('isbn', 'x'), 'EN')
    #assert isinstance(result, File) # should work # case-insensitive search using collation not working in mongomock

@mock_aws    
def test_commit(db, s3, tempfile):
    from pymongo.results import UpdateResult
    from dlx import Config, DB
    from dlx.file import S3, File, Identifier as ID
    
    S3.client.create_bucket(Bucket=S3.bucket) # this should be only necessary for testing
    File.import_from_handle(tempfile, identifiers=[ID('isbn', '1')], filename='fn.ext', languages=['EN'], mimetype='application/dlx', source='test')
    
    f = list(File.find_by_identifier(ID('isbn', '1')))[0]
    f.identifiers = [ID('issn', '2')]
    result = f.commit()
    assert isinstance(result, UpdateResult)
    f = list(File.find_by_identifier(ID('issn', '2')))[0]
    assert f.identifiers[0].type == 'issn'
    assert f.identifiers[0].value == '2'
    assert f.updated
    
    