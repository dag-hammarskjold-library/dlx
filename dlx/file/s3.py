import boto3
from moto import mock_s3
#from moto import mock_s3

class S3(object):
    '''Wrapper around the boto3 s3 client'''
    
    connected = False
    
    @classmethod
    def connect(cls, access_key_id, access_key, bucket):
        '''Start a global "connection" to a specific s3 bucket'''
        
        cls.client = boto3.client(
            "s3",
            region_name = "us-east-1",
            aws_access_key_id = access_key_id,
            aws_secret_access_key = access_key,
        )
        
        cls.bucket = bucket
        cls.connected = True
    
    @classmethod
    def upload(cls, handle, file_key, mimetype):
        '''Uploads a file using a file-like object
        
        Raises
        ------
        botocore.exceptions.ClientError
        '''
        
        if not cls.connected:
            raise Exception('Not connected to s3')
        
        cls.client.upload_fileobj(
            handle,
            cls.bucket,
            file_key,
            ExtraArgs = {
                'ContentType': mimetype, 
                'ContentDisposition': 'inline'
            }
        )
        
        return True