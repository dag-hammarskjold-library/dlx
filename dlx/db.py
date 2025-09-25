"""
Provides the DB class for connecting to and accessing the database.
"""

import re, certifi, redis, valkey
from pymongo import MongoClient

#from pymongo.errors import OperationFailure, ServerSelectionTimeoutError
from mongomock import MongoClient as MockClient

class DB():
    """Provides a global database connection.

    Class attributes
    -----------
        All class attributes are set automatically by DB.connect()

    client : pymongo.MongoClient
    connected : bool
    handle : pymongo.database.Database
    bibs : pymongo.collection.Collection
    bib_history : pymongo.collection.Collection
    auths : pymongo.collection.Collection
    auth_history : pymongo.collection.Collection
    files : pymongo.collection.Collection
    config : dict
    is_atlas : bool
    """

    client = None
    connected = False
    database_name = None
    handle = None
    bibs = None
    bib_history = None
    auths = None
    auth_history = None
    files = None
    config = {}
    is_atlas = False
    cache = None

    ## class

    @classmethod
    def connect(cls, connection_string, *, database=None, mock=False, cache=None):
        """Connects to the database and stores database and collection handles
        as class attributes.

        Parameters
        ----------
        param1 : str
            MongoDB connection string.

        *database : str
            The name of the database to use. If not specified, the name will be 
            attempted to be parsed from the connection string.

        Returns
        -------
        pymongo.database.Database
            The database handle automatically gets stored as class attribute 'handle'.

        Raises
        ------
        pymongo.errors.ServerSelectionTimeoutError
            If the server is not found.
        pymongo.errors.AuthenticationFailure
            If the supplied credentials are invalid.
        """

        if mock or connection_string == 'mongomock://localhost':
            # testing environment
            # allows MongoEngine mock client connection string
            client =  MockClient()
            mock = True
        else:
            kwargs = {'serverSelectionTimeoutMS': 5000}
            
            if re.match(r'mongodb\+srv', connection_string):
                # https://pypi.org/project/certifi/ 
                # https://www.mongodb.com/docs/mongodb-shell/reference/options/#std-option-mongosh.--tlsCAFile
                kwargs['tlsCAFile'] = certifi.where()
            
            if re.match('mongodb\+srv://.*mongodb.net/', connection_string):
                # appears to be an Atlas instance
                DB.is_atlas = True
            else:
                DB.is_atlas = False

            client = MongoClient(connection_string, **kwargs)

        if database:
            DB.database_name = database
        else:    
            match = re.search(r'\?authSource=([\w]+)', connection_string)
            
            if match:
                DB.database_name = match.group(1)
            elif mock:
                DB.database_name = 'testing'
            else:
                raise Exception('No database name was provided and could not parse database name from connection string')

        DB.connected = True
        DB.config['connection_string'] = connection_string
        print(f'connected to database "{DB.database_name}"')

        DB.client = client
        DB.handle = client[DB.database_name]
        DB.bibs = DB.handle['bibs']
        DB.bib_history = DB.handle['bib_history']
        DB.auths = DB.handle['auths']
        DB.auth_history = DB.handle['auth_history']
        DB.files = DB.handle['files']

        if cache:
            DB.cache = None

            # redis and valkey clients have the same interface so they can be treated the same
            assert isinstance(cache, (redis.Redis, valkey.Valkey))
            
            try:
                # throws exception if the server is unavailable
                cache.ping()
            except:
                raise Exception(f'Unable to connect to cache server: {cache}')
            
            DB.cache = cache

    @classmethod
    def disconnect(cls):
        if DB.connected:
            DB.client.close()
            DB.connected = False
