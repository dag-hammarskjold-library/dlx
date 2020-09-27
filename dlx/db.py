"""
Provides the DB class for connecting to and accessing the database.
"""

import re
from pymongo import MongoClient
#from pymongo.errors import OperationFailure, ServerSelectionTimeoutError
from mongomock import MongoClient as MockClient

class DB():
    """Provides a global database connection.

    Class attributes
    -----------
        All class attributes are set automatically by DB.connect()

    connected : bool
    handle : pymongo.database.Database
    bibs : pymongo.collection.Collection
    auths : pymongo.collection.Collection
    file : pymongo.collection.Collection
    config : dict
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

    ## class

    @classmethod
    def connect(cls, connection_string, **kwargs):
        """Connects to the database and stores database and collection handles
        as class attributes.

        Parameters
        ----------
        param1 : str
            MongoDB connection string.

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

        if 'mock' in kwargs.keys():
            client = MockClient(connection_string)
        elif connection_string == 'mongomock://localhost':
            # allows MongoEngine mock client connection string
            connection_string = 'mongodb://.../?authSource=dummy'
            client = MockClient(connection_string)
        else:
            client = MongoClient(connection_string, serverSelectionTimeoutMS=10)

        # raises pymongo exceptions if connection fails
        try:
            client.admin.command('ismaster')
        except NotImplementedError:
            pass

        DB.connected = True
        DB.config['connection_string'] = connection_string

        match = re.search(r'\?authSource=([\w]+)', connection_string)

        if match:
            DB.database_name = match.group(1)
        else:
            # this should be impossible
            raise Exception('Could not parse database name from connection string')

        DB.client = client
        DB.handle = client[DB.database_name]
        DB.bibs = DB.handle['bibs']
        DB.bib_history = DB.handle['bib_history']
        DB.auths = DB.handle['auths']
        DB.auth_history = DB.handle['auth_history']
        DB.files = DB.handle['files']
        
        return DB.client

    @classmethod
    def disconnect(cls):
        if DB.connected:
            DB.client.close()

    ## static

    @staticmethod
    def check_connection():
        """Raises an exception if the database has not been connected to.

        This is used to prevent attempts at database operations without
        being connected, which can create hard-to-trace errors.

        Returns
        -------
        None

        Raises
        ------
        Exception
            If the database has not been connected to yet.
        """

        try:
            DB.client.admin.command('ismaster')
        except NotImplementedError:
            pass

        return True
