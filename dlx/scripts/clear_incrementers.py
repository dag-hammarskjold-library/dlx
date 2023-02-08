# Run this as a precaution after copying data between databases.
# This can be run at any time if the inceremnter gets corrupted.

from argparse import ArgumentParser
from dlx import DB

parser = ArgumentParser()
parser.add_argument('--connect', required=True, help='MongoDB connection string')
parser.add_argument('--database', help='The database to use, if it differs from the one in the connection string')

def run():
    args = parser.parse_args()
    DB.connect(args.connect, database=args.database)
    
    DB.handle['bib_id_counter'].drop()
    DB.handle['auth_id_counter'].drop()