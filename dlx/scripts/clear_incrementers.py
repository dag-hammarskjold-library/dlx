# Run this as a precaution after copying data between databases.
# This can be run at any time if the inceremnter gets corrupted.

from argparse import ArgumentParser
from dlx import DB

parser = ArgumentParser()
parser.add_argument('--connect', required=True)
parser.add_argument('--dbname')

def run():
    args = parser.parse_args()
    
    if args.dbname:
        DB.connect(args.connect, database=args.dbname)
    else:
        DB.connect(args.connect)
    DB.handle['bib_id_counter'].drop()
    DB.handle['auth_id_counter'].drop()