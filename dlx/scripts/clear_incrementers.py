# Run this as a precaution after copying data between databases.
# This can be run at any time if the inceremnter gets corrupted.

from argparse import ArgumentParser
from dlx import DB

parser = ArgumentParser()
parser.add_argument('--connect', required=True)

def run():
    args = parser.parse_args()
    
    DB.connect(args.connect)
    DB.handle['bib_id_counter'].drop()
    DB.handle['auth_id_counter'].drop()