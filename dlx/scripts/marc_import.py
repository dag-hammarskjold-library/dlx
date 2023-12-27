import sys
from argparse import ArgumentParser
from dlx import DB
from dlx.marc import BibSet, AuthSet

def get_args(**kwargs):
    parser = ArgumentParser(prog='marc-import', description='Import MARC data from file into the database')
    parser.add_argument('--connect', required=True, help='MongoDB connection string')
    parser.add_argument('--database', help='The database to use, if it differs from the one in the connection string')
    parser.add_argument('--type', required=True, choices=['bib', 'auth'], help='Input record type')
    parser.add_argument('--format', required=True, choices=['mrk', 'xml'], help='Input file type')
    parser.add_argument('--skip_auth_check', action='store_true', help='Don\'t enforce auth control on import')
    parser.add_argument('--file', required=True, help='Path to input file')
    parser.add_argument('--skip_prompt', action='store_true', help='Skip confirmation prompt')

    # if run as function convert args to sys.argv
    if kwargs:
        skip_check = kwargs.pop('skip_auth_check')
        skip_prompt = kwargs.pop('skip_prompt')
        sys.argv[1:] = [f'--{key}={val}' for key, val in kwargs.items()]
        if skip_check: sys.argv.append('--skip_auth_check')
        if skip_prompt: sys.argv.append('--skip_prompt')

    return parser.parse_args()

def run(**kwargs):
    args = get_args(**kwargs)
    DB.connect(args.connect, database=args.database)
    cls = BibSet if args.type == 'bib' else AuthSet
    string = open(args.file, 'r', encoding='utf8').read()
    method = getattr(cls, 'from_' + args.format)  
    marcset = method(string, auth_control=False if args.skip_auth_check else True)
    
    for record in marcset.records:
        print(record.to_mrk())

    proceed = input('Import to database? y/n: ') if not args.skip_prompt else 'y'

    if proceed.lower().strip() == 'y':
        for record in marcset.records:
            record.commit(auth_check=False if args.skip_auth_check else True)
            print(f'imported record with new ID {record.id}')

if __name__ == '__main__':
    run()
