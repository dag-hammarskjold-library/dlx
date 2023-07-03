import sys
from time import time
from argparse import ArgumentParser
from dlx import DB
from dlx.marc import Auth

def get_args(**kwargs):
    parser = ArgumentParser()
    parser.add_argument('--connect', required=True, help='MongoDB connection string')
    parser.add_argument('--database', help='The database to use, if it differs from the one in the connection string')
    parser.add_argument('--gaining_id', required=True, type=int)
    parser.add_argument('--losing_id', required=True, type=int)
    parser.add_argument('--user', required=True)
    parser.add_argument('--skip_prompt', action='store_true', help='skip prompt for confirmation before merging')

    # if run as function convert args to sys.argv
    if kwargs:
        prompt = kwargs.pop('skip_prompt')
        sys.argv[1:] = [f'--{key}={val}' for key, val in kwargs.items()]
        if prompt: sys.argv.append('--skip_prompt')

    return parser.parse_args()
  
def run(**kwargs):
    args = get_args(**kwargs)

    if not DB.connected:
        # currently necessary for tests
        DB.connect(args.connect, database=args.database)

    print(f'Merging auth {args.losing_id} into {args.gaining_id}')
    started = time()
    gaining_auth = Auth.from_id(args.gaining_id)
    
    if gaining_auth is None:
        raise Exception(f'Gaining record with ID {args.gaining_id} not found')

    losing_auth = Auth.from_id(args.losing_id)

    if losing_auth is None:
        raise Exception(f'Losing record {args.losing_id} not found')

    if not args.skip_prompt:
        proceed = input(f'{losing_auth.in_use(usage_type="bib")} bibs and {losing_auth.in_use(usage_type="auth")} auths will be updated\nproceed (y/n)? ')

        if proceed.lower() != 'y':
            exit()
    
    # perform merge
    gaining_auth.merge(losing_record=losing_auth, user=args.user)
    print(f'Finished in {time() - started} seconds')

if __name__ == '__main__':
    run()
