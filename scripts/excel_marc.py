import sys
from argparse import ArgumentParser
from dlx import DB
from dlx.marc import BibSet, AuthSet

###

parser = ArgumentParser()
parser.add_argument('--connect')
parser.add_argument('--file')
parser.add_argument('--type')
parser.add_argument('--format')
parser.add_argument('--check')
args = parser.parse_args()

###

if __name__ == '__main__':
    main()
    
###

def main(*f_args):
    if f_args:
        args = parser.parse_args()
    
    DB.connect(args.connect)

    if args.type == 'bib':
        Cls = BibSet
    elif args.type == 'auth': 
        Cls = AuthSet
    else:
        raise Exception
    

    data = Cls.from_excel(args.file, auth_control=False, auth_flag=True, field_check=args.check)

    convert_method = 'to_' + args.format

    print(getattr(data, convert_method)(), end='')
