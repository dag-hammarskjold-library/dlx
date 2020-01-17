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
args = parser.parse_args()

###

def main():
    DB.connect(args.connect)

    if args.type == 'bib':
        Cls = BibSet
    elif args.type == 'auth': 
        Cls = AuthSet
    else:
        raise Exception
    
    data = Cls.from_excel(args.file, auth_control=False, auth_flag=True)
    convert_method = 'to_' + args.format

    print(getattr(data, convert_method)(), end='')

###

main()