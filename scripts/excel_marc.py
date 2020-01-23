import sys
from argparse import ArgumentParser
from dlx import DB
from dlx.marc import BibSet, AuthSet

###

parser = ArgumentParser()
parser.add_argument('--connect')
parser.add_argument('--file', required=True)
parser.add_argument('--type', required=True, choices=['bib', 'auth'])
parser.add_argument('--format', required=True, choices=['mrc', 'xml'])
parser.add_argument('--check')
parser.add_argument('--out')

###

if __name__ == '__main__':
    main()
    
###

def main():
    args = parser.parse_args()

    DB.connect(args.connect)
    
    Cls = BibSet if args.type == 'bib' else AuthSet
    data = Cls.from_excel(args.file, auth_control=False, auth_flag=True, field_check=args.check)
    convert_method = 'to_' + args.format
    fh = open(args.out, 'w', encoding='utf-8') if args.out else sys.stdout
    
    fh.write(getattr(data, convert_method)())
