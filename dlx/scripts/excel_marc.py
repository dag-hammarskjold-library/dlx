import sys
from argparse import ArgumentParser
from dlx import DB
from dlx.marc import BibSet, AuthSet

###

parser = ArgumentParser()
parser.add_argument('--connect', required=True, help='MongoDB connection string')
parser.add_argument('--database', help='The database to use, if it differs from the one in the connection string')
parser.add_argument('--file', required=True)
parser.add_argument('--type', required=True, choices=['bib', 'auth'])
parser.add_argument('--format', required=True, choices=['mrc', 'mrk', 'xml'])
parser.add_argument('--check')
parser.add_argument('--out')
parser.add_argument('--defaults')

###

def run():
    args = parser.parse_args()
    DB.connect(args.connect, database=args.database)
  
    Cls = BibSet if args.type == 'bib' else AuthSet

    data = Cls.from_excel(args.file, auth_control=False, auth_flag=True, field_check=args.check)
    
    if args.defaults:
        defaults = Cls.from_excel(args.defaults, auth_control=False, auth_flag=True, field_check=args.check).records[0]
        
        for record in data.records:
            record.merge(defaults)
            
            _008 = record.get_value('008').ljust(40, '|')
            _008 = \
            _008[0:15] + \
            record.get_value('049', 'a').ljust(3, '|') + \
            _008[18:35] + \
            record.get_value('041', 'a')[0:3].ljust(3, '|') + \
            _008[38:40]
            
            record.set('008', None, _008)
            
            record.set_008()
    
    convert_method = 'to_' + args.format
    fh = open(args.out, 'w', encoding='utf-8') if args.out else sys.stdout
    
    fh.write(getattr(data, convert_method)())

###

if __name__ == '__main__':
    run()
