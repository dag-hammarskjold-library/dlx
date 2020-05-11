import sys
from argparse import ArgumentParser
from dlx import DB, Config
from dlx.marc import Bib, Auth

###

parser = ArgumentParser()
parser.add_argument('--connect')

###

if __name__ == '__main__':
    main()
    
###

def main():
    args = parser.parse_args()
    
    DB.connect(args.connect)
    
    for tag, itype in Config.bib_index_fields.items():
        if itype == 'literal':
            Bib.literal_index(tag)
        elif itype == 'linked':
            Bib.linked_index(tag)
        elif itype == 'hybrid':
            Bib.hybrid_index(tag)
        else:
            raise Exception('Invalid index type')
        
    for tag, itype in Config.auth_index_fields.items():
        if itype == 'literal':
            Auth.literal_index(tag)
        elif itype == 'linked':
            Auth.linked_index(tag)
        elif itype == 'hybrid':
            Auth.hybrid_index(tag)
        else:
            raise Exception('Invalid index type')
    