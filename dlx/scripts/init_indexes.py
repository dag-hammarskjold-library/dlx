import sys, re
from argparse import ArgumentParser
from dlx import DB, Config
from dlx.marc import Bib, Auth
from pymongo.collation import Collation

###

parser = ArgumentParser()
parser.add_argument('--connect')
parser.add_argument('--verbose', action='store_true')
    
###

def run():
    args = parser.parse_args()
    
    DB.connect(args.connect)
    
    DB.bibs.create_index('updated')
    DB.auths.create_index('updated')
    
    DB.files.create_index('timestamp')
    DB.files.create_index('updated')
    DB.files.create_index('identifiers.type')
    DB.files.create_index('identifiers.value')
    DB.files.create_index(
        'identifiers.value',
        name='identifiers.value_caseinsensitive', 
        collation=Collation(locale='en', strength=2)
    )
    
    for _ in ('bibs', 'auths'):
        col = DB.handle[_]
        auth_ctrl = getattr(Config, _[:-1] + '_authority_controlled')
        index_fields = getattr(Config, _[:-1] + '_index')
        case_ins = getattr(Config, _[:-1] + '_index_case_insensitive')
        logical_fields = getattr(Config, _[:-1] + '_logical_fields')
        
        for tag in index_fields:
            for name in col.index_information().keys():
                if re.search('code', name):
                    col.drop_index(name)

            col.create_index(f'{tag}.subfields.value')
        
            if tag in auth_ctrl:
                col.create_index(f'{tag}.subfields.xref')
        
        for tag in case_ins:
            col.create_index(
                f'{tag}.subfields.value',
                name=f'{tag}.subfields.value_caseinsensitive',
                collation=Collation(locale='en', strength=2)
            )
            
        col.create_index([('$**', 'text')])
        
        for field_name in logical_fields.keys():
            col.create_index(field_name)
            
    DB.auths.create_index('bib_use_count')
    DB.auths.create_index('auth_use_count')
            
    total = sum(
        [
            len(list(DB.bibs.list_indexes())),
            len(list(DB.auths.list_indexes())),
            len(list(DB.files.list_indexes()))
        ]
    ) - 3
    
    print(f'{total} indexes in DB')
    
    if args.verbose:
        for col in [DB.bibs, DB.auths, DB.files]:
            print(
                *list(map(lambda x: f'{col.name}: {x}', sorted(col.index_information().keys()))), 
                sep='\n'
            )
###

if __name__ == '__main__':
    run()
    