import sys
from argparse import ArgumentParser
from dlx import DB, Config
from dlx.marc import Bib, Auth
from pymongo.collation import Collation

###

parser = ArgumentParser()
parser.add_argument('--connect')

###

if __name__ == '__main__':
    run()
    
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
        case_ins = getattr(Config, _[:-1] + '_index_case_insensitive')
        logical_fields = getattr(Config, _[:-1] + '_logical_fields')
        
        for tag in Config.bib_index:
            col.create_index(f'{tag}.subfields.code')
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
            
    created = sum(
        [
            len(list(DB.bibs.list_indexes())),
            len(list(DB.auths.list_indexes())),
            len(list(DB.files.list_indexes()))
        ]
    ) - 3
    
    print(f'created {created} indexes')
    