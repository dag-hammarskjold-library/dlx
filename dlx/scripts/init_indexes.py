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
    
    for tag in Config.bib_index:
        DB.bibs.create_index(f'{tag}.subfields.code')
        DB.bibs.create_index(f'{tag}.subfields.value')
        
        if tag in Config.bib_authority_controlled:
            DB.bibs.create_index(f'{tag}.subfields.xref')
        
    for tag in Config.bib_index_case_insensitive:
        DB.bibs.create_index(
            f'{tag}.subfields.value', 
            name=f'{tag}.subfields.value_caseinsensitive', 
            collation=Collation(locale='en', strength=2)
        )
        
    for tag in Config.auth_index:
        DB.auths.create_index(f'{tag}.subfields.code')
        DB.auths.create_index(f'{tag}.subfields.value')
        
        if tag in Config.auth_authority_controlled:
            DB.auths.create_index(f'{tag}.subfields.xref')
        
    for tag in Config.auth_index_case_insensitive:
        DB.auths.create_index(
            f'{tag}.subfields.value', 
            name=f'{tag}.subfields.value_caseinsensitive', 
            collation=Collation(locale='en', strength=2)
        )
    
    created = sum(
        [
            len(list(DB.bibs.list_indexes())),
            len(list(DB.auths.list_indexes())),
            len(list(DB.files.list_indexes()))
        ]
    ) - 3
    
    print(f'created {created} indexes')
    