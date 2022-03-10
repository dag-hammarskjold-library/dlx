"""Creates all indexes based on configurations in dlx.Config"""

import sys, re
from argparse import ArgumentParser
from dlx import DB, Config
from dlx.marc import Bib, Auth
from pymongo.collation import Collation

parser = ArgumentParser()
parser.add_argument('--connect')
parser.add_argument('--verbose', action='store_true')

def run():
    args = parser.parse_args()
    
    DB.connect(args.connect)

    print('creating file indexes...')
    
    DB.files.create_index('timestamp')
    DB.files.create_index('updated')
    DB.files.create_index('identifiers.type')
    DB.files.create_index('identifiers.value')
    DB.files.create_index(
        'identifiers.value',
        name='identifiers.value_caseinsensitive', 
        collation=Collation(locale='en', strength=2)
    )
    
    for _ in ['auths']:
        col = DB.handle[_]
        auth_ctrl = getattr(Config, _[:-1] + '_authority_controlled')
        index_fields = getattr(Config, _[:-1] + '_index')
        case_ins = getattr(Config, _[:-1] + '_index_case_insensitive')
        logical_fields = getattr(Config, _[:-1] + '_logical_fields')
        logical_numeric = getattr(Config, _[:-1] + '_index_logical_numeric')
        text_weights = getattr(Config, _[:-1] + '_text_index_weights')
        
        print(f'creating {_} indexes...')
        
        col.create_index('updated')
        col.create_index(f'{_}_use_count')
        
        for tag in index_fields:
            for name in col.index_information().keys():
                if re.search('code', name):
                    # deprecated indexes
                    col.drop_index(name)
        
            col.create_index(f'{tag}.subfields.value')
        
            if tag in auth_ctrl:
                col.create_index(f'{tag}.subfields.xref')
        
        print('creating case-insenstive indexes...')
        for tag in case_ins:
            col.create_index(
                f'{tag}.subfields.value',
                name=f'{tag}.subfields.value_caseinsensitive',
                collation=Collation(locale='en', strength=2)
            )
            
        print('creating numeric indexes...')
        for field in logical_numeric:
            col.create_index(field, name=f'{field}_numeric', collation=Collation(locale='en', numericOrdering=True))
        
        print('creating logical field indexes...')
        for field_name in logical_fields.keys():                    
            col.create_index(field_name)
            
        print('creating text index')
        for k, v in col.index_information().items():
            if v['key'][0][0] == '_fts':
                # drop any existing text index as the logical fields may have changed
                col.drop_index(k)

        col.create_index([(x, 'text') for x in logical_fields.keys()], weights=text_weights)
            
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
    