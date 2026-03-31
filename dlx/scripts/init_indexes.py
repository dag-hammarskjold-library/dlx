"""Creates all indexes based on configurations in dlx.Config"""

import sys, re
from argparse import ArgumentParser
from dlx import DB, Config
from dlx.marc import Bib, Auth
from pymongo.collation import Collation
from pymongo.operations import SearchIndexModel

parser = ArgumentParser()
parser.add_argument('--connect', required=True, help='MongoDB connection string')
parser.add_argument('--database', help='The database to use, if it differs from the one in the connection string')
parser.add_argument('--verbose', action='store_true')
parser.add_argument('--atlas_search', action='store_true', help='Create MongoDB Atlas Search indexes')

def create_atlas_search_indexes(col, *, record_type):
    if not hasattr(col, 'create_search_index'):
        print(f'skipping atlas search indexes for {col.name}: client does not support search index API')
        return []

    logical_fields = getattr(Config, f'{record_type}_logical_fields')
    auth_ctrl = getattr(Config, f'{record_type}_authority_controlled')
    index_fields = getattr(Config, f'{record_type}_index')

    logical_map = {
        field: {'type': 'string'}
        for field in logical_fields.keys()
    }
    logical_map.update({
        '_record_type': {'type': 'string'},
        'text': {'type': 'string'},
        'words': {'type': 'string'}
    })

    tags = sorted(set(index_fields + list(auth_ctrl.keys())))
    marc_map = {
        tag: {
            'type': 'document',
            'fields': {
                'subfields': {
                    'type': 'document',
                    'fields': {
                        'code': {'type': 'string'},
                        'value': {'type': 'string'},
                        'xref': {'type': 'number'}
                    }
                }
            }
        }
        for tag in tags
    }

    models = [
        SearchIndexModel(
            definition={'mappings': {'dynamic': False, 'fields': logical_map}},
            name='logical_fields',
            type='search'
        ),
        SearchIndexModel(
            definition={'mappings': {'dynamic': False, 'fields': marc_map}},
            name='marc_fields',
            type='search'
        )
    ]

    existing = {idx.get('name') for idx in col.list_search_indexes()}

    for model in models:
        if model.document['name'] in existing:
            col.drop_search_index(model.document['name'])

    created = []

    for model in models:
        created.append(col.create_search_index(model))

    return created

def run():
    args = parser.parse_args()
    DB.connect(args.connect, database=args.database)
    
    indexes = []

    print('creating file indexes...')
    indexes.append(DB.files.create_index('timestamp'))
    indexes.append(DB.files.create_index('updated'))
    indexes.append(DB.files.create_index('identifiers.type'))
    indexes.append(DB.files.create_index('identifiers.value'))
    indexes.append(
        DB.files.create_index(
            'identifiers.value',
            name='identifiers.value_caseinsensitive', 
            collation=Collation(locale='en', strength=2)
        )
    )
    
    for _ in ['bibs', 'auths']:
        col = DB.handle[_]
        auth_ctrl = getattr(Config, _[:-1] + '_authority_controlled')
        index_fields = getattr(Config, _[:-1] + '_index')
        case_ins = getattr(Config, _[:-1] + '_index_case_insensitive')
        logical_fields = getattr(Config, _[:-1] + '_logical_fields')
        logical_numeric = getattr(Config, _[:-1] + '_index_logical_numeric')
        text_weights = getattr(Config, _[:-1] + '_text_index_weights')
        
        print(f'creating {_} indexes...')
        indexes.append(
            col.create_index(
                # to allow sorting if the search is using this collation
                'updated',
                name='updated_collated',
                collation=Config.marc_index_default_collation
            )
        )
        indexes.append(
            col.create_index(
                # to allow sorting if the search is using this collation
                'created',
                name='created_collated',
                collation=Config.marc_index_default_collation
            )
        )
        indexes.append(col.create_index('_record_type_collated', collation=Config.marc_index_default_collation))

        print(f'creating text indexes...')
        indexes.append(col.create_index('words', collation=Config.marc_index_default_collation))
        indexes.append(col.create_index('text', collation=Config.marc_index_default_collation))

        print(f'creating tag indexes...')
        for tag in index_fields + list(auth_ctrl.keys()):
            #col.drop_index(f'{tag}.$**_1')
            indexes.append(col.create_index(f'{tag}.$**', collation=Config.marc_index_default_collation))

        print('creating logical field indexes...')
        for field_name in logical_fields.keys():
            #if field_name + '_1' in col.index_information().keys():
            #    col.drop_index(field_name + '_1')
            indexes.append(
                col.create_index(
                    field_name,
                    name=field_name + '_collated',
                    collation=Config.marc_index_default_collation
                )
            )

        print('creating wildcard index...')
        exclude = index_fields + list(auth_ctrl.keys())
        exclude += list(logical_fields.keys())
        exclude += ['updated', 'created', 'words', 'text', '_record_type']

        for name, index in col.index_information().items():
            if index['key'][0][0] == '$**':
                # drop any existing wildcard index if the excluded fields changed
                if set(index['wildcardProjection'].keys()) != set(exclude):
                    col.drop_index(name)

        indexes.append(
            col.create_index(
                "$**",
                wildcardProjection=dict.fromkeys(exclude, 0),
                collation=Config.marc_index_default_collation
            )
        )

        # not currently in use  
        #print('creating logical field text index...')
        #for k, v in col.index_information().items():
        #    if v['key'][0][0] == '_fts':
        #        # drop any existing wildcard index as the logical fields may have changed
        #        col.drop_index(k)
        #
        #
        #indexes.append(
        #    col.create_index([(x, 'text') for x in logical_fields.keys()], default_language='none', weights=text_weights)
        #)

        print('creating logical field text collection indexes...')
        for field in logical_fields.keys():
            index_col = DB.handle[f'_index_{field}']
            
            # debug
            #for k, v in index_col.index_information().items():
            #    if v['key'][0][0] == '_fts':
            #        index_col.drop_index(k)
            
            # not currently in use
            #indexes.append(
            #    index_col.create_index([('_id', 'text')], default_language='none')
            #)

            indexes.append(
                index_col.create_index('text')
            )
            indexes.append(
                index_col.create_index(
                    'text',
                    name='text_collated',
                    collation=Config.marc_index_default_collation
                )
            )
            indexes.append(
                index_col.create_index('words')
            )
            indexes.append(
                index_col.create_index('_record_type')
            )

        print('creating tag field text indexes: ', end='', flush=True)
         # get all tags in collection
        result = col.aggregate(
            [
                {"$project": {"data": {"$objectToArray": "$$ROOT"}}},
                {"$project": {"data": "$data.k"}},
                {"$unwind": "$data"},
                {"$group": {"_id": None, "keys": {"$addToSet": "$data"}}}
            ]
        )
        tags = filter(lambda x: re.match('\d{3}', x), list(result)[0]['keys'])
    
        for tag in sorted(tags):
            print(tag + ' ', flush=True, end='')
            index_col = DB.handle[f'_index_{tag}']
        
            # debug
            #for k, v in index_col.index_information().items():
            #    if v['key'][0][0] == '_fts':
            #        index_col.drop_index(k)
        
            # not currently in use
            #indexes.append(
            #    index_col.create_index([('subfields.value', 'text')], default_language='none')
            #)
            indexes.append(
                index_col.create_index('words')
            )
            indexes.append(
                index_col.create_index('text')
            )
        
        print('\n')

        print('Dropping extraneous indexes...')
        for index in col.index_information().keys():
            if index != '_id_' and index not in indexes:
                # drop any other indexes on collection
                col.drop_index(index)

        if args.atlas_search:
            print(f'creating atlas search indexes on {_}...')
            create_atlas_search_indexes(col, record_type=_[:-1])

    total = len(indexes)
    
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
    