# This script (re)builds the logical fields and browse indexes in the database. 
# Run time is approx 15 minutes at this time. This can be run at any time, even 
# if the fields are already built, without detriment to the data.

import sys
from bson import Regex
from pymongo import UpdateOne, ASCENDING as ASC, DESCENDING as DESC
from argparse import ArgumentParser
from dlx import DB, Config
from dlx.marc import BibSet, Bib, AuthSet, Auth, Query

parser = ArgumentParser()
parser.add_argument('--connect', required=True)
parser.add_argument('--type', required=True, choices=['bib', 'auth'])
parser.add_argument('--start', default=0)
  
def run():
    args = parser.parse_args()
    
    if not DB.connected:
        DB.connect(args.connect)

    build_literal_logical_fields(args)
    build_auth_controlled_logical_fields(args)
    
    if args.type == 'auth':
        calculate_auth_use()

def build_literal_logical_fields(args):
    cls = BibSet if args.type == 'bib' else AuthSet
    auth_controlled = Config.bib_authority_controlled if cls == BibSet else Config.auth_authority_controlled
    logical_fields = Config.bib_logical_fields if cls == BibSet else Config.auth_logical_fields
    tags, literals = [], []

    for field, d in list(logical_fields.items()):
        tags += list(d.keys())
        literals.append(field)
    
    tags = set(tags)            
    literals = set(literals)
    
    print(f'building {list(literals)}')
    
    c = r = start = int(args.start)
    inc = 10000
    query = {}
    end = cls().handle.estimated_document_count() #cls.from_query(query).count
    
    for i in range(start, end, inc):
        updates, browse_updates = [], {}
        
        for record in cls.from_query(query, sort=[('_id', DESC)], skip=i, limit=inc, projection=dict.fromkeys(tags, 1)):
            for field, values in record.logical_fields(*list(literals)).items():
                updates.append(UpdateOne({'_id': record.id}, {'$set': {field: values}}))
                
                browse_updates.setdefault(field, {})

                for val in values:
                    browse_updates[field].setdefault(val, UpdateOne({'_id': val}, {'$setOnInsert': {'_id': val}}, upsert=True))
                
            last_r = r
            r += 1
            print('\b' * (len(str(last_r)) + len(str(end)) + 3) + f'{r} / {end}', end='', flush=True)
                
        if updates:
            cls().handle.bulk_write(updates)
            c += len(updates)
            
        if browse_updates:
            for field in record.logical_fields(*list(literals)).keys():
                DB.handle[f'_index_{field}'].bulk_write(list(browse_updates[field].values()))

    print(f'\nupdated {c} logical fields')

def build_auth_controlled_logical_fields(args):
    if args.type == 'auth':
        # there are no auth ctrld auth logical fields
        return

    for field, tags in Config.bib_logical_fields.items():
        if field not in Config.auth_controlled_bib_logical_fields():
            continue
        
        values, updates, browse_updates = {}, [], {}
        
        for tag, groups in tags.items():                
            print(f'building: {field}, {tag}')
            
            codes = []
            
            for group in groups: codes += group

            source_tag = Config.authority_source_tag('bib', tag, codes[0])
            
            if source_tag is None: continue

            computed = DB.bibs.aggregate(
                [ 
                    {'$match': {f'{tag}.subfields': {'$elemMatch': {'xref': {'$exists': 1}, 'code': {'$in': codes}}}}},
                    {'$project': {tag: 1}},
                    {'$lookup': {'from': 'auths', 'localField': f'{tag}.subfields.xref', 'foreignField': '_id', 'as': 'matched'}},
                    {'$project': {f'matched.{source_tag}': 1}},
                    {'$unwind': '$matched'}
                ]
            )

            for doc in computed:
                rid = doc['_id']

                # each doc represents one marc field
                auth = Auth(doc['matched'])
                value = ' '.join(auth.get_values(source_tag, *codes))
                
                values.setdefault(rid, {})
                values[rid].setdefault(field, [])
                values[rid][field].append(value)
   
        for rid, d in values.items():
            for field, vals in d.items():
                updates.append(UpdateOne({'_id': rid}, {'$set': {field: vals}}))
                
                for val in vals:
                    browse_updates.setdefault(val, (UpdateOne({'_id': val}, {'$setOnInsert': {'_id': val}}, upsert=True)))
                
        print(f'updates for {field}: {len(updates)}')
        
        start, end, inc = 0, len(updates), 1000
        
        for i in range(start, end, inc):
            result = DB.bibs.bulk_write(updates[i:i+inc])
                
            print('\b' * (len(str(i)) + len(str(end)) + 3) + f'{i+inc} / {end}', end='', flush=True)
            
        if end:
            print('\n') 
        
        print(f'updating {field} browse indexes')
        
        updates = list(browse_updates.values())
        
        start, end, inc = 0, len(updates), 1000 
        
        for i in range(start, end, inc):
            DB.handle[f'_index_{field}'].bulk_write(updates[i:i+inc])
                
            print('\b' * (len(str(i)) + len(str(end)) + 3) + f'{i+inc} / {end}', end='', flush=True)
            
        if end:
            print('\n')
    
def calculate_auth_use():
    print('calculating auth usage')
    
    count = {}
    
    for tag in Config.bib_authority_controlled.keys():
        results = DB.bibs.aggregate(
            [
                {'$unwind': f'${tag}'},
                {'$unwind': f'${tag}.subfields'},
                {'$group': {'_id' : f'${tag}.subfields.xref', 'count': {'$sum': 1}}},
            ]
        )
        
        i = 0
        
        for r in results:
            xref = r['_id']
            count.setdefault(xref, 0)
            count[xref] += r['count']
            i += 1
        
        print(f'counted {i} xrefs for tag {tag}')
        
    print('updating the database...')
    
    updates, inc = [], 50000
        
    for auth_id, count in count.items():
        updates.append(UpdateOne({'_id': auth_id}, {'$set': {'bib_use_count': count}}))
    
    for start in range(0, len(updates), inc):
        DB.auths.bulk_write(updates[start:start+inc])
        print('\b' *  (len(str(start-inc)) + len(str(len(updates))) + 3) + str(start) + f' / {len(updates)}', end='', flush=True)

###

if __name__ == '__main__':
    run()