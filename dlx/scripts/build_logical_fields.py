# work in progress

import sys
from bson import Regex
from pymongo import UpdateOne, ASCENDING as ASC, DESCENDING as DESC
from argparse import ArgumentParser
from dlx import DB, Config
from dlx.marc import BibSet, Bib, AuthSet, Query

parser = ArgumentParser()
parser.add_argument('--connect')
parser.add_argument('--type', required=True)
parser.add_argument('--start', default=0)
  
def run():
    args = parser.parse_args()
    
    if not DB.connected:
        DB.connect(args.connect)
    
    cls = BibSet if args.type == 'bib' else AuthSet if args.type == 'auth' else None
    
    #build_logical_fields(cls)
     
    #if cls == AuthSet:
    #    calcuate_auth_use()
    
    tags = []
    
    for x in list(Config.bib_logical_fields.values()) + list(Config.auth_logical_fields.values()):
        tags += list(x.keys())
        
    c = r = start = int(args.start)
    inc = 1000
    query = {}
    end = cls.from_query(query).count
    
    for i in range(start, end, inc):
        updates = []
        
        for record in cls.from_query(query, sort=[('_id', DESC)], skip=i, limit=inc, projection=dict.fromkeys(tags, 1)):
            for field, values in record.logical_fields().items():
                updates.append(UpdateOne({'_id': record.id}, {'$set': {field: values}}))
                
            last_r = r
            r += 1
            print('\b' * (len(str(last_r)) + len(str(end)) + 3) + f'{r} / {end}', end='', flush=True)
                
        if updates:
            cls().handle.bulk_write(updates)
            c += len(updates)

    print(f'\nupdated {c} logical fields')
    
def build_logical_fields(cls):
    for field, tags in Config.bib_logical_fields.items():
        for tag, groups in tags.items():
    
            if tag not in ['600']: #Config.bib_authority_controlled:
                continue
            
            codes = []
            
            for group in groups:
                codes += group

            source_tag = Config.authority_source_tag('bib', tag, codes[0])

            computed = DB.bibs.aggregate(
                [ 
                    {'$match': {f'{tag}.subfields': {'$elemMatch': {'xref': {'$exists': 1}, 'code': {'$in': codes}}}}},
                    {'$unwind': f'${tag}'},
                    {'$unwind': f'${tag}.subfields'},
                    {'$match': {f'{tag}.subfields.xref': {'$exists': 1}}},
                    {'$lookup': {'from': 'auths', 'localField': f'{tag}.subfields.xref', 'foreignField': '_id', 'as': 'matched'}},
                    {'$project': {'': 1, 'matched': 1}},
                    {'$unwind': '$matched'},
                    {'$unwind': f'$matched.{source_tag}'},
                    {'$unwind': f'$matched.{source_tag}.subfields'},
                    {'$project': {'matched._id': 1, f'matched.{source_tag}.subfields.code': 1, f'matched.{source_tag}.subfields.value': 1}}
                ]
            )

            for doc in computed:
                code = doc['matched'][source_tag]['subfields']['code']
                value = doc['matched'][source_tag]['subfields']['value']
                
                print(doc)
                
                exit()
    
def calcuate_auth_use():
    print('calculating auth usage...')
    
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
    
    updates = []
        
    for auth_id, count in count.items():
        updates.append(UpdateOne({'_id': auth_id}, {'$set': {'bib_use_count': count}}))

    for start in range(0, len(updates), 1000):
        DB.auths.bulk_write(updates[start:start+1000])
        print('\b' *  (len(str(start-1000)) + len(str(len(updates))) + 3) + str(start) + f' / {len(updates)}', end='', flush=True)

###

if __name__ == '__main__':
    run()