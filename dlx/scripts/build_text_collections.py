
# This script (re)builds the text index collections.
# This can take a large amount of time. If necessary, use the --start parameter
# to stop the script and start from where you left off.
# This can be run at any time, even if the fields are already built, without 
# detriment to the data.

import sys, os, time, re
from copy import deepcopy
from collections import Counter
from pymongo import UpdateOne, ReplaceOne, InsertOne
from argparse import ArgumentParser
from dlx import DB, Config
from dlx.marc import BibSet, AuthSet, Auth
from dlx.util import Tokenizer

parser = ArgumentParser()
parser.add_argument('--connect', required=True, help='MongoDB connection string')
parser.add_argument('--database', help='The database to use, if it differs from the one in the connection string')
parser.add_argument('--type', required=True, choices=['bib', 'auth'])
parser.add_argument('--start', default=0)

def run():
    args = parser.parse_args()
    DB.connect(args.connect, database=args.database)
    
    cls = BibSet if args.type == 'bib' else AuthSet
    start = int(args.start) or 0
    print(list(DB.bibs.find({})))
    end = list(cls.from_query({}, sort=[('_id', -1)], limit=1).records)[0].id if DB.database_name != 'testing' else 1 # doesn't work in the test for some reason
    inc = 10000
    tags = set()
    exclude_fields = [] #list(Config.bib_logical_fields.keys() if args.type == 'bib' else Config.auth_logical_fields.keys())
    exclude_fields += ['words', 'text']

    print('building auth cache...')
    Auth.build_cache()

    # chunks
    for i in range(start, end, inc):
        print(f'{i+1}-{i+inc}', end='', flush=True)

        # records
        updates, record_updates, start_time = {}, [], time.time()
        to_set = {}
        seen = {}

        for record in cls.from_query({'$and': [{'_id': {'$gte': i}}, {'_id': {'$lte': i + inc}}]}):
            all_text, all_words, tagindex = [], [], {}

            for field in record.datafields:
                if field.tag in ('949', '998', '999'):
                    continue

                #field.subfields = list(filter(lambda x: not hasattr(x, 'xref'), field.subfields))
                tagindex.setdefault(field.tag, 0)
                tags.add(field.tag)

                # concatenated subfield text
                text = ' '.join(filter(None, [x.value for x in field.subfields]))
                scrubbed = Tokenizer.scrub(text)
                all_text.append(scrubbed)
                words = list(Counter(Tokenizer.tokenize(scrubbed)).keys())
                all_words += words

                # skip if exists 
                if seen.get(field.tag, {}).get(text, {}).get('subfields'):
                    if [(x['code'], x['value']) for x in seen[field.tag][text]['subfields']] == [(x.code, x.value) for x in field.subfields]:
                        continue
                
                # add to seen index
                seen.setdefault(field.tag, {})
                seen[field.tag].setdefault(text, {})
                seen[field.tag][text]['subfields'] = [{'code': x.code, 'value': x.value} for x in field.subfields]

                updates.setdefault(field.tag, [])
                
                # whole field
                updates[field.tag].append(
                    UpdateOne(
                        {'_id': text},
                        {
                            '$set': {
                                'text': f' {scrubbed} ',
                                'words': words
                            }
                        },
                        upsert=True
                    )
                )

                # individual subfields
                for subindex, subfield in enumerate(field.subfields):
                    # text col
                    if not subfield.value:
                        continue

                    updates[field.tag].append(
                        UpdateOne(
                            {'_id': text},
                            {
                                '$addToSet': {
                                    'subfields': {
                                        'code': subfield.code,
                                        'value': subfield.value
                                    }
                                }
                            },
                            #upsert=True
                        )
                    )

                # tag counter
                tagindex[field.tag] += 1

            # while record text 
            all_text = ' ' + ' '.join(all_text) + ' '
            count = Counter(all_words)
            words = list(count.keys())
           
            if record.data.get('text') != all_text:
                record_updates.append(UpdateOne({'_id': record.id}, {'$set': {'text': all_text}}))

            if record.data.get('words') != words:
                record_updates.append(UpdateOne({'_id': record.id}, {'$set': {'words': words}}))

            if record.data.get('word_count'):
                record_updates.append(UpdateOne({'_id': record.id}, {'$unset': {'word_count': ''}}))

        print('\u2713' + str(int(time.time() - start_time)) + 's', end='', flush=True)

        for tag in updates.keys():
            col = DB.handle[f'_index_{tag}']
            found = False
            
            if updates[tag]:
                col.bulk_write(updates[tag])

        record_col = DB.handle[args.type + 's']

        if record_updates:
            record_col.bulk_write(record_updates)
        
        print('\u2713:' + str(int(time.time() - start_time)) + 'ts', end=' ', flush=True)

def build_text_cache(type, skip, limit):
    # not in use. uses too much memory

    col = DB.bibs if type == 'bib' else DB.auths
    text_cache = {}
    range_match = {"$and": [{"_id": {"$gte": skip}}, {"_id": {"$lte": limit}}]}

    result = col.aggregate(
        [
            {"$skip": skip},
            {"$limit": limit},
            {"$project": {"data": {"$objectToArray": "$$ROOT"}}},
            {"$project": {"data": "$data.k"}},
            {"$unwind": "$data"},
            {"$group": {"_id": None, "keys": {"$addToSet": "$data"}}},
        ]
    )

    tags = filter(lambda x: re.match('\d{3}', x), list(result)[0]['keys'])

    for tag in sorted(tags):
        if tag in ('949', '998', '999'):
            continue

        tag_col = DB.handle[f'_index_{tag}']

        print(tag + ' ', end='', flush=True)

        for doc in tag_col.find({}, skip=skip, limit=limit):
            text_cache.setdefault(tag, {})
            text_cache[tag].setdefault(doc['_id'], {})
            text_cache[tag][doc['_id']]['subfields'] = doc.get('subfields')

    print('\n')

    return text_cache

if __name__ == '__main__':
    run()