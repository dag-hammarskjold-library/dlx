
# This script (re)builds the text index collections.
# This can take a large amount of time. If necessary, use the --start parameter
# to stop the script and start from where you left off.
# This can be run at any time, even if the fields are already built, without 
# detriment to the data.

import sys, os, time
from pymongo import UpdateOne
from argparse import ArgumentParser
from dlx import DB, Config
from dlx.marc import BibSet, AuthSet

parser = ArgumentParser()
parser.add_argument('--connect', required=True)
parser.add_argument('--type', required=True, choices=['bib', 'auth'])
parser.add_argument('--start', default=0)

def run():
    args = parser.parse_args()
    DB.connect(args.connect)
    cls = BibSet if args.type == 'bib' else AuthSet

    start = int(args.start)
    end = cls.from_query({}).count
    inc = 1000
    tags = set()
    exclude_fields = list(Config.bib_logical_fields.keys() if args.type == 'bibs' else Config.auth_logical_fields.keys()) \
        + ['user', 'updated']

    for i in range(start, end, inc):
        print(f'{i+1}-{i+inc}', end='', flush=True)

        updates, start_time = {}, time.time()

        for record in cls.from_query({}, skip=i, limit=inc, projection=dict.fromkeys(exclude_fields, 0)):
            for field in record.datafields:
                tags.add(field.tag)

                for s in field.subfields:
                    if s.value is None:
                        print(f'\n{record.id}')

                text = ' '.join(filter(None, [x.value for x in field.subfields]))
                updates.setdefault(field.tag, [])
                updates[field.tag].append(UpdateOne({'_id': text}, {'$setOnInsert': {'_id': text}}, upsert=True))
                
                for s in field.subfields:
                    updates[field.tag].append(UpdateOne({'_id': text}, {'$addToSet': {'subfields': {'code': s.code, 'value': s.value}}}))

        print('\u2713', end='', flush=True)

        for tag in updates.keys():
            col = DB.handle[f'_index_{tag}']

            for index in col.list_indexes():
                if index['name'] == 'subfields.value_text':
                    if index['default_language'] != 'none':
                        # old indexes
                        col.drop_index('subfields.value_text')
                    else:
                        found = True

            if not found:    
                col.create_index([('subfields.value', 'text')], default_language='none')
            
            col.bulk_write(updates[tag])
        
        print('\u2713:' + str(int(time.time() - start_time)) + 's', end=' ', flush=True)

if __name__ == '__main__':
    run()