
# This script (re)builds the text index collections.
# This can take a large amount of time. If necessary, use the --start parameter
# to stop the script and start from where you left off.
# This can be run at any time, even if the fields are already built, without 
# detriment to the data.

import sys, os, time
from collections import Counter
from pymongo import UpdateOne
from argparse import ArgumentParser
from dlx import DB, Config
from dlx.marc import BibSet, AuthSet
from dlx.util import Tokenizer

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

    # chunks
    for i in range(start, end, inc):
        print(f'{i+1}-{i+inc}', end='', flush=True)

        # records
        updates, record_updates, start_time = {}, [], time.time()

        for record in cls.from_query({}, skip=i, limit=inc, projection=dict.fromkeys(exclude_fields, 0)):
            all_text = []

            for field in record.datafields:
                tags.add(field.tag)

                for s in field.subfields:
                    if s.value is None:
                        print(f'\n{record.id}')

                # concatenated subfield text
                text = ' '.join(filter(None, [x.value for x in field.subfields]))
                scrubbed = Tokenizer.scrub(text)
                all_text.append(scrubbed)
                updates.setdefault(field.tag, [])
                updates[field.tag].append(UpdateOne({'_id': text}, {'$setOnInsert': {'_id': text}}, upsert=True))

                # individual subfields
                for s in field.subfields:
                    updates[field.tag].append(
                        UpdateOne(
                            {'_id': text},
                            {'$addToSet': {'subfields': {'code': s.code, 'value': s.value}}},
                            #upsert=True
                        )
                    )

                # text
                words = Tokenizer.tokenize(text)
                count = Counter(words)
                updates[field.tag].append(
                    UpdateOne(
                        {'_id': text}, 
                        {'$set': {'words': list(count.keys()), 'word_count': [{'stem': k, 'count': v} for k, v in count.items()]}},
                    )
                )

            # all-text 
            #all_text_col = DB.handle[f'__index_{record.record_type}s']
            all_text = ' '.join(all_text)
            all_words = Tokenizer.tokenize(all_text)
            count = Counter(all_words)
            
            record_updates.append(
                UpdateOne(
                    {'_id': record.id},
                    {'$set': {'text': all_text, 'words': list(count.keys()), 'word_count': [{'stem': k, 'count': v} for k, v in count.items()]}},
                )
            )

        print('\u2713', end='', flush=True)

        for tag in updates.keys():
            col = DB.handle[f'_index_{tag}']
            found = False

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

        record_col = DB.handle[args.type + 's']
        record_col.bulk_write(record_updates)
        
        print('\u2713:' + str(int(time.time() - start_time)) + 's', end=' ', flush=True)

if __name__ == '__main__':
    run()