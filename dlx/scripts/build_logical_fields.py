import sys
from pymongo import UpdateOne, ASCENDING as ASC, DESCENDING as DESC
from argparse import ArgumentParser
from dlx import DB, Config
from dlx.marc import BibSet, Bib, AuthSet, Query

parser = ArgumentParser()
parser.add_argument('--connect')
parser.add_argument('--start', default=0)
  
def run():
    args = parser.parse_args()
    
    if not DB.connected:
        DB.connect(args.connect)
    
    tags = []
    
    for x in list(Config.bib_logical_fields.values()) + list(Config.auth_logical_fields.values()):
        tags += list(x.keys())
        
    c = r = start = int(args.start)
    inc = 1000
    
    for cls in BibSet, AuthSet:
        end = cls.from_query({}).count
        
        for i in range(start, end, inc):
            updates = []
            
            for record in cls.from_query({}, sort=[('_id', DESC)], skip=i, limit=inc, projection=dict.fromkeys(tags, 1)):
                for field, values in record.logical_fields().items():
                    updates.append(UpdateOne({'_id': record.id}, {'$set': {field: values}}))
                    
                last_r = r
                r += 1
                print('\b' * (len(str(last_r)) + len(str(end)) + 3) + f'{r} / {end}', end='', flush=True)
                    
            if updates:
                cls().handle.bulk_write(updates)
                c += len(updates)

    print(f'updated {c} records')
    
###

if __name__ == '__main__':
    run()