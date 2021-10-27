import sys, pymongo
from argparse import ArgumentParser
from dlx import DB, Config
from dlx.marc import BibSet, Bib, AuthSet, Query

parser = ArgumentParser()
parser.add_argument('--connect')

if __name__ == '__main__':
    run()

###
  
def run():
    args = parser.parse_args()
    
    if not DB.connected:
        DB.connect(args.connect)
    
    i = 0
      
    for cls in BibSet, AuthSet:
        for record in cls.from_query({}):
            for field, values in record.logical_fields().items():
                cls.handle.update_one({'_id': record.id}, {'$set': {field: values}})
                i += 1
        
    return(i)