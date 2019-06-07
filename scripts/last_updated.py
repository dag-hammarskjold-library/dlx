
import sys, re
from dlx import DB, Bib, Auth

db = DB.connect(sys.argv[1])

pipeline = [
    {
        '$unwind' : '$998'
    },
    {
        '$unwind' : '$998.subfields'
    },
    {
        '$match' : {
            '998.subfields.code' : 'z'
        }
    },
    {
        '$group' : {
            '_id' : '$998.subfields.value'
        }
    },
    {
        '$sort' : {
             '_id' : -1
        }
    },
    {
        '$limit' : 1
    }
]

for doc in db.auths.aggregate(pipeline): 
    print('Largest value in 998$z : ' + doc['_id'])


