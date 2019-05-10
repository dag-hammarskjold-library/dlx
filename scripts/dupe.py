
import sys, re, json, codecs
from dlx import DB, Auth
from dlx.query import match_value

DB.connect('mongodb://undlFilesAdmin:password@18.235.152.183:8080/?authSource=undlFiles')

cur = Auth.find (
    {    
        '$and' : [
            {
                '100.subfields.code' : 'a'
            },
            #{
            #    '100.subfields.code' : {
            #        '$not' : {
            #            '$eq' : 'z'
            #        }
            #    }
            #}
        ]
    }
)

index = {}
file = codecs.open('dupes.tsv','w','utf-8')

#for marc in [next(cur) for i in range(0,100)]:
for marc in cur:
    val = marc.get_value('100','a')
    g = marc.get_value('100','g')
    
    if g is None: 
        g = ''
    else:
        g = '(g: {})'.format(g)
        #print(g)
    
    try:
        index[val].append(marc.id)
    except KeyError:
        index[val] = [marc.id + g]

for val in index.keys():
    ids = [str(x) for x in index[val]]
    
    if len(ids) > 1:
        p = [val] + [x for x in ids]
        
        file.write('\t'.join(p) + '\n')
        
