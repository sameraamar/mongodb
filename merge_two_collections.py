# -*- coding: utf-8 -*-
"""
Created on Fri Aug 26 03:56:25 2016

@author: SAMERA
"""

"""
merge two mongodb collections
"""

from pymongo import MongoClient
from pymongo.errors import BulkWriteError


host = 'localhost'
port = 27017
client = MongoClient(host, int(port))

src_dbname = 'twitter_db'
trgt_dbname = 'twitter_db'

src_coll = '2016-08-23-2016-08-25'
trgt_coll = '2016-08-23-2016-08-24'


src_db = client[src_dbname]
trgt_db = client[trgt_dbname]


src = src_db[src_coll]
trgt = trgt_db[trgt_coll]

trgt_count = trgt.count()
print('Source: ', src.count(), ' target: ', trgt_count)        

cursor = src.find({})
c = 0
inserts = 0
duplicates = 0

bulk = None
for s in cursor:
    c+=1
    
    if bulk==None:
        bulk = src.initialize_unordered_bulk_op()
    
#    res = bulk.find({'_id': s['_id']})
#    if res.count() == 0:
#        bulk.insert(s)
#        inserts+=1
#    else:
#        duplicates+=1

 
    bulk.insert(s)


#    res = trgt.find({'_id': s['_id']})
#    if res.count() == 0:
#        trgt.insert_one(s)
#        inserts+=1
#    else:
#        duplicates+=1
    
    #ruff = int(5.0/100*src.count())
    
    if c % 2500==0:
        perc = 100.0*c/src.count()
        try: 
            result = bulk.execute()
            inserts += 2500
        except BulkWriteError as bwe:
            inserts += bwe.details['nInserted']
            duplicates += len(bwe.details['writeErrors'])
        bulk = None
        
        print('Handled (', c ,') out of (',src.count(),') which is %', perc)
        #res.remove()

if bulk != None:
    try: 
        result = bulk.execute()
        inserts += 1000
    except BulkWriteError as bwe:
        inserts += bwe.details['nInserted']
        duplicates += len(bwe.details['writeErrors'])

print('Source: ', src.count(), ' target: ', trgt.count(), '. was (',trgt_count,')')     
print('Inserted: ', inserts, ', duplicates: ', duplicates)   
print('You can safely remove ', src_dbname,'.', src_coll)


#src.delete_many({})

