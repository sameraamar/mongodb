# -*- coding: utf-8 -*-
"""
Created on Fri Aug 26 03:56:25 2016

@author: SAMERA
"""

"""
merge two mongodb collections
"""

from pymongo import MongoClient


host = 'localhost'
port = 27017
client = MongoClient(host, int(port))

src_dbname = 'twitter_db'
trgt_dbname = 'twitter_db'

src_coll = '2016-08-24-2016-08-25-b'
trgt_coll = '2016-08-24-2016-08-25-a'


src_db = client[src_dbname]
trgt_db = client[trgt_dbname]


src = src_db[src_coll]
trgt = trgt_db[trgt_coll]

print('Source: ', src.count(), ' target: ', trgt.count())        

cursor = src.find({})
c = 0
for s in cursor:
    c+=1
    res = trgt.find({'_id': s['_id']})
    if res.count() == 0:
        trgt.insert_one(s)
    
    perc = 100.0*c/src.count()
    ruff = int(5.0/100*src.count())
    
    if c % ruff==0:
        print('Handled (', c ,') our of (',src.count(),') which is %', perc)
        #res.remove()

print('Source: ', src.count(), ' target: ', trgt.count())        

#src.delete_many({})

