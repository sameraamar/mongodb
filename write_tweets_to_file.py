# -*- coding: utf-8 -*-
"""
Created on Sun Aug 28 03:38:50 2016

@author: SAMERA
"""

"""
split a collection
"""


#%%

import configparser

CONF_INI_FILE = 'c:/data/conf.ini'

#conf.ini should look like this (in c:/temp folder)
#[DEFAULT]
#consumer_key = <key>
#consumer_secret = <secret>
#access_key = <key>
#access_secret = <secret>
#
#; default is localhost:27017 for mongodb
#mongodb_host = localhost
#mongodb_port = 27017

def load_config(section='DEFAULT'):
    config = configparser.ConfigParser()
    config.read(CONF_INI_FILE)
    
    
    default = config['DEFAULT']
    host = default['mongodb_host']
    port = default['mongodb_port']    

    return host, port


#%%

from pymongo import MongoClient
from pymongo.errors import BulkWriteError

def copy_db():    
    
    host, port = load_config()
    
    client = MongoClient(host, int(port))
    #client = MongoClient("mongodb://"+host+":"+port)
    
    db1 = client['events2012-a']
    dbcoll = db1.ids

    db2 = client['events2012-a']
    db2coll = db2.errors
    
    
    count = 0
    bulk = None
    try:
        cursor = dbcoll.find({'status' : 'Error'}) #.limit(10011)
        for doc in cursor:
            #tmp = {}
            #tmp['_id'] = doc['_id']
            #tmp['user_id'] = doc['user_id']
            #
            #tmp['json'] = doc['json']
            tmp = doc
        
            if bulk == None:
                bulk = db2coll.initialize_unordered_bulk_op()
                   
            bulk.insert(tmp)
            count+=1


            if count % 10000==0:


                try:
                    bulk.execute()
                except BulkWriteError as bwe:
                    #print(bwe.details)
                    werrors = bwe.details['writeErrors']
                    print('Duplicates: ' , len(werrors))
                    pass


                bulk = None
                print('Wrote ', count)
                
        if bulk != None:
            try:
                bulk.execute()
            except BulkWriteError as bwe:
                #print(bwe.details)
                werrors = bwe.details['writeErrors']
                print('Duplicates: ' , len(werrors))
                pass

#        db2coll = db2.ids
#        cursor = dbcoll.find({}) 
#        for doc in cursor:
#            tmp = {}
#            tmp['_id'] = doc['_id']
#            tmp['user_id'] = doc['user_id']
#            tmp['status'] = doc['status']
#            
#            #tmp = doc
#        
#            if bulk == None:
#                bulk = db2coll.initialize_unordered_bulk_op()
#                   
#            bulk.insert(tmp)
#            count+=1
#
#
#            if count % 10000==0:
#
#
#                try:
#                    bulk.execute()
#                except BulkWriteError as bwe:
#                    #print(bwe.details)
#                    werrors = bwe.details['writeErrors']
#                    print('Duplicates: ' , len(werrors))
#                    pass
#
#
#                bulk = None
#                print('Wrote ', count)
#                
#        if bulk != None:
#            try:
#                bulk.execute()
#            except BulkWriteError as bwe:
#                #print(bwe.details)
#                werrors = bwe.details['writeErrors']
#                print('Duplicates: ' , len(werrors))
#                pass
            
    

    except:
        if doc != None:
            print(doc['_id'])
        raise
    
    print('Finished!')


#%%
from pymongo import MongoClient
import pymongo
import codecs
import json


def writeToCSVFile():

    
    host, port = load_config()
    
    client = MongoClient(host, int(port))
    #client = MongoClient("mongodb://"+host+":"+port)
    
    db = client.twitter_db
    dbcoll = db['2016-08-24-2016-08-25-Italy']
    
    out = codecs.open('c:/temp/posts.csv', 'w', 'utf-8')
    
    count = 0
    cursor = dbcoll.find({'status':'Loaded'}).limit(5000)
    #cursor = dbcoll.find({'status':'Loaded'}).sort([('_id', pymongo.DESCENDING)]).limit(500000)
    for doc in cursor:
        text = json.dumps(doc)
        out.write(text)
#        out.write(doc['json']['id_str']) 
#        out.write('\t')
#        out.write(doc['json']['text'].replace('\t', ' ').replace('\n', ' ')) 
        out.write('\n')
        count+=1
        if count % 1000==0:
            print('Wrote ', count)
    
    print('Finished!')
    out.close()


import gzip

def writeToJSONFile():
    host, port = load_config()
    
    client = MongoClient(host, int(port))
    #client = MongoClient("mongodb://"+host+":"+port)
    
    db = client.twitter_db
    dbcoll = db['2016-08-24-2016-08-25-Italy']
    
    #out = gzip.open('c:/temp/Italy2.json', mode='wt', encoding='utf-8')
    
    out = codecs.open('c:/temp/Italy1.json', 'w', 'utf-8')
    
    count = 0
    cursor = dbcoll.find({}).sort([('_id', pymongo.ASCENDING)])
    #cursor = dbcoll.find({'status':'Loaded'}).sort([('_id', pymongo.DESCENDING)]).limit(500000)
    for doc in cursor:
        text = json.dumps(doc['json'])
        #out.write(doc['json']['id_str'] + '\t')
#        out.write(doc['json']['created_at'] + '\t')
        out.write(text)
#        out.write(doc['json']['id_str']) 
#        out.write('\t')
#        out.write(doc['json']['text'].replace('\t', ' ').replace('\n', ' ')) 
        out.write('\n')
        count+=1
        if count % 1000==0:
            print('Wrote ', count)

    
    print('Finished!')
    out.close()
    
#%%    
#copy_db()
writeToJSONFile()

#writeToFile()
