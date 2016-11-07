import time
import pymongo
import os
import numpy as np
import pandas as pd
import requests
import magic
from collections import defaultdict

""" A util for making a connection to mongo """
def _connect_mongo(host, port, username, password, db):
    try:
        if username and password:
            mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
            conn = pymongo.MongoClient(mongo_uri)
            
        else:
            conn = pymongo.MongoClient(host, port)

        print "Connected successfully!!!"
        return conn[db]
    
    except pymongo.errors.ConnectionFailure, e:
        print "Could not connect to MongoDB: %s" % e 

""" this inserts a folio """
def insert_folio(db,collection,insert_query={},
                 host='localhost',port=27017,username=None, password=None,):
    # Connect to MongoDB
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)
    return db
    
""" send a query and return the count """   
def mongo_count(db,collection, query={}, host='localhost', port=27017, 
               username=None, password=None, no_id=True):
    # Connect to MongoDB
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)
    
    # With query get the count
    count = db[collection].find(query).count()
    return count

""" Read from Mongo and Store into Pandas DataFrame """
def read_mongo(db, collection, query={}, nb_limit=None, host='localhost', port=27017,
               username=None, password=None, no_id=True):

    # Connect to MongoDB
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)
    
    if nb_limit == None:
        # just query
        cursor = db[collection].find(query)
    else:
        cursor = db[collection].find(query).limit(nb_limit)
    
    df = pd.DataFrame(list(cursor))
    if no_id :
        del df['_id']
    return df


def _download_file(url):
    local_filename = url.split('/')[-1]
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True, verify = False)
    with open('./'+local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
    return local_filename


# we need to change the infilename into something else for the respuestas
def download_file(url,filename,path,extension=""):
    infilename = _download_file(url)
    newname = os.path.splitext(path+filename)[0]+extension
    # yes
    output = os.rename(infilename, newname)
    print "File is located at:",newname
    
    
"""Downloads a stack of urls"""
def download_stack_urls(stackOfUrls,fileNameBase,path):
    count = 0
    length = len(stackOfUrls)
    for url in stackOfUrls:
        r = requests.get(url, verify = False)
        if r.status_code == 200:
            download_file_raw(url,str(fileNameBase)+str(count),path)
        else:
            print r.status_code,"for html url"
            print url
        count+=1
        print "Downloaded",count,"out of",length
        time.sleep(1)
        
def request_to_db(request,db):
    pass

def update_request(request,db):
    pass

def uploadRequest(db):
    pass

"""Determine the file_extension of a magicFile"""
def file_extension(magicFile):
    r = magic.from_file(magicFile)
    if re.search("Microsoft Word",r):
        return ".docx"
    if re.search("Composite Document File V2 Document",r):
        return ".doc"
    if re.search("PDF document",r):
        return ".pdf"
    if re.search("Rich Text Format data",r):
        return ".rtf"
    if re.search("HTML",r):
        return ".html"
    if re.search("Zip",r):
        return ".zip"
    if re.search("ASCII text",r):
        return ".txt"
    else:
        return None

def a_function(document,attachment_directory,afilename):
    object_id = document['_id']
    year = document['year']
    url = document['ARCHIVORESPUESTA']
    path_year = str(attachment_directory)+str(year)+'/'
    path_to_download = str(attachment_directory)+str(year)+'/'+str(object_id) 
    
    try:
        os.stat(path_to_download)
    except:
        os.mkdir(path_to_download)
    
    downloaded_file = download_file(url,afilename,path_to_download)
    extension = what_is_file_extension(str(downloaded_file))
    renamed_file = os.path.join(path_to_download,afilename+str(extension))
    print renamed_file
    print path_to_download
    try:
        os.rename(path_to_download+afilename,renamed_file)
        print "successfuly renamed"
    except:
        print "unsuccessful rename"
        return renamed_file
    
    return renamed_file
        
def build_request_from_folio(folio_document,path_to_file,mongo_request):
    import datetime
    request = {}
    request['folio_id'] = folio_document['_id']
    request['request_url'] = folio_document['ARCHIVORESPUESTA']
    request['date'] = datetime.datetime.now()
    request['path_to_file'] = str(path_to_file)
    result_of_insert = mongo_request.insert_one(request)
    return result_of_insert        