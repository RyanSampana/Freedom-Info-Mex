import pymongo
import os
import pandas as pd
import requests
from collections import defaultdict
import magic
from bson.objectid import ObjectId
import datetime
import time
from time import sleep


def build_update_request(response_dictionary,response_collection):
    """ Build update """
    url = response_dictionary['url']
    dl_path = response_dictionary['path_to_file']
    dl_path,_ = os.path.split(dl_path)
    dl_path = dl_path+"/"
    try:
        response_code,path_to_download,time_stamp,f = download_file(url,dl_path)
    except:
        print "Server is down... Url: {} is not working".format(url)
        pass
        
    m_result = response_collection.update_one({'Folio_id': response_dictionary['Folio_id']}, 
                             {'$push': { 'pings': {'$each': [{
                            'time':time_stamp,
                            'response_code':response_code,
                            'file': str(f)}]}},
                              '$set': {'Last_Modified': time_stamp}})
    return m_result


def build_initial_request(folio,mongo_response):
    """ Build the initial request document to be placed in mongodb """
    url = folio['ARCHIVORESPUESTA']
    # build path to file
    path_to_file = validate_path(folio)
    path_to_file = path_to_file+str(folio['_id'])+"/"
    
    #download file
    try:
        response_code,path_to_download,time_stamp,filename = download_file(url,path_to_file)
    except:
        print "Server is down... Url: {} is not working".format(url)
        pass
    
    # dictionary object to insert into mongodb
    new_dict = { "Folio_id": ObjectId(str(folio['_id'])),
                "path_to_file": str(path_to_download),
                "url" : str(folio['ARCHIVORESPUESTA']),
                "pings" : [{'time': time_stamp,'response_code' : response_code,'file' : filename}],
                "Last_Modified" : time_stamp }
    
    insert = mongo_response.insert_one(new_dict)
    
    return insert


def validate_path(folio):
    """ Given a folio, we determine if there exis
    a path to file. If there is a path to file,
    great out put that path. If there isn't, make that path and return it"""
        path_to_file = folio['path_to_file']

        if path_to_file == None:
            # initialize variables
            year = str(folio['year'])
            object_id = str(folio['_id'])
            path_year = path_to_attachments+year+'/'

            # check path to the year
            try:
                os.stat(path_year)
            except:
                os.mkdir(path_year)

            # check path to the object_id
            path_to_file = path_year+object_id+'/'
            try:
                os.stat(path_to_file)
            except:
                os.mkdir(path_to_file)
            
            return path_to_file
        
        else:
            path_to_file,_ = os.path.split(str(folio['path_to_file']))
            path_to_file = path_to_file+'/'
            return path_to_file
        

def what_is_file_extension(magicFile):
    """Determine the file_extension of a magicFile and return the extesion"""
    import re
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
        return ""
    

def download_file(url,path_to_attachments, sleep_delay=0.05):
    """ Given a url and path to the attachments folder, we download the file and return the response_code,path 
    to the downloaded file, time_stamp of the download."""
    a_request = requests.get(url, verify = False)
    response_code = a_request.status_code
    if response_code == 200:
        
        file_name = url.split('/')[-1]
        with open(str(path_to_attachments)+'/'+file_name,'wb') as f:
            for chunk in a_request.iter_content(chunk_size=1024): 
                if chunk: # filter out keep-alive new chunks
                    f.write(chunk)
        
        # get file extension 
        file_extension = what_is_file_extension(str(path_to_attachments)+'/'+file_name)
        
        # add timestamp to file name
        time_stamp = datetime.datetime.now()
        ts = time_stamp.strftime("%d-%m-%Y-%H-%M-%S")
        new_file_name = 'response'+'_date_'+str(ts)+str(file_extension)
        path_to_download = str(path_to_attachments)+new_file_name
        
        # rename old filename to new filename, with date tag and file extension included
        os.rename(str(path_to_attachments)+'/'+file_name, path_to_download)
        filename = new_file_name
    else:
        time_stamp = datetime.datetime.now()
        path_to_download = None
        filename = None
    sleep(sleep_delay)
    return response_code,path_to_download,time_stamp,filename