import pymongo
import os
import pandas as pd
import requests
from collections import defaultdict
import magic

""" Given a folio, we determine if there exists a path to file. If there is a path to file,
great out put that path. If there isn't, make that path and return it"""
def validate_path(folio):
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
            path_to_file,_ = os.path.split(str(row[1]['path_to_file']))
            path_to_file = path_to_file+'/'
            return path_to_file
        
"""Determine the file_extension of a magicFile and return the extesion"""
def what_is_file_extension(magicFile):
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
    
""" Given a url and path to the attachments folder, we download the file and return the response_code,path to the downloaded file, time_stamp of the download."""
def download_file(url,path_to_attachments):
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
        ts = time_stamp.strftime("%d-%m-%Y")
        path_to_download = str(path_to_attachments)+'response'+'_date_'+str(ts)+str(file_extension)
        
        # rename old filename to new filename, with date tag and file extension included
        os.rename(str(path_to_attachments)+'/'+file_name,
                  str(path_to_attachments)+'response'+'_date_'+str(ts)+str(file_extension))
    else:
        time_stamp = datetime.datetime.now()
        path_to_download = None
    
    return response_code,path_to_download,time_stamp