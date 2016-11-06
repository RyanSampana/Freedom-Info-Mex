import re
import os
import itertools
import pandas as pd
import numpy as np
import datetime
import time
from bson.objectid import ObjectId
import pymongo

# inital mongodb_setup
def mongo_database_setup():
    database_name={}

    # Try to connect to MongoDB,  exit if not successful.
    try:
        conn=pymongo.MongoClient()
        print "Connected successfully!!!"
        
    except pymongo.errors.ConnectionFailure, e:
        print "Could not connect to MongoDB: %s" % e 


    #Use todays date for the database name:
    name='polisci_test'+time.strftime("%d%b%y")

    if name in conn.database_names():
        conn.drop_database(name) #Drop the database if it exists
        db = conn[name] #Create the database
        
        #Create two collections in the database
        folios = db.folios
        responses=db.responses
   
    else:
        db = conn[name] #Create the database
        #Create two collections in the database
        folios = db.folios
        responses = db.responses
   
    #return the connection, database name, collections names.     
    return conn,db,folios,responses

# this function will return a pandas dataframe ready to be uploaded to mongodb
def import_csv(path_to_file):
    # read file
    directory,file_name = os.path.split(path_to_file)
    df = pd.read_csv(path_to_file,encoding='latin-1')
    year = int(file_name[-8:-4])
    # extract columns we want
    df = df[["FOLIO","FECHASOLICITUD","TIPOSOLICITUD","DEPENDENCIA",
               "ESTATUS","MEDIOENTRADA","DESCRIPCIONSOLICITUD","OTROSDATOS",
               "ARCHIVOADJUNTOSOLICITUD","RESPUESTA","FECHARESPUESTA","PAIS","ESTADO",
               "MUNICIPIO","CODIGOPOSTAL","SECTOR","PDFOCR","ARCHIVORESPUESTA",
               "attach_full","attach.inc","word.count"]]
    
    # new column names
    columns = ["FOLIO","FECHASOLICITUD","TIPOSOLICITUD","DEPENDENCIA",
               "ESTATUS","MEDIOENTRADA","DESCRIPCIONSOLICITUD","OTROSDATOS",
               "ARCHIVOADJUNTOSOLICITUD","RESPUESTA","FECHARESPUESTA","PAIS","ESTADO",
               "MUNICIPIO","CODIGOPOSTAL","SECTOR","PDFOCR","ARCHIVORESPUESTA",
               "attach_full","attach_inc","word_count"]
    df.columns = columns
    
    # add the year to column
    list_years = list(itertools.repeat(year,len(df)))
    df['year'] = list_years
    
    # convert columns to date and time
    df['FECHASOLICITUD'] = pd.to_datetime(df['FECHASOLICITUD'],format="%d/%m/%Y %H:%M")
    df['FECHARESUPESTA'] = pd.to_datetime(df['FECHASOLICITUD'],format="%d/%m/%Y")

    return df

# function replaces the included attachments to a text file and returns a new df with a new column to the file instead
def attach_to_text(df,path):
    paths = []
    for row in df.iterrows():
        is_null = str(row[1]['attach_full'].encode('UTF-8'))
        is_null = is_null.replace(" ","")
        if is_null == "":
            # there is no included attachment so there doesn't exist a path to a file
            paths.append(None)
        else:
            # save 
            text = str(row[1]['attach_full'].encode('UTF-8'))
            year = row[1]['year']
            file_name = 'attach_pdf'
            directory_year = path+str(year)+"/"
            
            # if year directory doesn't exist, create it
            try:
                os.stat(directory_year)
            except:
                os.mkdir(directory_year) 
            directory = path+str(year)+'/'+str(row[1]['FOLIO'])+'/'
            
            # if folio directory doesn't exist create it
            try:
                os.stat(directory)
            except:
                os.mkdir(directory)      
            
            path_to_file = str(directory)+str(file_name)+'.txt'    
            f = open(path_to_file, 'wb+')
            f.write(text)
            f.close()            
            paths.append(path_to_file)
            
    # no need of the attach_full frame delete it and return df       
    del df['attach_full']
    del df['attach_inc']
    del df['word_count']
    df['path_to_file'] = paths
    return df        

# here we take the data frame and transform it to a nice document format for mongodb to ingest
def mongod_insert_df(df,collection):
    for row in df.iterrows():
    	# doing dictionary inserts here
        new_dict = {}
        new_dict['FOLIO'] = row[1]['FOLIO']
        new_dict['FECHASOLICITUD'] = datetime.datetime.strptime(str(row[1]['FECHASOLICITUD']),"%Y-%m-%d %H:%M:%S")
        new_dict['TIPOSOLICITUD'] = row[1]['TIPOSOLICITUD']
        new_dict['DEPENDENCIA'] = row[1]['DEPENDENCIA']
        new_dict['ESTATUS'] = row[1]['MEDIOENTRADA']
        new_dict['MEDIOENTRADA'] = row[1]['MEDIOENTRADA']
        new_dict['DESCRIPCIONSOLICITUD'] = row[1]['DESCRIPCIONSOLICITUD']
        new_dict['OTROSDATOS'] = row[1]['OTROSDATOS']
        new_dict['ARCHIVOADJUNTOSOLICITUD'] = row[1]['ARCHIVOADJUNTOSOLICITUD']
        new_dict['RESPUESTA'] = row[1]['RESPUESTA']
        new_dict['FECHARESPUESTA'] = datetime.datetime.strptime(str(row[1]['FECHARESPUESTA']),'%d/%m/%Y')
        new_dict['SECTOR'] = row[1]['SECTOR']
        new_dict['PDFOCR'] = row[1]['PDFOCR']
        new_dict['ARCHIVORESPUESTA'] = row[1]['ARCHIVORESPUESTA']
        new_dict['year'] = row[1]['year']
        new_dict['LOCALIDAD'] = {'PAIS' : row[1]['PAIS'], 
                                 'ESTADO' : row[1]['ESTADO'], 
                                 'MUNICIPIO' : row[1]['MUNICIPIO'],
                                 'CODIGOPOSTAL' : row[1]['CODIGOPOSTAL']                         
                                }
        new_dict['path_to_file'] = row[1]['path_to_file']
        
        # we are now ready to insert to mongodb
        insert_result = collection.insert_one(new_dict)
        object_id = str(insert_result.inserted_id)

        # we have to handle the path_to_file now
        # we want ./year/ObjectId/
        if new_dict['path_to_file'] != None:
            # get path name
            pathname = new_dict['path_to_file']
            # get path to folio_id file
            d,f = os.path.split(pathname)
            # get directory of folio_id
            r1 = os.path.dirname(d)
            # rename folio_id file to object_id file
            os.rename(d,r1+"/"+object_id)
            new_path = r1+"/"+object_id+"/"+f
            path_update = collection.update_one(
                {"_id": ObjectId(object_id)},
                {"$set": {"path_to_file": str(new_path) }}
            )
    return insert_result,path_update

## Main
raw_data_directory = "./"
attachment_directory = "../attachments"

# I've got to finish this off
print "Import CSV"
df = import_csv('../full_request_data/attachdata2014.csv')
print "Cleaning Data"
df = attach_to_text(df,'./')
print "Uploading to Mongodb"
i,p = mongod_insert_df(df,folios)
