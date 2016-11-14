import re
import os
import itertools
import pandas as pd
import datetime
import time
from bson.objectid import ObjectId
import pymongo
import time

def mongo_database_setup():
    database_name={}

    # Try to connect to MongoDB,  exit if not successful.
    try:
        conn=pymongo.MongoClient()
        print "Connected successfully!!!"
        
    except pymongo.errors.ConnectionFailure, e:
        print "Could not connect to MongoDB: %s" % e 


    #Use todays date for the database name:
    name='polisci_test'

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

# this function will return a pandas dataframe 
def import_folio_csv(path_to_file):
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
    df['FECHASOLICITUD'] = pd.to_datetime(df['FECHASOLICITUD'],infer_datetime_format=True).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['FECHARESPUESTA'] = pd.to_datetime(df['FECHARESPUESTA'],infer_datetime_format=True).dt.strftime('%Y-%m-%d')

    return df

# function replaces the included attachments to a text file and returns a new df with a new column to the file instead
def attach_to_text(df,path_to_attachments):
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
            directory_year = path_to_attachments+str(year)+"/"
            
            # if year directory doesn't exist, create it
            try:
                os.stat(directory_year)
            except:
                os.mkdir(directory_year) 
                
            directory = path_to_attachments+str(year)+'/'+str(row[1]['FOLIO'])+'/'
            
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
    
    # insert path_to_file column in dataframe
    df['path_to_file'] = paths
    
    return df

def mongo_insert_folio(folio,follios_collection,path_to_attachments):
    new_dict = {}
    new_dict['FOLIO'] = folio['FOLIO']
    try:
        new_dict['FECHASOLICITUD'] = datetime.datetime.strptime(str(folio['FECHASOLICITUD']),'%Y-%m-%d %H:%M:%S')
    except:
        new_dict['FECHASOLICITUD'] = None
    new_dict['TIPOSOLICITUD'] = folio['TIPOSOLICITUD']
    new_dict['DEPENDENCIA'] = folio['DEPENDENCIA']
    new_dict['ESTATUS'] = folio['MEDIOENTRADA']
    new_dict['MEDIOENTRADA'] = folio['MEDIOENTRADA']
    new_dict['DESCRIPCIONSOLICITUD'] = folio['DESCRIPCIONSOLICITUD']
    new_dict['OTROSDATOS'] = folio['OTROSDATOS']
    new_dict['ARCHIVOADJUNTOSOLICITUD'] = folio['ARCHIVOADJUNTOSOLICITUD']
    new_dict['RESPUESTA'] = folio['RESPUESTA']
    fecha_respuesta = str(folio['FECHARESPUESTA'])
        # we try to insert as datetime object.
    try:
        new_dict['FECHARESPUESTA'] = datetime.datetime.strptime(str(folio['FECHARESPUESTA']),'%Y-%m-%d')
    except:
        # the gov hasn't responded yet thus there is no date
        new_dict['FECHARESPUESTA'] = None
    new_dict['SECTOR'] = folio['SECTOR']
    new_dict['PDFOCR'] = folio['PDFOCR']
    new_dict['ARCHIVORESPUESTA'] = folio['ARCHIVORESPUESTA']
    new_dict['year'] = folio['year']
    new_dict['LOCALIDAD'] = {'PAIS' : folio['PAIS'], 
                             'ESTADO' : folio['ESTADO'], 
                             'MUNICIPIO' : folio['MUNICIPIO'],
                             'CODIGOPOSTAL' : folio['CODIGOPOSTAL']
                            }
    new_dict['path_to_file'] = folio['path_to_file']
    
    # we are now ready to insert to mongodb
    insert_result = follios_collection.insert_one(new_dict)
    
    # get the object_id
    object_id = str(insert_result.inserted_id)    
    
    # path_year folder
    path_year = path_to_attachments+'/'+str(new_dict['year'])
    try:
        os.stat(path_year)
    except:
        os.mkdir(path_year)
    # path_id folder    
    path_id = path_year+'/'+str(object_id)
    try:
        os.stat(path_id)
    except:
        os.mkdir(path_id)    
        
    # update mongodb    
    update_result = follios_collection.update_one({"_id": ObjectId(object_id)},
                                           {"$set": {"path_to_file": str(path_id)} }
                                          )
    return update_result,insert_result

""" This function takes a pandas dataframe and inserts it into local mongodb"""
def mongod_insert_df(df,follios_collection):
    length_of_df = len(df)
    inserts = []
    for row in df.iterrows():
        new_dict = {}
        new_dict['FOLIO'] = row[1]['FOLIO']
        try:
            new_dict['FECHASOLICITUD'] = datetime.datetime.strptime(str(row[1]['FECHASOLICITUD']),'%Y-%m-%d %H:%M:%S')
        except:
            new_dict['FECHASOLICITUD'] = None
        new_dict['TIPOSOLICITUD'] = row[1]['TIPOSOLICITUD']
        new_dict['DEPENDENCIA'] = row[1]['DEPENDENCIA']
        new_dict['ESTATUS'] = row[1]['MEDIOENTRADA']
        new_dict['MEDIOENTRADA'] = row[1]['MEDIOENTRADA']
        new_dict['DESCRIPCIONSOLICITUD'] = row[1]['DESCRIPCIONSOLICITUD']
        new_dict['OTROSDATOS'] = row[1]['OTROSDATOS']
        new_dict['ARCHIVOADJUNTOSOLICITUD'] = row[1]['ARCHIVOADJUNTOSOLICITUD']
        new_dict['RESPUESTA'] = row[1]['RESPUESTA']
        fecha_respuesta = str(row[1]['FECHARESPUESTA'])
        # we try to insert as datetime object.
        try:
            new_dict['FECHARESPUESTA'] = datetime.datetime.strptime(str(row[1]['FECHARESPUESTA']),'%Y-%m-%d')
        except:
            # the gov hasn't responded yet thus there is no date
            new_dict['FECHARESPUESTA'] = None
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
        # print new_dict
        # we are now ready to insert to mongodb
        insert_result = follios_collection.insert_one(new_dict)
        inserts.append(insert_result)
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
            path_update = follios_collection.update_one({"_id": ObjectId(object_id)},
                                                        {"$set": {"path_to_file": str(new_path) }}
                                                       )
        else: 
            
            # if year directory doesn't exist, create it
            directory_year = '../att/'+str(new_dict['year'])
            try:
                os.stat(directory_year)
            except:
                os.mkdir(directory_year) 
                
            pathname = '../att/'+str(new_dict['year'])+'/'+str(object_id)
            
            # if objectid directory doesn't exist create it
            try:
                os.stat(pathname)
            except:
                os.mkdir(pathname)
                
            path_update = follios_collection.update_one({"_id": ObjectId(object_id)},
                                                        {"$set": {"path_to_file": str(pathname) }}
                                                       )
            
    print 'There was a total of {} out of {}'.format(len(inserts),length_of_df) 
    return inserts,path_update