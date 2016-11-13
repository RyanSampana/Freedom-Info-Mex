import pymongo
import datetime
import pandas as pd
import response
# initialize path directories
raw_data_directory = '../raw_data/'
path_to_attachments = '../attachments/'

# paths '../attachments/years/ObjectId/files

# connect to mongodb
try:
    conn = pymongo.MongoClient()
    name = 'polisci_test12Nov16'
    db = conn[name]
    folios = db.folios
    responses = db.responses
except:
    print "Failed Connection"

# query to find the urls in ARCHIVORESPUESTA
cursor = db['folios'].find({'ARCHIVORESPUESTA': {'$regex': 'respuesta='} })

# load to pandas dataframe
df = pd.DataFrame(list(cursor))

for row in df.iloc[0:5].iterrows():
    folio = row[1]
    # get object_id
    object_id = folio['_id']
    
    # validate the path
    path_to_file = validate_path(folio)

    # get the url from 'ARCHIVORESPUESTA'
    url = folio['ARCHIVORESPUESTA']

    # download the file from the url
    response_code,path_to_download,time_stamp = download_file(url,path_to_file)
    
    # find one entry of Folio_id containing object_id
    r = responses.find_one({'Folio_id': object_id})

    # we determine if response entry already exists
    if r == None:
        # insert the file to mongodb
        insert = build_initial_request(folio,path_to_download,time_stamp,response_code,responses)
    else:
        # build the update
        update = build_update(folio,path_to_download,time_stamp,response_code,responses)