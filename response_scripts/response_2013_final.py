import response_functions
import warnings
warnings.filterwarnings('ignore')
import pymongo
import pandas as pd
# initialize path directories
raw_data_directory = '/home/rsampana/raw_data/'
path_to_attachments = '/mex-data'

def connect_mongo():
    try:
        conn = pymongo.MongoClient()
        name = 'polisci_final'
        db = conn[name]
        folios = db.folios
        responses = db.responses
        return conn,db,folios,responses
    except:
        print "Failed Connection"

# connect to db
conn,db,folios,responses = connect_mongo()        
    
# query 
cursor = db['folios'].find({'year' : 2013,'ARCHIVORESPUESTA': {'$regex': 'respuesta='} })

# load to pandas dataframe
df = pd.DataFrame(list(cursor))

# close the mongodb connection. we will be making multiple connections in parallel reponse
conn.close()

from concurrent.futures import ProcessPoolExecutor

pool  = ProcessPoolExecutor()

def parallel_response(folio):
    
    # connect to db
    conn,db,folios,responses = connect_mongo()

    # find one entry of Folio_id containing object_id
    r_dict = responses.find_one({'Folio_id': folio['_id']})
    
    # logic to hand if the Folio_id is already in the response database
    if r_dict != None:
        # Folio_id already exits, we need to update
        update = response_functions.build_update_request(r_dict,responses)
        
        # close connection
        conn.close()
        
        return update
    else:
        # Folio_id does not exits, we need to build the initial request
        insert = response_functions.build_initial_request(folio,responses)
        
        # close connection
        conn.close()
        
        return insert

# folios list
print "building folios list"
folios = []

for row in df.iterrows():
    folios.append(row[1])
    
print "started downloads"    
# star the mapping of the jobs to the pool    
results = list(pool.map(parallel_response,folios))

print "DONE"