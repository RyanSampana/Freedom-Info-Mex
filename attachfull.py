import os
import folio_insert_final as fif
import pymongo
import time
from concurrent.futures import ProcessPoolExecutor

# define path variables
raw_data_directory = '../raw_data/'
path_to_attachments = '../att/'

e = ProcessPoolExecutor()

# generate a list of filenames to process concurrently        
filenames = []
for filename in os.listdir(raw_data_directory): 
    # we have already inserted 2003, lets pass on inserting it again
    if filename == 'attachdata2003.csv':
        pass
    else:
        filenames.append(os.path.join(raw_data_directory, filename))
    
def parrallel_insert(filename,raw_data_directory = '../raw_data/',path_to_attachments = '../att/'):
    try:
        # create single mongodb connection
        conn = pymongo.MongoClient()
        
        # connect to the db name
        name = 'polisci_test'
        db = conn[name]
        
        # connect to folios
        folios = db.folios
        responses = db.responses
        
        print "Connection Succesfull"
        
        print "Working on {}".format(filename)
        path_to_csv = os.path.join(raw_data_directory, filename)
        
        # print "Converting {} to CSV".format(filename)
        df = fif.import_folio_csv(path_to_csv)
        
        # count the updates and inserts these should be equal
        updates = []
        inserts = []
        
        # being the inserts
        for row in df.iterrows():
            folio = row[1]
            update,insert = fif.mongo_insert_folio(folio,folios,path_to_attachments)
            updates.append(update)
            inserts.append(insert) 
            # if there is an attachment lets include it
            if folio['attach_inc'] == 1:
                
                # look for the folio
                r = folios.find_one({'FOLIO' : folio['FOLIO']})
                # return path_to_file
                path_to_file = r['path_to_file']
                path_to_file = path_to_file + "/attachedpdf.txt"

                # have text
                text = str(folio['attach_full'].encode('UTF-8'))

                # write to file
                f = open(path_to_file, 'wb+')
                f.write(text)
                f.close()   
                
        print "FINISHED WITH {}".format(filename)
        
        # close the connection
        conn.close()
        
        return inserts
    except:
        print "Conn Unsuccessful"    

print filenames
# star the mapping of the jobs to the pool    
inserts = list(e.map(parrallel_insert,filenames)) 

print inserts