# Freedom-Info-Mex
Working with Professor Aaron Erlich and Genevieve Fried on Freedom of Information Requests made to the Mexican Government.

## Class Structure


## Files

### Legacy import

Files and functions to important legacy work that download original folio data. Unless serious replication exercise is going on, they should not need to be ever run
`folio_insert_final.py` 
  - `mongo_database_setup()` connects to mongodb, returns connection, folios, and respons objects
  `response` objects are the infomex responses to requests. 
  -`import_folio_csv()` imports original csv and returns parsed dataframe to insert into Mongo
  - `attach_to_text()` Removes attached text from datafram and puts it into a textfile and puts a path to that textfile. 
  - `mong_insert_df()` insert the imported df into mongo
  
  
  `Folio_Insert_One.ipynb` This is a Python Notebook that demonstrates the importation process for one year of data. 
  `Folio_Insert_All_Data.ipynb` This file as the main important file and ru all the importation code for all  years. T
    -script uses parallelization to import multiple years over multiple thread to speed up the process.
    
#sdfsdfsdf
    
    
  
  
