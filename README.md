# Freedom-Info-Mex
Working with Professor Aaron Erlich and Genevieve Fried on Freedom of Information Requests made to the Mexican Government.

## Scripts Structure
`response_functions` is package with various functions to help deal with response data
  - `built_initial_request` 
  - `build_update_request`
  - `download_file` Downloads and parsed file adding metadata included `time_stamp`, `path_to_file`, and the `file_name`. It also adds a time delay. 
  - `while_is_file_extension` Determines the file type of the downloaded response (could refactored for request data as well). The extension is not stored separately but any download is adjusted to have the appropriate file extension if `download_file`

`file_functions` is a package to extract metadata from downloaded files. It will be used to help text conversion further down the pipeline
  - `get_metadata_pdf()` 
    - getModDatPdf()
  -`get_metadData_not_pdf()`

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
    
### Download Response Data
Files and functions to download the Mexican government's rsponses. While inefficient, because of errors, currently this is done as a separate script for each year where file is `response_####_final.py`
these are located in `\response_scripts`
  -`connect_mongo()`
  -`parallel_response()` Parallelizes the pinging of the info-mex to download response data. 
`response_inserts.py` runs `response_functions.py`. 



    
    
  
  
