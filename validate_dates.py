import pandas as pd
import os

# define path variables
raw_data_directory = '../raw_data/'
path_to_attachments = '../attachments/'

def validate_date(filename):
    errors = []
    print "Validating {}".format(filename)
    for x in range(len(df)):
        try:
            y = pd.to_datetime(df['FECHASOLICITUD'].iloc[x],infer_datetime_format=True)
            y = y.to_datetime().strftime('%Y-%m-%d %H:%M:%S')    
            z = y = pd.to_datetime(df['FECHARESPUESTA'].iloc[x],infer_datetime_format=True)
            z = z.to_datetime().strftime('%Y-%m-%d')
        except:
            xs.append((filename,x))
            print "Error with date for file {} at index {}".format(filename,x)
    return errors
errors = []
            
for filename in os.listdir(raw_data_directory):
    if filename.endswith(".csv"):
        full_path = os.path.join(raw_data_directory, filename)
        errors.append(validate_date(full_path))

print errors