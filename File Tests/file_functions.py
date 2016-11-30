
from collections import defaultdict
from hachoir_metadata import metadata
from hachoir_core.cmd_line import unicodeFilename
from hachoir_parser import createParser
from pdfminer.pdfparser import PDFParser
from pdfminer.pdfdocument import PDFDocument
from time import mktime, strptime
from datetime import datetime

def get_metadata_pdf(f):
    if f[-4:] != '.pdf':
        print "Not A pdf"
        return None
    else:
        fp = open(file_path,'rb')
        parser = PDFParser(fp)
        doc = PDFDocument(parser)
        return doc.info

def getModDatePdf(f):
    if file_path[-4:] != '.pdf':
        print "Not a pdf"
    else:
        fp = open(f,'rb')
        parser = PDFParser(fp)
        doc = PDFDocument(parser)
        datestring = doc.info[0]['ModDate'][2:-7]
        ts = strptime(datestring, "%Y%m%d%H%M%S")
        dt = datetime.fromtimestamp(mktime(ts))
        return dt

def get_metaData_not_pdf(f):
    filename, realname = unicodeFilename(f), f
    parser = createParser(filename)

    # Turn the tags into a defaultdict
    metalist = metadata.extractMetadata(parser).exportPlaintext()
    meta = defaultdict(defaultdict)
    for item in metalist:
        if item[0] != '-':
            top_key = item[:-1]
            meta[top_key] = {}
        else:
            danksplit = item.split(': ')
            key = str(danksplit[0][2:])
            try:
                if meta[top_key][key] == {} and len(danksplit[1:]) == 1:
                    meta[top_key][key] = danksplit[1:][0] 

                elif meta[top_key][key] == {} and len(danksplit[1:]) == 2:
                    meta[top_key][key] = {str(danksplit[1]): danksplit[2]}

                elif meta[top_key][key] != {} and len(danksplit[1:]) == 2:
                    meta[top_key][key][str(danksplit[1])] = danksplit[2]

                else:
                    print danksplit
                    
            except KeyError:
                meta[top_key][key] = {}
                if meta[top_key][key] == {} and len(danksplit[1:]) == 1:
                    meta[top_key][key] = danksplit[1:][0] 

                elif meta[top_key][key] == {} and len(danksplit[1:]) == 2:
                    meta[top_key][key] = {str(danksplit[1]): danksplit[2]}

                elif meta[top_key][key] != {} and len(danksplit[1:]) == 2:
                    meta[top_key][key][str(danksplit[1])] = danksplit[2]

                else:
                    print danksplit
                
    return meta,metalist    