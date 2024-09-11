
#////////////////////////////////////////////////////////////////
#////////////////////////////////////////////////////////////////

#Name: Python Search Terms in Folder(s) of Word Documents
#NOTE: All deatils including file paths, search terms and identifying variables have been obfuscated from the original usecase

#Business Usecase: 
#After input variables are set below, the query runs through the table specified and identifies
#all instances in which any word appears in a word (.dox) document. If the file has a hit, it is moved to a new destination folder.
#Many usescases where this has been useful. Most noteably, in searching through documents for a specific name or merchant to identify a population for audit or investigation purposes.


#////////////////////////////////////////////////////////////////
#////////////////////////////////////////////////////////////////


import docx
import os
import fnmatch
import shutil
import fnmatch
import re

#### Set Varibales

rootPaths = [os.path.join(os.getcwd(),"output")]

pattern = '*.docx'

search_names = ["AUDIT FLAG #1","AUDIT FLAG #2"]

#### Set Functions

def read_docx(file_path):
    try:
        doc = docx.Document(file_path)
        fullText = re.sub('[^a-zA-Z]+','',''.join(list(map(lambda x: x.text, doc.paragraphs)))).lower()
#        print(fullText)
        return fullText
        
    except:
        print('Could not parse docx : {}'.format(file_path))
        
def search_result(text, name):
    if not isinstance(text, (str, bytes)):
        text = ""
#        
# Allows for specific usecase handling, if one of the items requires different "cleaning" than others
#

    if name == "Example":
        clean_name = re.compile(".*" + re.sub('[^a-zA-Z]+','',name).lower() + ".*")
    else:
        clean_name = re.compile(".*" + re.sub('[^a-zA-Z]+','.?.?.?.?.?',name).lower() + ".*")
    
    clean_text = re.sub('[^a-zA-Z]+','',text).lower()
    
    if clean_name.match(clean_text):
        return True
    else:
        return False


for rootPath in rootPaths:
    print("Parsing docx from folder: {}".format(rootPath))

    for root, dirs, files in os.walk(rootPath):
        all_files = files

        for file in fnmatch.filter(all_files, pattern):
            name, ext = os.path.splitext(file)
            full_name = os.path.join(root, file)
            #print(full_name)
            docxtext = read_docx(full_name)
            if docxtext == "":
                print("No text read from file: {}".format(file))
            else:
                for name in search_names:
                    if search_result(docxtext, name) == True:
                        print("Found a match for {0}: {1}".format(name, file))
                        dest = os.path.join(os.getcwd(), "result", name)
                        destFile = os.path.join(dest, file)
                        if not os.path.exists(dest):
                            os.mkdir(dest)
                        if not os.path.exists(destFile):
                            shutil.copy2(full_name, dest)