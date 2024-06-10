#////////////////////////////////////////////////////////////////
#////////////////////////////////////////////////////////////////

#Name: Python Search Terms in ALL Columns
#NOTE: All deatils including table name, columns names, and identifying variables have been obfuscated from the original usecase

#Business Usecase: 
#After input variables are set below, the query runs through the table specified and identifies
#all instances in which any field in the table contains any of the search terms, also specified below.

#Multiple usecase in which this comes in handy - searching through for releavnt transactions in an investigation (such as fraud rings, money laundering schemes, etc) as well as for regulatory requests involving requests of specific kinds of transactions.
#Main benefit: The following will loop through ALL columns in a data set, improving performance and lag time from querying the following data directly via SQL or BigQuery.
# The query also includes a shell for pulling the source data from a SQL swerver and dumping the results (including the original records) into a new table

#The final results are exported to a table in the specified server with the table name format: py_Search_Results_YYYMMDD_ + search_title (defined in inputs below)

#////////////////////////////////////////////////////////////////
#////////////////////////////////////////////////////////////////

#////////////////////////////////////////////////////////////////
#Load Appropraite Packages
#////////////////////////////////////////////////////////////////

import pyodbc
import sqlalchemy
import pandas as pd
from pandas.io import sql
import datetime
from datetime import date
from fuzzywuzzy import fuzz

#////////////////////////////////////////////////////////////////
#Set Input Variables 
#////////////////////////////////////////////////////////////////

#This will be the only section that will require any editing prior to run

#Set Target Database and Table
database = 'target_data_serve'
schema = 'transaction_schema'
table = 'transactions'

#Set Date Ranges (If Required, if date range is irrelevant, keep as '')
date_column = '' #Column in the table mentioned above. If date range is irrelevant, keep as ''
date_rng_begin = '' #Format YYYY-MM-DD, If date range is irrelevant, keep as ''
date_rng_end = '' #Format YYYY-MM-DD, If date range is irrelevant, keep as ''

#Set Search names
#IMPORTANT: INPUT ALL SEARCH NAMES AS UPPERCASE
search_names = ['ERIK DE MONTE','SUSPICIOUS MERCHANT #1','SUSPICIOUS MERCHANT #2','HIGH RISK IP CARRIER']

#Set Percentage match for search terms and search fields
#Some Reference Bands:
# ERIK DE MONTE + ERIK = 50
# ERIK DE MONTE + ERIK DE = 70
# ERIK DE MONTE + ERIK DE JONG = 80
# ERIK DE MONTE + ERIC DEL MONTE = 89
# ERIK DE MONTE + ERIKDEMONTE = 90
#Numeric number >=0
match_per = 85

#This will be used for the SQL table name that is outputted.
#IMP: PLEASE ENSURE NO SPACES, USE "_"
search_title = '20210731_fraud_ring'

#////////////////////////////////////////////////////////////////
#Begin Main Query
#////////////////////////////////////////////////////////////////

#Logging Run Time
start_dtm = datetime.datetime.now()
print('')
print( "Query Started: " + str(start_dtm))
print('')

#////////////////////////////////////////////////////////////////
#Create target data set
#////////////////////////////////////////////////////////////////

#Define the target database
search_target_db = database + '.' + schema + '.' + table

#Identify the proper query to run based on whether or not a date range is required to filter
if date_rng_begin == '':
    q = "select * from " + search_target_db
else:
    q = "select * from " + search_target_db + " where [" + date_column + "] between '" + date_rng_begin + "' and '" + date_rng_end + "'"

#Run the SQL, format the data and drop into the dataframe
CONN=sqlalchemy.create_engine("mssql+pyodbc://user:erikdemonteexample")
query = """{}""".format(q)
df = pd.read_sql(query,CONN)
df = df.fillna('')
df = df.apply(lambda x: x.astype(str).str.upper())

print('')
print( "Step 01/04: Data Import Complete: " + str(datetime.datetime.now()))
print('')

#////////////////////////////////////////////////////////////////
#Set Target Columns
#////////////////////////////////////////////////////////////////

#Identify the array of columns names by running a query on the information schema for the database identified in the variables above
qc = "SELECT distinct cast(COLUMN_NAME as varchar(255)) as 'Col' FROM " + database + ".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table + "'"
query_columns = """{}""".format(qc)
df_col = pd.read_sql(query_columns,CONN)
search_columns = df_col['Col'].values.T.tolist()

#Define a variable for the total number of columns to be cyced through
col_total = len(search_columns)

print('')
print( "Step 02/04: Column Import Complete: " + str(datetime.datetime.now()))
print('')


#////////////////////////////////////////////////////////////////
#Perform search loop
#////////////////////////////////////////////////////////////////

#Prepare dataframes and variables for search functionality
res = pd.DataFrame()
res_term = pd.DataFrame()
res_distinct = pd.DataFrame()
col_curr = 0
hitList= list()

#For each column, perform a search for the terms inputted in the search terms above.
for column_name in search_columns:
    col_curr = col_curr + 1 #Identify which number column is being searched through for audit output later in the query
    column_attr = list(df[column_name]) #Dumps all values in a column into a list for search loop functionality below
    for search_term in search_names:
        hitList= list() #clears the list each run to ensure no dupelicates
        res_term = pd.DataFrame() #clears the dataframe each run to ensure no dupelicates
        for src_attr in column_attr:
            r = fuzz.token_set_ratio(src_attr, search_term) #fuzzywuzzy function to compare two terms. Comparison is for search term and each field.
            if r >= match_per: #set percentage above for how much of a match you want to return
                num =len(hitList)
                hitList.insert(num, src_attr) #add all hits to a list
        for hit in hitList:
            res_term = df[df[column_name].str.contains(hit)] #dump a dataframe with all records that contain the hits
        if res_term.empty:
            continue
        else:
            #Prior to attaching the search term and column ID (below), take a distinct dataframe copy for use later
            frames_dis = [res_distinct,res_term] 
            res_distinct = pd.concat(frames_dis) 
            #The following code attaches the search term and column the search term was located in.
            #This will be used later on to concatinate a reference field for use of navigating final results.
            res_term['Search_Term'] = search_term
            res_term['Column_ID'] = column_name
            frames = [res,res_term]
            res = pd.concat(frames)
     
    #Dynamic if statements below improve readaibility of terminal log by adding leading 0 to the count of columns,
    #depending on the number of inteigers in the total amount of columns in the loop
    if len(str(col_total)) == 1:
        print('['+ str(col_curr).zfill(1) + '/' + str(col_total).zfill(1) + '] Search Complete for: ' + column_name)
    if len(str(col_total)) == 2:
        print('['+ str(col_curr).zfill(2) + '/' + str(col_total).zfill(2) + '] Search Complete for: ' + column_name)
    if len(str(col_total)) == 3:
        print('['+ str(col_curr).zfill(3) + '/' + str(col_total).zfill(3) + '] Search Complete for: ' + column_name)
        
print('')
print( "Step 03/04: Search Loop Complete: " + str(datetime.datetime.now()))
print('')

#////////////////////////////////////////////////////////////////
#Clean Up Results and Export to SQL Table
#////////////////////////////////////////////////////////////////
    
#Set variables for table name to be unique
today = str(date.today())
today = today.replace('-','')
currentDT = datetime.datetime.now()

if currentDT.hour >= 12:
    AMPM = 'PM'
else:
    AMPM = 'AM'

table_name = 'pySearch_Results_' + today + AMPM + str(currentDT.hour) +':'+ str(currentDT.minute) + '_' + search_title

#To save time, if there are no results then return this as a message. Else, proceed with cleaning up data.
if res.empty:
    print('//////////////////////////////////////////////////')
    print('No Results Match Search Criteria')
    print('//////////////////////////////////////////////////')
    
    print('')
    print( "Step 04/04: SQL Extraction Complete: " + str(datetime.datetime.now()))
    print('')
    
else:
    #Clean up the distinct table for use later and add an index column for ease of join further below
    res_distinct = res_distinct.drop_duplicates()
    res_distinct['index'] = res_distinct.index
    res = res.drop_duplicates()
    res.to_sql(table_name, CONN, schema = 'Database_Server.dbo.')
    
    #The following query is used to concatinate a grouping of all search terms and columns that hit for each record
    qr = "SELECT distinct [index], Search_Terms = (STUFF((SELECT DISTINCT '[Column: '+isnull(Column_ID,'')+', Term: '+isnull(Search_Term,'')+']; ' FROM [Database_Server].[dbo].["+ table_name +"] pystuff (nolock) WHERE pystuff.[index]=i.[index] ORDER BY 1 FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,0,'')) FROM [Database_Server].[dbo].[" + table_name +"] i"
    stuff = """{}""".format(qr)
    agg_df = pd.read_sql(stuff,CONN)
    
    #Join the distinct results dataframe to the aggregate created above and remove the index column as the final output does not need it.
    final = pd.merge(agg_df, res_distinct, on='index')
    del final['index']
    
    #Drop the original table and then drump the new resuts in the same table (for consistency)
    drop_tb = "DROP TABLE [Database_Server].[dbo].[" + table_name +"]"
    sql.execute(drop_tb, CONN)
    final.to_sql(table_name, CONN, schema = 'Database_Server.dbo.', index=False)
    
    print('')
    print( "Step 04/04: SQL Extraction Complete: " + str(datetime.datetime.now()))
    print('')
    
 #Logging Run Time End
end_dtm = datetime.datetime.now()
print( "Query Ended: " + str(end_dtm))
print('')
print('I have served my purpose. You look great today!')


    