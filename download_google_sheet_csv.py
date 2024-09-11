#download_google_sheet_csv.py

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pprint
import csv
import sys

var_google_sheet_url = sys.argv[1]
var_google_sheet_tab_name = int(sys.argv[2])
var_output_file_name = sys.argv[3]

# To request access using OAuth 2.0, your application needs the scope information
scope = ['https://spreadsheets.google.com/feeds';]

# Service Account credential for OAuth 2.0 signed JWT grants
creds = ServiceAccountCredentials.from_json_keyfile_name('/usr/local/airflow/secrets/bqconfig/bigquery_config.json', scope)
client =gspread.authorize(creds)

# open file using credential
sheet = client.open_by_url("'"+ var_google_sheet_url + "'")

# print result
pp = pprint.PrettyPrinter()
result = sheet.get_worksheet(var_google_sheet_tab_name).get_all_values()

#result = sheet.sheet1.get_all_values()
pp.pprint(result)

# writing result to csv files
output_path= '/tmp/'+var_output_file_name
csvfile = output_path

#Assuming result is a flat list
with open(csvfile, "w") as output:
 writer = csv.writer(output, lineterminator='\n')
 for val in result:
 writer.writerow(val)