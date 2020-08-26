
"""
Created on Wed Mar  8 12:31:05 2017

@author: USERNAME

THIS SCRIPT HAS BEEN MODIFIED TO EXTRACT ONLY THE HCI CODE AND HCI NAME VARIABLE
FROM THE PHL HOSPITAL DATA WE'VE BEEN USING IN ORDER TO MAP FROM HCI TO SUBNATIONAL LOCATIONS

Philippines claims
    we want these two files: 
        "FILEPATH"
        "FILEPATH"
    documentation is at 
        "FILEPATH" for the variables
    files are in .accdb format, there appears to be a way to pull those into
    python:
        URL
        URL
        URL
        URL


CODEBOOK:
NEWPIN Pseudo PIN. To identify PhilHealth members
PRO_NAME PhilHealth Regional Office (PRO). Philhealth operational cluster
AMT_ACTUAL Amount Being Claimed to PhilHealth
AMT_PHIC Total Amount of Claims Paid. This includes Hospital Fees and Professional Fees
WORKER_TYP Membership Category
ICDCODES ICD10 codes the first code is the primary illness
RVSCODES Procedure codes
PATAGE Age of patient
PATSEX Sex of patient
OPDTST if outpatient, T=True F=Fals
DATE_ADM Admission Date
DATE_DIS Discharge Date
HCICODE Hospital Code
HCI_NAME Hospital Name
CLASS_DEF Facility Type 
DATE_OF_DEATH Date of Death of Patient
EXTRACTION_DATE Date Record was extracted

THIS CODE PULLS JUST THE HOSPITAL NAMES AND CODES TO PASS TO COLLABORATORS
SO THEY CAN MAP THEM TO THE SUBNATIONAL LEVEL FOR US
"""

import platform
import pyodbc  
import pandas as pd


if platform.system() == "Linux":
    root = r"FILENAME"
else:
    root = r"FILEPATH"





assert 'Microsoft Access Driver (*.mdb, *.accdb)' in\
    [x for x in pyodbc.drivers() if x.startswith('Microsoft Access Driver')],\
    ("LOOK OUT, YOUR MACHINE DOESN'T HAVE THE DRIVER "
     "'Microsoft Access Driver (*.mdb, *.accdb)'" + "\n\n"
     "If you see an empty list then you are running 64-bit Python and "
     "you need to install the 64-bit version of the 'ACE' driver. If you "
     "only see ['Microsoft Access Driver (*.mdb)'] and you need to work with "
     "an .accdb file then you need to install the 32-bit version of "
     "the 'ACE' driver." + "\n\n"
     "See URL
     "Microsoft-Access"
     " For more information.")

DRV = "{Microsoft Access Driver (*.mdb, *.accdb)}"

ACCDB_2013 = (root + r"FILENAME"
         r"FILENAME"
         r"Y2015M08D25.ACCDB")  
connection_2013 = pyodbc.connect('DRIVER={};DBQ={}'.format(DRV, ACCDB_2013))




sheets_2013 = ["A JAN2013", "B FEB2013", "C MAR2013", "D APR2013", "E MAY2013",
               "F JUN2013", "G JUL2013", "H AUG2013", "I SEP2013", "J OCT2013",
               "K NOV2013", "L DEC2013"]

df_list_2013 = []

for sheet in sheets_2013:
    query = (QUERY)
    temp_df = pd.read_sql("DB QUERY")
    df_list_2013.append(temp_df)
df_2013 = pd.concat(df_list_2013)
connection_2013.close()  



ACCDB_2014 = (root + r"FILENAME"
         r"FILENAME"
         r"Y2015M08D25.ACCDB")  
connection_2014 = pyodbc.connect('DRIVER={};DBQ={}'.format(DRV, ACCDB_2014))

sheets_2014 = ["A JAN2014", "B FEB2014", "C MAR2014", "D APR2014", "E MAY2014",
               "F JUN2014", "G JUL2014", "H AUG2014", "I SEP2014", "J OCT2014",
               "K NOV2014","L DEC2014"]

df_list_2014 = []

for sheet in sheets_2014:
    query = (QUERY)
    temp_df = pd.read_sql("DB QUERY")
    df_list_2014.append(temp_df)
df_2014 = pd.concat(df_list_2014)
connection_2014.close()  

df = pd.concat([df_2013, df_2014])

df['row_count'] = 1
df = df.groupby(['HCICODE', 'HCI_NAME', 'CLASS_DEF']).agg({'row_count': 'sum'}).reset_index()

df.sort_values('row_count', inplace=True, ascending=False)

df = df.drop_duplicates()

df.to_excel("FILEPATH", index=False)
df.to_csv("FILEPATH", index=False)
