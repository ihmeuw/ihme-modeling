import platform
import pyodbc  # not on most cluster environments
import pandas as pd

# Environment:
if platform.system() == "Linux":
    root = r"FILEPATH"
else:
    root = r"FILEPATH"

# YOUR MACHINE NEEDS TO HAVE THE DRIVER:
#    'Microsoft Access Driver (*.mdb, *.accdb)'

# check driver:
assert "Microsoft Access Driver (*.mdb, *.accdb)" in [
    x for x in pyodbc.drivers() if x.startswith("Microsoft Access Driver")
], (
    "LOOK OUT, YOUR MACHINE DOESN'T HAVE THE DRIVER "
    "'Microsoft Access Driver (*.mdb, *.accdb)'" + "\n\n"
    "If you see an empty list then you are running 64-bit Python and "
    "you need to install the 64-bit version of the 'ACE' driver. If you "
    "only see ['Microsoft Access Driver (*.mdb)'] and you need to work with "
    "an .accdb file then you need to install the 32-bit version of "
    "the 'ACE' driver." + "\n\n"
    "See https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-"
    "Microsoft-Access"
    " For more information."
)

DRV = "{Microsoft Access Driver (*.mdb, *.accdb)}"

ACCDB_2013 = root + r"FILEPATH"  # path to data files
connection_2013 = pyodbc.connect("DRIVER={};DBQ={}".format(DRV, ACCDB_2013))

sheets_2013 = [
    "A JAN2013",
    "B FEB2013",
    "C MAR2013",
    "D APR2013",
    "E MAY2013",
    "F JUN2013",
    "G JUL2013",
    "H AUG2013",
    "I SEP2013",
    "J OCT2013",
    "K NOV2013",
    "L DEC2013",
]

df_list_2013 = []

for sheet in sheets_2013:
    query = ("QUERY").format(sheet)
    temp_df = pd.read_sql(query, connection_2013)
    df_list_2013.append(temp_df)
df_2013 = pd.concat(df_list_2013)
connection_2013.close()  # close connection to file

######################################################################

ACCDB_2014 = root + r"FILEPATH"  # path to data files
connection_2014 = pyodbc.connect("DRIVER={};DBQ={}".format(DRV, ACCDB_2014))

sheets_2014 = [
    "A JAN2014",
    "B FEB2014",
    "C MAR2014",
    "D APR2014",
    "E MAY2014",
    "F JUN2014",
    "G JUL2014",
    "H AUG2014",
    "I SEP2014",
    "J OCT2014",
    "K NOV2014",
    "L DEC2014",
]

df_list_2014 = []

for sheet in sheets_2014:
    query = ("QUERY").format(sheet)
    temp_df = pd.read_sql(query, connection_2014)
    df_list_2014.append(temp_df)
df_2014 = pd.concat(df_list_2014)
connection_2014.close()  # close connection to file

df = pd.concat([df_2013, df_2014])

df["row_count"] = 1
df = (
    df.groupby(["HCICODE", "HCI_NAME", "CLASS_DEF"])
    .agg({"row_count": "sum"})
    .reset_index()
)

df.sort_values("row_count", inplace=True, ascending=False)

df = df.drop_duplicates()

df.to_excel("FILEPATH", index=False)
df.to_csv("FILEPATH", index=False)
