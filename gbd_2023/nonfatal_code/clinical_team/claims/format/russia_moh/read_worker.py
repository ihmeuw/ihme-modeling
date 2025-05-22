# Using the docx package, this script reads the tables in the document
# and transforms the data into .csv format. 

from docx import Document
import pandas as pd
import re
import sys
from pathlib import Path

# This function reads in a single table in the document to create a df
def table_to_df(table):

    # read everything in table into a list of lists
    data = []
    for row in table.rows:
        row_data = []
        for cell in row.cells:
            row_data.append(cell.text)
        data.append(row_data)

    # create a header from the first four rows
    header_rows = data[0:4]
    header = ["{}+{}+{}+{}".format(a,b,c,d) for a,b,c,d in zip(header_rows[0], 
              header_rows[1], header_rows[2], header_rows[3])]
    header[0] = 'location'
    
    # the rest is the data
    data = data[4:]
    
    # create the df with the header, then make it long
    df = pd.DataFrame(data, columns = header)
    df.set_index('location', inplace=True)
    df = df.stack().reset_index()
    
    # change column names with \n to space
    df['level_1'] = df['level_1'].str.replace('\n', ' ')
    df['location'] = df['location'].str.replace('\n', ' ')
    
    # split the column originating from the header back into 4 sections
    df['population'] = df['level_1'].str.split('+', expand=True)[0]
    df['cause'] = df['level_1'].str.split('+', expand=True)[1]
    df['metric'] = df['level_1'].str.split('+', expand=True)[2]
    df['year'] = df['level_1'].str.split('+', expand=True)[3]
    df.drop(columns='level_1', inplace=True)
    return df

# read the file with the docx package
fp = sys.argv[1]
doc = Document(fp)

# recursively call table_to_df on all tables in file
df_list = []
for table in doc.tables[4:]:
    df_list.append(table_to_df(table))
total = pd.concat(df_list)

# write intermediate file with same naming convention
name = Path(fp).name.split('.')[0]
total.to_csv(f'FILEPATH/{name}.csv', index=False, encoding='utf-8-sig')