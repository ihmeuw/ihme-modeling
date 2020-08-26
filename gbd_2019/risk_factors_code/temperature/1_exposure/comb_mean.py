import pandas as pd
import sys
import os

year = os.environ.get("SGE_TASK_ID")

df = pd.DataFrame()

monthly = 'FILEPATH'

for file in os.listdir(monthly):
    if str(year) in file:
        df = df.append(pd.read_csv(monthly + file))

df.to_csv(f'FILEPATH',index=False)