import pandas as pd
import time
from datetime import datetime

from db_queries import get_best_model_versions

df = pd.read_excel('FILEPATH/in_out_meid_map.xlsx', 'in_meids')
meid_col = df.modelable_entity_id.tolist()
meids = [a for a in meid_col if isinstance(a, int)]

meids = meids + [2510, 3263, 18682]

mvdfs = []

for meid in meids:
    mvdf = get_best_model_versions('modelable_entity', meid)
    mvdate = mvdf.loc[0, 'date_inserted'].to_pydatetime()
    diff = datetime.now()-mvdate
    days = diff.days
    mvdf['days'] = days
    mvdfs.append(mvdf)
    print "It has been {d} days since the best model for {m} was run.".format(d=days, m=mvdf.loc[0, 'modelable_entity_name'])

mvdfs = pd.concat(mvdfs)
mvdfs.to_csv(FILEPATH, index=False)