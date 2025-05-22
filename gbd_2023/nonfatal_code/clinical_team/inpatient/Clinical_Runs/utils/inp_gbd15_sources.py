import numpy as np
import pandas as pd

"""
A collection of sources that were orignally formatted for GBD 2015. These file have a .dta extension
"""

sources = [
    {
        "db_source_name": "EUR_HMDB",
        "file_source_name": "EUR_HMDB",
        "year_id": [e for e in np.arange(2001, 2014)],
    },
    {
        "db_source_name": "NOR_NIPH",
        "file_source_name": "NOR_NIPH_08_12",
        "year_id": [e for e in np.arange(2008, 2013)],
    },
    {
        "db_source_name": "USA_NAMCS",
        "file_source_name": "USA_NAMCS",
        "year_id": [e for e in np.arange(2012, 2014)],
    },
    {
        "db_source_name": "USA_NHAMCS",
        "file_source_name": "USA_NHAMCS_92_10",
        "year_id": [e for e in np.arange(1992, 2011)],
    },
    {
        "db_source_name": "USA_NHDS",
        "file_source_name": "USA_NHDS_79_10",
        "year_id": [e for e in np.arange(1979, 2011)],
    },
]

df_list = []
for e in sources:
    size = len(e["year_id"])
    source_dict = {
        "db_source_name": [e["db_source_name"] for _ in np.arange(size)],
        "file_source_name": [e["file_source_name"] for _ in np.arange(size)],
        "year_id": e["year_id"],
    }
    df = pd.DataFrame(columns=list(source_dict.keys()), data=source_dict)
    df_list.append(df)

gbd_15_sources = pd.concat(df_list, sort=False)
