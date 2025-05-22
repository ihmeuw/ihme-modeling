import sys
import getpass
sys.path.append("FILEPATH".format(getpass.getuser()))
from cod_prep.utils.outlier_db_tools import outliers
from db_queries import get_location_metadata
import time

locs = get_location_metadata(location_set_id=21)
locs = list(locs.query("parent_id == 214")['location_id'])

locs += [214]

ot = outliers(
        decomp_id=1,
        sex_id=2,
        year_id=list(range(1980, 2020)),
        reason="Shock",
        researcher="NAME",
        source_id=461,
        data_type_id=5,
        cause_id=366,
        location_id=locs,
        outlier=0)

ot.query_db()

ot.sleep(10)

ot.outlier_db()
