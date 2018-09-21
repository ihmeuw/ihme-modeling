import sys
import pandas as pd
from adding_machine.agg_locations import save_custom_results


def save_custom(meid):
    save_custom_results(meid=meid, description='{description}',
                        input_dir="/{FILEPATH}/"+str(meid), years=[{YEAR IDS}],
                        sexes=[{SEX IDS}], mark_best=True,
                        in_counts=False, env="prod",
                        custom_file_pattern="{measure_id}_{location_id}.h5",
                        h5_tablename='draws', gbd_round={GBD ROUND})

if __name__ == "__main__":
    try:
        fp = int(sys.argv[1])
    except:
        fp = [meid for meid in meids]

    save_custom(fp)
