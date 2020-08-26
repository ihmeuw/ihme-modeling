import os
from multiprocessing import Pool
import pandas as pd

DRAWCOLS = ['draw_%s' % i for i in range(1000)]
THIS_PATH = os.path.dirname(__file__)


def combine_dws(row):
    location_id = int(row['location_id'])
    year_id = int(row['year_id'])
    print(location_id, year_id)
    all_combined = []
    for healthstate_id, rows in combos.groupby('healthstate_id'):
        healthstates_to_combine = rows['healthstates_to_combine']

        # Subset to draws for constituent healthstates
        draws_to_combine = standard_dws[
            standard_dws.healthstate.isin(healthstates_to_combine)]

        # Append epilepsy draws
        draws_to_combine = draws_to_combine.append(pd.DataFrame([row]), sort=True)

        # Combine using multiplicative equation
        combined = (
            1 - (1 - draws_to_combine.filter(like='draw').values).prod(axis=0))
        combined = pd.DataFrame(data=[combined], columns=DRAWCOLS)

        # Add to output data frame
        combined = combined.join(pd.DataFrame(data=[
            {'location_id': location_id,
             'year_id': year_id,
             'healthstate_id': healthstate_id}
        ]))
        all_combined.append(combined)
    all_combined = pd.concat(all_combined)
    return all_combined


def epilepsy_combos(como_dir):
    global standard_dws, epilepsy_dws, combos
    # Read dw file
    standard_dws = pd.read_csv(
        "FILEPATH/dw.csv")

    # Read epilepsy dw file generated in combine_epilepsy_any
    epilepsy_dws = pd.read_hdf(f"{como_dir}/info/epilepsy_any_dws.h5")

    # Read in combinations map
    combos = pd.read_excel(
        "%s/epilepsy_subcombos_map.xlsx" % THIS_PATH)
    combos = combos[combos.healthstates_to_combine != 'epilepsy_any']

    # Combine DWs
    rowlist = [row for i, row in epilepsy_dws.iterrows()]
    pool = Pool(20)
    all_combined = pool.map(combine_dws, rowlist)
    pool.close()
    pool.join()
    all_combined = pd.concat(all_combined)

    # Output to file
    col_order = ['location_id', 'year_id', 'healthstate_id'] + DRAWCOLS
    all_combined = all_combined[col_order]
    all_combined.to_hdf(f"{como_dir}/info/epilepsy_combo_dws.h5",
                        'draws', mode='w', format='table',
                        data_columns=['location_id', 'year_id'])
