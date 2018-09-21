import pandas as pd
import os

here = "filepath"

# Read dw file
std_dws = pd.read_csv(
    "filepath/02_standard/dw.csv")
std_dws.rename(
    columns={'draw%s' % i: 'draw_%s' % i for i in range(1000)},
    inplace=True)

# Read in combinations map
combos = pd.read_excel(os.path.join(here, "mnd_combos.xlsx"))
combos = pd.melt(
    combos,
    id_vars=['healthstate_id', 'healthstate', 'description'],
    var_name='to_combine')
combos = combos[combos.value.notnull()]
combos_2016 = pd.read_excel(os.path.join(here, "2016_combine_map.xlsx"))
combos_2016 = pd.melt(
    combos_2016,
    id_vars=['healthstate_id', 'healthstate'],
    var_name='to_combine')
combos_2016["to_combine"] = combos_2016["value"]
combos_2016 = combos_2016[combos_2016.value.notnull()]
combos = combos.append(combos_2016)
combos = combos.append(
    pd.read_excel(os.path.join(here, "combined_dw_map.xlsx")))

# Combine DWs
drawcols = ['draw_%s' % i for i in range(1000)]
combo_dws = []
for hsid in combos.healthstate_id.unique():
    df = combos[combos.healthstate_id == hsid]
    to_combine = std_dws[std_dws.healthstate_id.isin(df.to_combine.unique())]

    # Subset to draws for constituent healthstates
    to_combine = 1 - to_combine.filter(like='draw')

    # Combine using multiplicative equation
    combined_draws = 1 - to_combine.prod()
    combined_draws['healthstate_id'] = hsid
    combo_dws.append(pd.DataFrame([combined_draws]))

combo_dws = pd.concat(combo_dws)
combo_dws['healthstate_id'] = combo_dws.healthstate_id.astype(int)

# Output to file
col_order = ['healthstate_id']
col_order.extend(drawcols)
combo_dws.to_csv(
    "filepath/03_custom/"
    "combined_mnd_dws.csv", index=False)
