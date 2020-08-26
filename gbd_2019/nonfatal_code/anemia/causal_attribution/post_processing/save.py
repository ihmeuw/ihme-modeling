# this part uploads the results of the Proportion splits to the epi database
from argparse import ArgumentParser, Namespace

import pandas as pd
from save_results import save_results_epi
from chronos.expansion import interpolate
from db_queries import get_location_metadata


def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument('--modelable_entity_id', type=int)
    parser.add_argument('--year_ids', nargs='*', type=int)
    parser.add_argument('--gbd_round_id', type=int)
    parser.add_argument('--decomp_step', type=str)
    parser.add_argument('--save_dir', type=str)
    return parser.parse_args()


def interpolate_props(modelable_entity_id, location_ids,
                      year_ids, save_dir) -> pd.DataFrame:
    df = pd.DataFrame()
    for yr in year_ids:
        year_df = pd.read_csv(f'FILEPATH')
        df = df.append(year_df)
    df['metric_id'] = 3
    print("Years compiled")
    for loc in location_ids:
        df_subset = df[df['location_id']==loc]
        interp = interpolate(df_subset, year_start_id = year_ids[0], year_end_id = year_ids[-1], location_id = loc, gbd_id = modelable_entity_id,
                            measure_id = 18, interp_method = 'linear')
        interp.to_csv("FILEPATH")





def save_proportions(modelable_entity_id, gbd_round_id, decomp_step,
                     year_ids, save_dir) -> int:
    mv = save_results_epi(
        input_dir=save_dir,
        input_file_pattern='FILEPATH',
        modelable_entity_id=modelable_entity_id,
        description='Proportions calculated from anemia casual attribution',
        year_id=year_ids,
        measure_id=18,
        mark_best=True,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step)
    return mv


if __name__ == "__main__":
    args = parse_args()
    print("Starting interpolation")
    locs = get_location_metadata(35, gbd_round_id = 6, decomp_step = 'step4')
    locs = locs[locs['most_detailed']==1]['location_id']
    all_years = list(range(args.year_ids[0], args.year_ids[-1] + 1))
    print(args.save_dir)
   


    interpolate_props(
        modelable_entity_id=args.modelable_entity_id,
        location_ids=locs,
        year_ids=args.year_ids,
        save_dir=args.save_dir)

    save_proportions(
        modelable_entity_id=args.modelable_entity_id,
        year_ids=all_years,
        gbd_round_id=args.gbd_round_id,
        decomp_step=args.decomp_step,
        save_dir=args.save_dir)
