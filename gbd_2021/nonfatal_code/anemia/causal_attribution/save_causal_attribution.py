from argparse import ArgumentParser, Namespace
import datetime
import os
import pandas as pd

from gbd.gbd_round import gbd_round_from_gbd_round_id
from save_results import save_results_epi
from nch_db_queries import get_bundle_xwalk_version_for_best_model


path_to_directory = os.path.dirname(os.path.abspath(__file__))



def parse_args() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument('--modelable_entity_id', type=int)
    parser.add_argument('--year_id', nargs='*', type=int)
    parser.add_argument('--gbd_round_id', type=int)
    parser.add_argument('--decomp_step', type=str)
    parser.add_argument('--modelversion_source', type=int)
    parser.add_argument('--save_dir', type=str)
    return parser.parse_args()


def save_custom(modelable_entity_id, year_id, gbd_round_id, decomp_step,
                save_dir, bundle_id, crosswalk_version_id) -> int:
    run_id = 10107
    description = (
        f"Anemia CA for GBD {gbd_round_from_gbd_round_id(gbd_round_id)} "
        f"{decomp_step}, runid: {run_id}, {datetime.date.today().strftime('%m/%d/%y')}")
    save_results_epi(
        input_dir=save_dir,
        input_file_pattern="{year_id}/{location_id}.csv",
        modelable_entity_id=modelable_entity_id,
        description=description,
        year_id=year_id,
        measure_id=5,
        mark_best=True,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
        bundle_id=bundle_id,
        crosswalk_version_id=crosswalk_version_id)


if __name__ == "__main__":
    args = parse_args()

    print("Starting save")

    orig_meid = args.modelversion_source

    bundle_tuple = get_bundle_xwalk_version_for_best_model(me_id = orig_meid, gbd_round_id = args.gbd_round_id, decomp_step = args.decomp_step)
    bundle_id = int(bundle_tuple[0])
    crosswalk_version_id = int(bundle_tuple[1])

    print(bundle_id)

    save_custom(
        modelable_entity_id=args.modelable_entity_id,
        bundle_id=bundle_id,
        crosswalk_version_id=crosswalk_version_id,
        year_id=args.year_id,
        gbd_round_id=args.gbd_round_id,
        decomp_step=args.decomp_step,
        save_dir=args.save_dir)
