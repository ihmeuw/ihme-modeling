from save_results import save_results_epi
import argparse


def save_custom(meid):
    save_results_epi(
        input_dir='FILEPATH/hiv_prop_interp/'+str(meid),
        input_file_pattern='{location_id}.h5',
        modelable_entity_id=meid,
        description='DESCRIPTION',
        year_id = range(1990, 2018),
        measure_id=18,
        metric_id=3,
        mark_best=True,
        db_env='prod',
        gbd_round_id=5
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("meid", help="anemia causal attribution out MEID", type=int)
    args = parser.parse_args()
    save_custom(args.meid)