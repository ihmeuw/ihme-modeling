from save_results import save_results_epi
import argparse
import datetime

today_string = datetime.date.today().strftime("%d/%m/%y")

def save_custom(meid):
    save_results_epi(
        input_dir='FILEPATH/str(meid)',
        input_file_pattern="{measure_id}_{location_id}.h5",
        modelable_entity_id=meid,
        description='DESCRIPTION ' + today_string,
        measure_id=5,
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

