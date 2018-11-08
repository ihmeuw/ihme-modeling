from save_results import save_results_epi
import argparse
import datetime

today_string = datetime.date.today().strftime("%d/%m/%y")

def save_custom(meid):
    save_results_epi(
        input_dir='FILEPATH/'+str(meid),
        input_file_pattern='{location_id}.csv',
        modelable_entity_id=meid,
        description='malaria anemia pre process ' + today_string,
        year_id = [1990, 1995, 2000, 2005, 2010, 2017],
        measure_id=5,
        metric_id=3,
        mark_best=True,
        db_env='prod',
        gbd_round_id=5
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("meid", help="modelable entity ID to save", type=int)
    args = parser.parse_args()
    save_custom(args.meid)