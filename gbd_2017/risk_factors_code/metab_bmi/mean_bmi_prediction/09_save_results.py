import os
import sys

from save_results import save_results_epi


def save(risk, date, mark_best=False):
    file_pattern = "{location_id}.csv"

    if risk == "adult_ov":
        save_results_epi(input_dir=os.path.join("FILEPATH", date, "expanded_adult_draws_ow"),
                         input_file_pattern=file_pattern, description="overweight model {}".format(date), metric_id=3,
                         measure_id=18, mark_best=mark_best, modelable_entity_id=9363)
        with open(os.path.join("FILEPATH", date, "step_10_{}.txt".format(risk)), 'w') as f:
            f.write("completed")
    elif risk == "adult_ob":
        save_results_epi(input_dir=os.path.join("FILEPATH", date, "draws_transformed_adult"),
                         input_file_pattern=file_pattern, description="obesity model {}".format(date), metric_id=3,
                         measure_id=18, mark_best=mark_best, modelable_entity_id=9364)
        with open(os.path.join("FILEPATH", date,
                               "step_10_{}.txt".format(risk)), 'w') as f:
            f.write("completed")
    elif risk == "child_ov":
        save_results_epi(input_dir=os.path.join("FILEPATH", date, "child_draws_ow"),
                         input_file_pattern=file_pattern, description="childhood overweight model {}".format(date),
                         metric_id=3, measure_id=18, mark_best=mark_best, modelable_entity_id=10344)
        with open(os.path.join("FILEPATH", date,
                               "step_10_{}.txt".format(risk)), 'w') as f:
            f.write("completed")
    elif risk == "child_ob":
        save_results_epi(input_dir=os.path.join("FILEPATH", date, "child_draws"),
                         input_file_pattern=file_pattern,
                         description="childhood obesity model {} (zeta by density)".format(date),
                         metric_id=3, measure_id=18, mark_best=mark_best, modelable_entity_id=10345)
        with open(os.path.join("FILEPATH", date,
                               "step_10_{}.txt".format(risk)), 'w') as f:
            f.write("completed")
    elif risk == "child_not_ob":
        save_results_epi(input_dir=os.path.join("FILEPATH", date, "child_ow_not_ob"),
                         input_file_pattern=file_pattern,
                         description="childhood overweight not obese for PAFs".format(date),
                         metric_id=3, measure_id=18, mark_best=mark_best, modelable_entity_id=20018)
        with open(os.path.join("FILEPATH", date,
                               "step_10_{}.txt".format(risk)), 'w') as f:
            f.write("completed")
    else:
        save_results_epi(input_dir=os.path.join("FILEPATH", date, "mean_bmi"),
                         input_file_pattern=file_pattern, description="adult mean BMI estimates {}".format(date),
                         metric_id=3, measure_id=19, mark_best=mark_best, modelable_entity_id=2548)
        with open(os.path.join("FILEPATH", date,
                               "step_10_{}.txt".format(risk)), 'w') as f:
            f.write("completed")


if __name__ == "__main__":
    args = sys.argv[1:]
    date = str(args[0])
    risk = str(args[1])
    mark_best = bool(args[2])

    save(risk, date, mark_best)
