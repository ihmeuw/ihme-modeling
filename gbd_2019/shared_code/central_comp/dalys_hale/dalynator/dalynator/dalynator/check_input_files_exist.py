import os
from itertools import product
from dalynator.get_yld_data import possible_patterns


def check_cod(cod_object, demo_dict):
    pattern_dict = {key: demo_dict[key] for key in demo_dict
                    if key in cod_object.file_pattern}
    for tup in product(*pattern_dict.values()):
        dummy_dict = dict(zip(pattern_dict.keys(), tup))
        file_name = cod_object.file_pattern.format(**dummy_dict)
        file_path = os.path.join(cod_object.abs_path_to_draws, file_name)
        if not os.path.isfile(file_path):
            raise ValueError("{} does not exist ".format(file_path))


def check_epi(epi_dir, location_ids, year_ids, measure_id):
    for my_loc in location_ids:
        for year_id in year_ids:
            for sex_id in [1, 2]:
                possible_files = [
                    os.path.join(epi_dir,
                                 pattern.format(
                                     measure_id=measure_id,
                                     location_id=my_loc,
                                     year_id=year_id,
                                     sex_id=sex_id)) for
                    pattern in possible_patterns]
                if not any(os.path.isfile(f) for f in possible_files):
                    raise ValueError("None of possible como inputs {} exist."
                                     "".format(
                                         ",".join(f for f in possible_files)))


def check_pafs(paf_dir, location_ids, year_ids):
    for my_loc in location_ids:
        for year_id in year_ids:
            my_file = "FILEPATH".format(paf_dir, my_loc, year_id)
            if not os.path.isfile(my_file):
                raise ValueError(my_file + " does not exist ")
