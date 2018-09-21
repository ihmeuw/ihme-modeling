import os
from dalynator.get_yld_data import possible_patterns


def check_cod(cod_dir, location_ids, measure_id):
    for my_loc in location_ids:
        my_file = "{}/{}_{}.h5".format(cod_dir, measure_id, my_loc)
        if not os.path.isfile(my_file):
            raise ValueError(my_file + " does not exist ")


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
            my_file = "{}/{}_{}.dta".format(paf_dir, my_loc, year_id)
            if not os.path.isfile(my_file):
                raise ValueError(my_file + " does not exist ")
