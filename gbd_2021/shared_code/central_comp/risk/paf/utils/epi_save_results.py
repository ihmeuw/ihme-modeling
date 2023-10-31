import argparse

from save_results import save_results_epi


def run_save_results(modelable_entity_id, year_id, measure_id, description, input_dir,
                     bundle_id, crosswalk_version_id, gbd_round_id, decomp_step,
                     n_draws, input_file_pattern):
    save_results_epi(input_dir=input_dir,
                     input_file_pattern=input_file_pattern,
                     modelable_entity_id=modelable_entity_id,
                     description=description,
                     year_id=year_id,
                     measure_id=measure_id,
                     bundle_id=bundle_id,
                     crosswalk_version_id=crosswalk_version_id,
                     gbd_round_id=gbd_round_id,
                     decomp_step=decomp_step,
                     n_draws=n_draws,
                     mark_best=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "--modelable_entity_id",
            help="modelable_entity_id to upload to",
            type=int)
    parser.add_argument(
            "--year_id",
            help="year_id(s) in data",
            type=str)
    parser.add_argument(
            "--measure_id",
            help="measure_id in data",
            type=int)
    parser.add_argument(
            "--description",
            help="description of estimates",
            type=str)
    parser.add_argument(
            "--input_dir",
            help="directory where files are saved",
            type=str)
    parser.add_argument(
            "--input_file_pattern",
            help="file spec",
            type=str)
    parser.add_argument(
            "--bundle_id",
            help="id for bundle",
            type=int)
    parser.add_argument(
            "--crosswalk_version_id",
            help="id for crosswalk version",
            type=int)
    parser.add_argument(
            "--gbd_round_id",
            help="id for GBD round",
            type=int)
    parser.add_argument(
            "--decomp_step",
            help="step of decomposition",
            type=str)
    parser.add_argument(
            "--n_draws",
            help="number of draws in data",
            type=int)

    args = parser.parse_args()
    modelable_entity_id = args.modelable_entity_id
    year_id = [int(i) for i in args.year_id.split()]
    measure_id = args.measure_id
    description = args.description
    input_dir = args.input_dir
    input_file_pattern = args.input_file_pattern
    bundle_id = args.bundle_id
    crosswalk_version_id = args.crosswalk_version_id
    gbd_round_id = args.gbd_round_id
    decomp_step = args.decomp_step
    n_draws = args.n_draws

    run_save_results(modelable_entity_id, year_id, measure_id, description,
                     input_dir, bundle_id, crosswalk_version_id,
                     gbd_round_id, decomp_step, n_draws, input_file_pattern)
