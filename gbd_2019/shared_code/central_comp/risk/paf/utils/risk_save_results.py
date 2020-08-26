import argparse
from save_results import save_results_risk


def run_save_results(me_id, year_id, measure_id, sex_id, risk_type, description,
                     input_dir, input_file_pattern, gbd_round_id, decomp_step,
                     n_draws):
    save_results_risk(input_dir=input_dir,
                      input_file_pattern=input_file_pattern,
                      modelable_entity_id=me_id,
                      description=description,
                      risk_type=risk_type,
                      year_id=year_id,
                      sex_id=sex_id,
                      measure_id=measure_id,
                      gbd_round_id=gbd_round_id,
                      decomp_step=decomp_step,
                      n_draws=n_draws,
                      mark_best=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "--me_id",
            help="me_id to upload to",
            type=int)
    parser.add_argument(
            "--year_id",
            help="year_id(s) in data",
            type=str)
    parser.add_argument(
            "--measure_id",
            help="measure_id(s) in data",
            type=str)
    parser.add_argument(
            "--sex_id",
            help="sex_id(s) in data",
            type=str)
    parser.add_argument(
            "--risk_type",
            help="type of risk estimates, rr/paf/tmrel",
            type=str)
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
    me_id = args.me_id
    year_id = [int(i) for i in args.year_id.split()]
    measure_id = [int(i) for i in args.measure_id.split()]
    sex_id = [int(i) for i in args.sex_id.split()]
    risk_type = args.risk_type
    description = args.description
    input_dir = args.input_dir
    input_file_pattern = args.input_file_pattern
    gbd_round_id = args.gbd_round_id
    decomp_step = args.decomp_step
    n_draws = args.n_draws

    run_save_results(
        me_id, year_id, measure_id, sex_id, risk_type, description,
        input_dir, input_file_pattern, gbd_round_id, decomp_step, n_draws)
