import argparse

import pandas as pd

from db_tools import ezfuncs
from save_results import save_results_risk


def run_save_results(modelable_entity_id, year_id, measure_id, sex_id, risk_type,
                     description, input_dir, input_file_pattern, gbd_round_id,
                     decomp_step, n_draws):
    model_version_df = save_results_risk(
        input_dir=input_dir,
        input_file_pattern=input_file_pattern,
        modelable_entity_id=modelable_entity_id,
        description=description,
        risk_type=risk_type,
        year_id=year_id,
        sex_id=sex_id,
        measure_id=measure_id,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
        n_draws=n_draws,
        mark_best=True)
    return model_version_df["model_version_id"].iat[0]


def insert_paf_model(model_version_id, input_dir):
    paf_metadata = pd.read_csv(f"{input_dir}/paf_model_metadata.csv")
    paf_metadata["paf_model_version_id"] = model_version_id
    paf_version = pd.read_csv(f"{input_dir}/paf_model_version.csv")
    paf_version["paf_model_version_id"] = model_version_id
    with ezfuncs.session_scope("epi-save-results") as scoped_session:
        paf_metadata.to_sql(
            name='paf_model_metadata',
            con=scoped_session.connection(),
            if_exists="append",
            index=False,
            method="multi",
        )
        paf_version.to_sql(
            name='paf_model_version',
            con=scoped_session.connection(),
            if_exists="append",
            index=False,
            method="multi",
        )


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
    modelable_entity_id = args.modelable_entity_id
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

    model_version_id = run_save_results(
        modelable_entity_id, year_id, measure_id, sex_id, risk_type, description,
        input_dir, input_file_pattern, gbd_round_id, decomp_step, n_draws)
    insert_paf_model(model_version_id, input_dir)
