"""
Implement the Clinical noise reduction process on CMS data
"""

from pathlib import PosixPath
from typing import List, Optional, Union

import pandas as pd
from crosscutting_functions import pipeline_functions
from crosscutting_functions.noise-reduction import clinical_nr as ClinicalNR


def nr_cms(
    df: pd.DataFrame, run_id: int, bundle_id: int, estimate_id: int, group_location_id: int
) -> pd.DataFrame:
    """Wraps around the ClinicalNR noise reduction class and performs noise reduction for both
    males and females, then concats data back together and returns pre and post nr columns

    Args:
        df (pd.DataFrame): Containing CMS data to be noise reduced
        run_id (int): Clinical run_id found in clinical.run_metadata.
        bundle_id (int): The bundle that will be processed
        estimate_id (int): The estimate that will be processed
        group_location_id (int): The national location_id

    Returns:
        pd.DataFrame: Noise reduced CMS data.
    """

    nr_l = []
    # loop over both sex_ids
    for sex_id in [sex for sex in df.sex_id.unique() if sex in [1, 2]]:
        tmp = df.query(f"sex_id == {sex_id}").copy()
        # create model group name
        model_group = f"{bundle_id}_{estimate_id}_{sex_id}_{group_location_id}"
        # run all the nr steps with mostly default args
        tmp_nr = run_nr_steps(df=tmp, model_group=model_group, run_id=run_id)
        nr_l.append(tmp_nr)

    nr = pd.concat(nr_l, sort=False, ignore_index=False)

    return nr


def run_nr_steps(
    df: pd.DataFrame,
    run_id: int,
    model_group: str,
    name: str = "CMS_Noise_Reduction",
    subnational: bool = True,
    model_type: str = "Poisson",
    model_failure_sub: str = "fill_average",
    cols_to_nr: List[str] = ["mean"],
    df_path: Optional[Union[str, PosixPath]] = None,
) -> pd.DataFrame:
    """ "Args match what the NR class needs to instantiate. This walks through
    NR methods in the appropriate order.
    nr_cms uses mostly default args"""

    # pass input args to nr class
    nr = ClinicalNR.ClinicalNR(
        name=name,
        run_id=run_id,
        subnational=subnational,
        df_path=df_path,
        model_group=model_group,
        model_type=model_type,
        model_failure_sub=model_failure_sub,
        cols_to_nr=cols_to_nr,
    )

    nr._assign_release_id(release_id=pipeline_functions.get_release_id(run_id=run_id))

    pre_cols = df.columns.tolist()

    # assign df to nr class and validate
    nr.df = df
    nr._eval_group()
    nr.check_required_columns
    nr._create_col_to_fit_model()
    nr.check_square()
    nr.create_count_col()
    pre = len(nr.df)

    # agg to national
    nr.create_national_df()

    # create nat and subnat models
    nr.df, nr.df_model_object = nr.fit_model(nr.df.copy())
    nr.national_df, nr.nat_df_model_object = nr.fit_model(nr.national_df.copy())

    # do noise reduction
    nr.df = nr.noise_reduce(nr.df)
    nr.national_df = nr.noise_reduce(nr.national_df)

    # create is_nat col and df for raking
    nr.df["is_nat"] = 0
    nr.national_df["is_nat"] = 1
    nr.raked_df = pd.concat([nr.df, nr.national_df], sort=False)

    # do raking
    nr.raking()
    # drop the input columns
    nr.raked_df.drop(nr.nr_cols, axis=1, inplace=True)

    # extract the subnational raked data
    nr.df = nr.raked_df.query("is_nat == 0").copy()
    if len(nr.df) != pre:
        raise RuntimeError("NoiseReduction changed the DataFrame shape.")

    # apply the floor
    pre_min = nr.df.mean_final.min()
    nr.apply_floor()
    non_zero_rows = len(nr.df[nr.df.mean_final > 0])
    post_min = nr.df.loc[nr.df.mean_final > 0, "mean_final"].min()
    if non_zero_rows > 0:
        if post_min < pre_min:
            raise RuntimeError("Applying the floor did not work correctly")

    nr.df = rename_cols(df=nr.df, cols_to_nr=cols_to_nr)
    diff_cols = set(pre_cols) - set(df.columns)

    if len(diff_cols) > 0:
        raise RuntimeError(f"Noise Reduction lost these columns \n{diff_cols}")

    return nr.df


def rename_cols(df: pd.DataFrame, cols_to_nr: List[str]) -> pd.DataFrame:
    """Rename the pre-NR and post NR columns"""

    for col in cols_to_nr:
        df.rename(columns={col: f"pre_nr_{col}"}, inplace=True)
        df.rename(columns={f"{col}_final": col}, inplace=True)
    return df
