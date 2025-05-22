import pandas as pd
from crosscutting_functions import demographic

from pol_nhf.utils.settings import PipelineAgeSettings


def prep_for_denoms(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add five year age bins and corresponding age_group_id onto data

    Cached population data is in five year age bins and only has age_group_id
    as an age column. Data must be transformed to this age space for sucessful merge
    """
    pre_shape = df.shape[0]
    temp = demographic.age_binning(
        df=df,
        clinical_age_group_set_id=PipelineAgeSettings.clinical_age_group_id,
        drop_age=False,
        terminal_age_in_data=False,
        under1_age_detail=PipelineAgeSettings.under1_age_detail,
        break_if_not_contig=False,
    )

    temp = demographic.group_id_start_end_switcher(
        df=temp,
        clinical_age_group_set_id=PipelineAgeSettings.clinical_age_group_id,
        remove_cols=False,
    )
    if pre_shape != temp.shape[0]:
        raise RuntimeError("Lost rows when preparing data for denominator")

    if temp[["age_start", "age_end", "age_group_id"]].isnull().sum().sum() != 0:
        raise RuntimeError("Null age columns are present in the data")

    return temp


def pull_cached_population(run_id: int) -> pd.DataFrame:
    path = (
        "FILEPATH"
    )
    pop = pd.read_parquet(path)
    return pop.drop("pop_run_id", axis=1)


def merge_denominator(df: pd.DataFrame, pop: pd.DataFrame) -> pd.DataFrame:
    """
    Merge cached population onto dataset
    """
    merge_cols = pop.columns.to_list()
    merge_cols.remove("population")
    df = df.merge(pop, on=merge_cols, how="left", validate="m:1")
    return df


def create_denominator(df: pd.DataFrame, run_id: int) -> pd.DataFrame:
    """
    Append sample size column onto dataset

    Pull cached population from run directory and merge onto input dataframe
    """
    pop = pull_cached_population(run_id)
    temp = merge_denominator(df, pop)
    temp.rename({"population": "sample_size"}, axis=1, inplace=True)
    return temp
