import pandas as pd
from crosscutting_functions import pipeline
from crosscutting_functions.pipeline import get_release_id
from get_draws.api import get_draws

from crosscutting_functions import uncertainty


def get_inpatient_envelope(version_id: int, release_id: int, draws: int) -> pd.DataFrame:
    """Pull the inpatient utilization envelope model with draws.

    Args:
        version_id: The version_id to use when calling the get_draws shared function.
        release_id: The release_id to use when calling the get_draws shared function.
        draws: The number of draws to reduce the model to.

    Returns:
        Downsampled to `draws` envelope utilization model.
    """
    df = get_draws(
        source="stgpr",
        gbd_id=25217,
        version_id=version_id,
        gbd_id_type="modelable_entity_id",
        release_id=release_id,
    )

    df = pipeline.downsample_draws(df=df, draws=draws)
    return df


def clean_env_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare the modeled outputs of the hospital inpatient utilization envelope for
    use in the inpatient pipeline by adding and removing columns.

    Args:
        df: The envelope data.

    Returns:
        The envelope data with slightly altered columns.
    """
    df = df.assign(year_start=df["year_id"], year_end=df["year_id"])

    unneeded_cols = [
        "measure_id",
        "metric_id",
        "modelable_entity_id",
        "stgpr_model_version_id",
        "year_id",
    ]

    df = df.drop(unneeded_cols, axis=1)

    return df


def write_inp_envelope_to_run(
    run_id: int, version_id: int, draws: int, env_path: str, no_draws_env_path: str
) -> None:
    """Pull the envelope model from central functions, modify the column names a bit
    and then write two versions of it. One with draws and one without draws.

    Args:
        run_id: Standard clinical run_id. Used to pull release_id.
        version_id: The model version id for the envelope to use.
        draws: The number of draws we should downsample the envelope to.
        env_path: Write path for the draws version of the envelope.
        no_draws_env_path: Write path for the drawless version of the envelope.

    Raises:
        KeyError: If expected  columns from the envelope are missing.
    """
    release_id = get_release_id(run_id)

    df = get_inpatient_envelope(version_id=version_id, release_id=release_id, draws=draws)
    df = clean_env_cols(df=df)

    df = uncertainty.add_ui(df=df, drop_draw_cols=False)
    df = df.drop(["median_CI_team_only"], axis=1)

    exp_cols = [
        "location_id",
        "age_group_id",
        "sex_id",
        "year_start",
        "year_end",
        "mean",
        "upper",
        "lower",
    ]

    if not set(exp_cols).issubset(df.columns):
        raise KeyError(f"The columns we expected are not present in the envelope.")

    # write full draws version
    df.to_hdf(env_path, mode="w", key="df")

    # write no draws envelope
    no_draw_df = df.drop([col for col in df.columns if "draw_" in col], axis=1)
    no_draw_df.to_csv(no_draws_env_path, index=False)
