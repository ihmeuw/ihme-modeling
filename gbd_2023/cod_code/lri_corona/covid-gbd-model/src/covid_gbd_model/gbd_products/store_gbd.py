from pathlib import Path
from typing import List, Tuple
import tqdm
import multiprocessing
import functools

import pandas as pd

from covid_gbd_model.variables import MODEL_YEARS, COV_YEAR_START, COD_YEAR_START


def backfill(
    data: pd.DataFrame, year_start: int, model_years: List[int],
):
    backfill_year_ids = list(range(year_start, min(model_years)))

    location_ids = data.index.get_level_values('location_id').unique().tolist()
    age_group_ids = data.index.get_level_values('age_group_id').unique().tolist()
    sex_ids = data.index.get_level_values('sex_id').unique().tolist()

    backfilled = pd.Series(
        0,
        index=pd.MultiIndex.from_product(
            [
                location_ids,
                backfill_year_ids,
                age_group_ids,
                sex_ids,
            ],
            names=[
                'location_id',
                'year_id',
                'age_group_id',
                'sex_id',
            ],

        )
    )
    backfilled = pd.concat(
        [
            backfilled.rename(data_col) for data_col in data.columns
        ], axis=1
    )
    data = pd.concat([backfilled, data]).sort_index()

    return data


def save_covariates(summaries: pd.DataFrame, inputs_root: Path, covariate_dir: Path):
    age_metadata = pd.read_parquet(inputs_root / 'age_metadata.parquet')

    (
        # age-/sex-specific
        summaries.loc[:, :, age_metadata['age_group_id'], [1, 2]]
        .reset_index()
        .to_csv(covariate_dir / 'covid_age_sex.csv', index=False)
    )
    (
        # crude, by sex
        summaries.loc[:, :, [22], [1, 2]]
        .reset_index()
        .to_csv(covariate_dir / 'covid_cdr.csv', index=False)
    )
    (
        # age-standardized, by sex
        summaries.loc[:, :, [27], [1, 2]]
        .reset_index()
        .to_csv(covariate_dir / 'covid_asdr.csv', index=False)
    )



def write_year_draws(year_draws: Tuple[int, pd.DataFrame], cod_dir: Path):
    year_id, year_draws = year_draws
    year_draws.reset_index().to_csv(cod_dir / f'{year_id}.csv', index=False)


def save_cod(
    draws: pd.DataFrame, inputs_root: Path, cod_dir: Path,
    year_start: int, model_years: List[int],
):
    location_metadata = pd.read_parquet(inputs_root / 'location_metadata.parquet')
    location_metadata = location_metadata.loc[location_metadata['most_detailed'] == 1]
    age_metadata = pd.read_parquet(inputs_root / 'age_metadata.parquet')
    year_ids = list(range(year_start, model_years[-1] + 1))

    draws = draws.loc[location_metadata['location_id'], year_ids, age_metadata['age_group_id'], [1, 2]]
    draws = list(draws.groupby('year_id'))

    _write_year_draws = functools.partial(
        write_year_draws,
        cod_dir=cod_dir,
    )

    with multiprocessing.Pool(10) as pool:
        _ = list(tqdm.tqdm(pool.imap(_write_year_draws, draws), total=len(draws)))


def store_gbd(
    inputs_root: Path, model_root: Path,
    cov_year_start: int = COV_YEAR_START, cod_year_start: int = COD_YEAR_START,
    model_years: List[int] = MODEL_YEARS,
):
    processed_dir = model_root / 'processed'

    gbd_dir = model_root / 'gbd'
    gbd_dir.mkdir()

    covariate_dir = gbd_dir / 'covariate'
    covariate_dir.mkdir()
    summaries = pd.read_parquet(processed_dir / 'raked_summaries.parquet')
    summaries = backfill(summaries, cov_year_start, model_years)
    save_covariates(summaries, inputs_root, covariate_dir)

    cod_dir = gbd_dir / 'cod'
    cod_dir.mkdir()
    draws = pd.read_parquet(processed_dir / 'raked_draws.parquet')
    draws = backfill(draws, cod_year_start, model_years)
    save_cod(draws, inputs_root, cod_dir, cod_year_start, model_years)
