from pathlib import Path

import pandas as pd
import numpy as np


def calc_manual_idr_coef(data: pd.DataFrame, op_data: pd.Series, population: pd.Series):
    data = data.copy().join(population, how='left')
    data['deaths'] = data['obs'] * data['population']
    data = data.loc[data['obs'].notnull()]
    all_avg_deaths = data['deaths'].groupby(['location_id', 'age_group_id', 'sex_id']).mean()
    data = data.join(op_data, how='right')
    overlap_avg_deaths = data['deaths'].groupby(['location_id', 'age_group_id', 'sex_id']).mean()

    manuel_coef_weight = (overlap_avg_deaths / all_avg_deaths).loc[overlap_avg_deaths.index].clip(0, 1)

    deaths_pct = (
        data['deaths']
        .divide(data['deaths'].groupby(['location_id', 'age_group_id', 'sex_id']).sum())
        .reorder_levels(data.index.names)
    )

    logit_vr = np.log(data['obs'] / (1 - data['obs']))
    logit_surv = np.log(data['op_obs'] / (1 - data['op_obs']))
    delta_idr = (data['idr'].max() - data['raw_idr'])
    manual_coef = ((logit_vr - logit_surv) / delta_idr).clip(0, np.inf)
    manual_coef = (manual_coef * deaths_pct).groupby(['location_id', 'age_group_id', 'sex_id']).sum()

    return pd.concat([manual_coef.rename('manual_coef'), manuel_coef_weight.rename('manuel_coef_weight')], axis=1)


def apply_idr_adjustment(val: pd.Series, idr_corr_factor: pd.Series):
    logit_val = np.log(val / (1 - val))
    logit_counterfactual = logit_val + idr_corr_factor

    val = (1 / (1 + np.exp(-logit_counterfactual))).clip(0, 1)

    return val


def idr_adjustment(inputs_root: Path, model_root: Path):
    if (model_root / 'data' / 'unadj_data.parquet').exists():
        data = pd.read_parquet(model_root / 'data' / 'unadj_data.parquet')
    else:
        data = pd.read_parquet(model_root / 'data' / 'data.parquet')
        data.to_parquet(model_root / 'data' / 'unadj_data.parquet')

    population = pd.read_parquet(inputs_root / 'population.parquet').loc[:, 'population']

    op_data = pd.read_parquet(inputs_root / 'age_sex_split_overlap_surveillance_data.parquet')
    op_data = op_data.reset_index('data_id', drop=True).loc[:, 'obs'].rename('op_obs')

    coef = pd.read_csv(model_root / 'results' / 'spxmod' / 'coef.csv')
    coef = coef.loc[coef['cov'] == 'idr', ['age_mid', 'sex_id', 'coef']]

    if (model_root / 'results' / 'spxmod' / 'unadj_predictions.parquet').exists():
        predictions = pd.read_parquet(model_root / 'results' / 'spxmod' / 'unadj_predictions.parquet')
    else:
        predictions = pd.read_parquet(model_root / 'results' / 'spxmod' / 'predictions.parquet')
        predictions.to_parquet(model_root / 'results' / 'spxmod' / 'unadj_predictions.parquet')

    data = data.merge(predictions, how='left').merge(coef, how='left')
    if data['coef'].isnull().any():
        raise ValueError('Unmatched observations and IDR coefficients.')

    data = data.set_index(['location_id', 'year_id', 'age_group_id', 'sex_id'])

    offset = (1 / data['weights']).clip(0, 1e-14)
    data['obs'] += offset
    data['obs'] = data['obs'].clip(0, 1 - 1e-4)

    manual_coef = calc_manual_idr_coef(data, op_data, population)
    data = data.join(manual_coef, how='left').reorder_levels(data.index.names)
    data['coef'] = (
        data['manual_coef'].fillna(0) * data['manuel_coef_weight'].fillna(0)
        +
        data['coef'] * (1 - data['manuel_coef_weight'].fillna(0))
    )
    data = data.drop(['manual_coef', 'manuel_coef_weight'], axis=1)

    idr_corr_factor = data['coef'] * (data['idr'].max() - data['idr'])

    data['obs'] = apply_idr_adjustment(data['obs'], idr_corr_factor)
    data['pred'] = apply_idr_adjustment(data['pred'], idr_corr_factor)

    prediction = data.loc[:, ['pred']]
    prediction['residual'] = np.nan
    prediction['residual_se'] = np.nan

    data = data.drop(['pred', 'coef'], axis=1)

    data.reset_index().to_parquet(model_root / 'data' / 'data.parquet')
    prediction.reset_index().to_parquet(model_root / 'results' / 'spxmod' / 'predictions.parquet')
