from typing import Dict, Iterable, Optional, TYPE_CHECKING

import pandas as pd
import numpy as np

from covid_model_seiir_pipeline.pipeline.regression.model import reslime

if TYPE_CHECKING:
    # The model subpackage is a library for the pipeline stage and shouldn't
    # explicitly depend on things outside the subpackage.
    from covid_model_seiir_pipeline.pipeline.regression.specification import CovariateSpecification


def run_beta_regression(beta_fit: pd.Series,
                        covariates: pd.DataFrame,
                        covariate_specs: Iterable['CovariateSpecification'],
                        gaussian_priors: Dict[str, pd.DataFrame],
                        prior_coefficients: Optional[pd.DataFrame],
                        hierarchy: pd.DataFrame) -> pd.DataFrame:
    regression_inputs = prep_regression_inputs(
        beta_fit,
        covariates,
        hierarchy
    )
    predictor_set, fixed_coefficients = build_predictors(
        regression_inputs,
        covariate_specs,
        gaussian_priors,
        prior_coefficients,
    )
    mr_data = reslime.MRData(
        data=regression_inputs.reset_index(),
        response_column='ln_beta',
        predictors=[p.name for p in predictor_set],
        group_columns=['super_region_id', 'region_id', 'location_id'],
    )
    mr_model = reslime.MRModel(mr_data, predictor_set)
    coefficients = mr_model.fit_model().reset_index(level=['super_region_id', 'region_id'], drop=True)
    coefficients = pd.concat([coefficients] + fixed_coefficients, axis=1)
    return coefficients


def prep_regression_inputs(beta_fit: pd.Series,
                           covariates: pd.DataFrame,
                           hierarchy: pd.DataFrame):
    regression_inputs = pd.merge(beta_fit.dropna(), covariates, on=beta_fit.index.names)
    group_cols = ['super_region_id', 'region_id', 'location_id']
    regression_inputs = (regression_inputs
                         .merge(hierarchy[group_cols], on='location_id')
                         .reset_index()
                         .set_index(group_cols)
                         .sort_index())
    regression_inputs['intercept'] = 1.0
    regression_inputs['ln_beta'] = np.log(regression_inputs['beta'])
    return regression_inputs


def build_predictors(regression_inputs: pd.DataFrame,
                     covariate_specs: Iterable['CovariateSpecification'],
                     gaussian_priors: Dict[str, pd.DataFrame],
                     prior_coefficients: Optional[pd.DataFrame]):
    location_ids = pd.Index(regression_inputs.reset_index()['location_id'].unique(), name='location_id')
    predictors = []
    fixed_coefficients = []
    for covariate in covariate_specs:
        if np.all(regression_inputs[covariate.name] == 0):
            fixed_coefficients.append(
                pd.Series(
                    0.0,
                    name=covariate.name,
                    index=location_ids,
                )
            )
        elif prior_coefficients is not None and not covariate.group_level and covariate.name in prior_coefficients:
            coefficient_val = (
                prior_coefficients
                .reset_index()
                .set_index('location_id')[covariate.name]
                .drop_duplicates()
            )
            assert len(coefficient_val.index) == 1
            coefficient_val = pd.Series(coefficient_val.iloc[0], name=covariate.name, index=location_ids)
            fixed_coefficients.append(coefficient_val)
            coefficient_val = coefficient_val.reindex(regression_inputs.index, level='location_id')
            regression_inputs['ln_beta'] -= coefficient_val * regression_inputs[covariate.name]
        else:
            predictor = reslime.PredictorModel.from_specification(covariate)
            if predictor.original_prior == 'data':
                predictor.original_prior = gaussian_priors[covariate.name]
            predictors.append(predictor)
    predictor_set = reslime.PredictorModelSet(predictors)
    return predictor_set, fixed_coefficients

