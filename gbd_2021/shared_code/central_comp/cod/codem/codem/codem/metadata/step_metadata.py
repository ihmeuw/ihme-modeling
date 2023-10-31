"""
Contains the metadata for steps, including input files
and upstream tasks for each step
"""


class StepMetadata:
    def __init__(self, step_id, pickled_outputs, pickled_inputs):
        self.step_id = step_id
        self.pickled_inputs = pickled_inputs
        self.pickled_outputs = pickled_outputs


STEP_IDS = {
    'InputData': 0,
    'GenerateKnockouts': 1,
    'CovariateSelection': 2,
    'LinearModelBuilds': 3,
    'ReadSpacetimeModels': 4,
    'ApplySpacetimeSmoothing': 5,
    'ApplyGPSmoothing': 6,
    'ReadLinearModels': 7,
    'SpacetimePV': 8,
    'LinearPV': 9,
    'OptimalPSI': 10,
    'LinearDraws': 11,
    'GPRDraws': 12,
    'EnsemblePredictions': 13,
    'WriteResults': 14,
    'Diagnostics': 15,
    'Email': 16
}

STEP_NAMES = {v: k for k, v in STEP_IDS.items()}


# Step Dictionary Maps a Step ID to the StepMetadata object
STEP_DICTIONARY = {
    'InputData':
        StepMetadata(step_id=STEP_IDS['InputData'],
                     pickled_inputs={},
                     pickled_outputs=[]),

    'GenerateKnockouts':
        StepMetadata(step_id=STEP_IDS['GenerateKnockouts'],
                     pickled_inputs={},
                     pickled_outputs=[]),

    'CovariateSelection':
        StepMetadata(step_id=STEP_IDS['CovariateSelection'],
                     pickled_inputs={},
                     pickled_outputs=[]),

    'LinearModelBuilds':
        StepMetadata(step_id=STEP_IDS['LinearModelBuilds'],
                     pickled_inputs={},
                     pickled_outputs=['response_list']),

    'ReadSpacetimeModels':
        StepMetadata(step_id=STEP_IDS['ReadSpacetimeModels'],
                     pickled_inputs={STEP_IDS['LinearModelBuilds']: ['response_list']},
                     pickled_outputs=['st_models_linear']),

    'ApplySpacetimeSmoothing':
        StepMetadata(step_id=STEP_IDS['ApplySpacetimeSmoothing'],
                     pickled_inputs={STEP_IDS['LinearModelBuilds']: ['response_list'],
                                     STEP_IDS['ReadSpacetimeModels']: ['st_models_linear']},
                     pickled_outputs=['st_models_spacetime']),

    'ApplyGPSmoothing':
        StepMetadata(step_id=STEP_IDS['ApplySpacetimeSmoothing'],
                     pickled_inputs={STEP_IDS['LinearModelBuilds']: ['response_list'],
                                     STEP_IDS['ApplySpacetimeSmoothing']: ['st_models_spacetime']},
                     pickled_outputs=['st_models_gp']),

    'ReadLinearModels':
        StepMetadata(step_id=STEP_IDS['ReadLinearModels'],
                     pickled_inputs={STEP_IDS['LinearModelBuilds']: ['response_list']},
                     pickled_outputs=['linear_models_linear']),

    'SpacetimePV':
        StepMetadata(step_id=STEP_IDS['SpacetimePV'],
                     pickled_inputs={STEP_IDS['LinearModelBuilds']: ['response_list'],
                                     STEP_IDS['ApplyGPSmoothing']: ['st_models_gp']},
                     pickled_outputs=['st_models_pv',
                                      'st_models_rmse_all',
                                      'st_models_trend_all']),

    'LinearPV':
        StepMetadata(step_id=STEP_IDS['LinearPV'],
                     pickled_inputs={STEP_IDS['LinearModelBuilds']: ['response_list'],
                                     STEP_IDS['ReadLinearModels']: ['linear_models_linear']},
                     pickled_outputs=['linear_models_pv',
                                      'linear_models_rmse_all',
                                      'linear_models_trend_all']),

    'OptimalPSI':
        StepMetadata(step_id=STEP_IDS['OptimalPSI'],
                     pickled_inputs={STEP_IDS['LinearPV']: ['linear_models_pv'],
                                     STEP_IDS['SpacetimePV']: ['st_models_pv'],
                                     STEP_IDS['LinearModelBuilds']: ['response_list']},
                     pickled_outputs=['model_pv',
                                      'ensemble_preds',
                                      'st_models_id',
                                      'linear_models_id',
                                      'submodel_covariates',
                                      'submodel_trend',
                                      'submodel_rmse',
                                      'all_st_predictions',
                                      'all_linear_predictions']),

    'LinearDraws':
        StepMetadata(step_id=STEP_IDS['LinearDraws'],
                     pickled_inputs={STEP_IDS['OptimalPSI']: ['linear_models_id'],
                                     STEP_IDS['LinearModelBuilds']: ['response_list']},
                     pickled_outputs=['linear_models_draws']),

    'GPRDraws':
        StepMetadata(step_id=STEP_IDS['GPRDraws'],
                     pickled_inputs={STEP_IDS['OptimalPSI']: ['st_models_id'],
                                     STEP_IDS['LinearModelBuilds']: ['response_list']},
                     pickled_outputs=['st_models_draws']),

    'EnsemblePredictions':
        StepMetadata(step_id=STEP_IDS['EnsemblePredictions'],
                     pickled_inputs={STEP_IDS['OptimalPSI']: ['model_pv'],
                                     STEP_IDS['LinearDraws']: ['linear_models_draws'],
                                     STEP_IDS['GPRDraws']: ['st_models_draws']},
                     pickled_outputs=['model_pv',
                                      'draw_id']),

    'WriteResults':
        StepMetadata(step_id=STEP_IDS['WriteResults'],
                     pickled_inputs={STEP_IDS['LinearModelBuilds']: ['response_list'],
                                     STEP_IDS['EnsemblePredictions']: ['draw_id']},
                     pickled_outputs=[]),

    'Diagnostics':
        StepMetadata(step_id=STEP_IDS['Diagnostics'],
                     pickled_inputs={STEP_IDS['OptimalPSI']: ['submodel_covariates',
                                                              'submodel_trend',
                                                              'submodel_rmse'],
                                     STEP_IDS['EnsemblePredictions']: ['model_pv',
                                                                       'draw_id']},
                     pickled_outputs=[]),

    'Email': StepMetadata(step_id=STEP_IDS['Email'],
                          pickled_inputs={STEP_IDS['EnsemblePredictions']: ['model_pv'],
                                          },
                          pickled_outputs=[])
}
