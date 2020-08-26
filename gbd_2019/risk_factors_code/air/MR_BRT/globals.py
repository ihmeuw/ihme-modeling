from datetime import datetime

DATA_DIR = 'FILEPATH'

IER_DIR = 'FILEPATH'

BIAS_COLS = ['cv_subpopulation', 'cv_exposure_population', 'cv_exposure_selfreport', 'cv_exposure_study',
             'cv_outcome_selfreport', 'cv_outcome_unblinded', 'cv_reverse_causation', 'cv_confounding_nonrandom',
             'cv_counfounding.uncontroled_a', 'cv_counfounding.uncontroled_b',
             'cv_selection_bias_a', 'cv_selection_bias_b',
             'incidence']

OUT_DIR = f'FILEPATH_{datetime.now().strftime("%m_%d")}'
