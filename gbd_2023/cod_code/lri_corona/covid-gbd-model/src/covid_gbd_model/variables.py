MODEL_YEARS = list(range(2020, 2025))
COV_YEAR_START = 1950
COD_YEAR_START = 1980
RELEASE_ID = 16
PREVALENCE_RELEASE_ID = 9

VARIANTS = [
    'ancestral',
    'alpha',
    'beta',
    'gamma',
    'delta',
    'omicron',
    'ba5',
]

GBD_COVARIATES = {
    'obesity': (2156, 'detailed'),  # Obesity/High BMI (2156: age-/sex-specific high BMI SEV, 453: age-/sex-specific prevalence; 455: age-standardized prevalence)
    'smoking': (2102, 'detailed'),  # Smoking (2102: age-/sex-specific SEV, 155: age-/sex-specific prevalence; 282: age-standardized prevalence)
    'uhc': (1097, 'aggregate'),  # Universal health coverage
    'haq': (1099, 'aggregate'),  # Healthcare access and quality index
}
GBD_CAUSES = {
    'cancer': 410,
    'cvd': 491,
    'copd': 509,
    'diabetes': 587,
    'ckd': 589,
}
