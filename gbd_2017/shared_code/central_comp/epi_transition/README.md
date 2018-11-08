# Epidemiological transition

### Purpose
The epi transition model aims to isolate the relationship between development and disease, injury, and risk factor burden. This adds an useful dimension to the GBD - namely, it provides context to the descriptive statistics found in the GBD results to help identify areas in which a country is in lagging behind what would be expected based on its peers, and where it is surpassing that expectation.

### Functional overview
The estimation process can be broken down into six model types:

1. All-cause mortality
2. Population age structure
3. Population sex structure
4. Cause-specific mortality
5. Years lived with disability (YLD)
6. Summary exposure value (SEV)

From these six sets models, we can produce the full suite of burden estimates, including years of life lost due to premature mortality (YLLs), disability-adjusted life years (DALYs), life expectancy, health-adjusted life expectancy (HALE), and risk attributable burden.

Each of these six models shares the same base model structure: we use gaussian process regression to estimate curves by sociodemographic index (SDI) by age and sex (and cause/risk if relevant). Beyond that, a different set of processes can be prescribed based on the measure in question.
