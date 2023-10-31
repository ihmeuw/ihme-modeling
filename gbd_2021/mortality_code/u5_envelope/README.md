# Under-5 Envelope

## Goal:

To estimate the envelope (number of deaths) for under-5 age groups. Outputs from this process are also used to get under-1 population within the population modeling process.

## Input: 

- Age-sex draws

- Live births draws

- Human Mortality Database (HMD) life tables (single-year)

## Output:

- Deaths for all under-5 age groups (draws and summaries)

- Person-years for all under-5 age groups (draws and summaries)

## Scripts:

**00_run_all.R**

- Launch all steps of U5 envelope estimates

**01_u5.py**

- Read in empirical life tables from HMD, age-sex results, and live births

- Calculate qx by single year ages and select aggregates

- Fit regression to predict lnqx using lnqx_agg

- Predict single-year qx values

- Re-scale single year age qx value sto match 3q2

- Convert to day qx values

**02_u5_scaling.R**

- Scale subnational envelope to national level for both deaths and person-years

**03_generate_summary_files.py**

- Create summary files from draws