# Fertility Model

Uses ASFR, births, CEB data to estimate ASFR for ages 10-54
Uses a space-time smoothed hierarchical model as the prior for GPR
To run, must clone repo into: 'FILEPATH '

For detailed notes on the entire fertility model see the hub: WEBSITE

## fertility_run_all
  - runs all steps
  - See hub for detailed run instructions: WEBSITE
  - If the pipeline fails: go into FILEPATH  and look for
  the Fert_Pipeline_XXXXXXXXXXXX with the year, month, day, time the run began. 
  Make fixes here. Update the fert_pipeline_run_all.R (at the base of the 
  Fert_Pipeline_XXXXXXXXXXXX directory) and resume (via IDE, qsub, or screen).

## 00_input_data
  - Reads in input data from database
  - Converts births to ASFR
  - Adds VR completeness
  - Adds covariate estimates (female education)
  
## 01_fit_first_stage
  - Parallelized by age/super_reg
  - Fits the first stage model using standard locations to get coefficients
  - Fits a separate model to get source random intercepts
  - Applies adjustments to all non-reference sources
  
## 02_second_stage
  - Parallelized by age
  - Calculate data density
  - Calculate data variance
  - Assign parameters based on data density
  - Space-time smooth predictions
  
## 03_gpr
  - Parallelized by age, location
  - Fits GPR model
  - Saves estimates and draws
  
## 04_rake_agg_by_loc
  - Parallelized by age/parent location
  - Most locations rake to national
  - Some locations (TBD) aggregate to national

## 05a_split_sbh
  - Parallelized by SBH location
  - Calculate cohort parity based on loop 1 estimates
  - Use data parity and loop 1 parity to calculate scalar
  - Split SBH using scalar
  
## 05b_split_tb
  - Parallelized by TB location
  - Split total births data using loop 1 ASFR estimates
  
## 07_tfr
  - Parallelized by location
  - Calculate TFR for all location years at draw level
  - Save estimates and draws
  
## 08_compile
  - Compile data
  - Compile stage1 and stage2 results
  - Compile GPR results
  

  
## Functions
### space_time.r
  - Calculates time weights using beta distribution
  - Calculates space weights using zeta parameter
  - Generates new, smoothed predictions