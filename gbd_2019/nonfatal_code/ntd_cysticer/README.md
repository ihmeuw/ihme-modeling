# ntd_cysticer execution  

___

## Nonfatal

### Sex CW ---> (Main) Dismod ---> PNAR ---> Epilepsy Env. ---> Save NF 

#### *Crosswalk*

1. Fit age/sex crosswalk  
    **RStudio** `step_0_crosswalker_NCC.R` --- save bundle_versions  
    **RStudio** `step_0_sex_xw_fit.R` --- run mr_brt sex crosswalk  

2. Apply Crosswalks  
    **RStudio** `step_0_crosswalker.R` --- apply mr_brt crosswalk to analytical dataset  

#### *Main Model*
  **Dismod Epi GUI** `ADDRESS (ADDRESS Edit Model)` --- Runs for 2hrs on cluster  

#### *Post Main Processing*

1. **Ipython CLI** `%run cli.py --acause "ntd_cysticer" --step 1` --- Gen PNAR (pop. not at risk)  

2. **Ipython CLI** `%run cli.py --acause "ntd_cysticer" --step 2` -- Modify prevalence with epilepsy envelope and generated PNAR, save NF 

Can run the full nonfatal pipeline by listing all above steps: `--steps 1 2` in one command.
___

## Fatal


### Nonfatal ---> Build COD/Covariates ---> Save COD 

#### *Main Model*

3.  **Ipython CLI** `%run cli.py --acause "ntd_cysticer" --step 3` --- Build COD Dataset/Process out to Draws, Save COD Model 
    **stata-mp** `FILEPATH` (Manual Exec.)  
            - (Child) `FILEPATH`  
            - (Child) `FILEPATH`  

___

# ntd_cysticer methods

___

## Nonfatal

Dismod model with country level fixed effects covariates on religion muslim, pigs raised per capita, sociodemographic index.  
Post-regression adjusted for religion proportion of muslims and the epilepsy envelope.

___

## Fatal

Loads covariate ids: [ADDRESS], runs a mixed effects poisson on :  
- cod_study_deaths, age_group_id, super_region_id, sex_id MCI, logit_sanitation, religion_muslim_prop, pop_dens_under_150_psqkm_pct.  
- Also using random effects on location_id and regional age_group_id.