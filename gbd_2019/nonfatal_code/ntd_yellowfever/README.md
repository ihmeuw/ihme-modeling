# ntd_yellowfever execution  
___
## Nonfatal

###  Dem&Pops Skeleton ---> Clean/Dedup Data ---> Brazil Proportions ---> Age CW (Custom Age Dist) ---> Case fatality studies ---> Expansion Factors ---> Custom model

1. Create demographic skeleton w/ population  
   **RStudio** `step_1_make_skeleton.R` --- Makes demographic skeleton w/ population added  

2. Clean/Dedup  
    **stata-mp** `step_2_data_prep.do` --- Cleans and dedups data, also adds placeholder year columns for >2017 study years  

3. Estimate Brazil proportions  
    **stata-mp** `step_3_brazil_prop.do` --- Creates subnational proportions and endemicity off cod dataset, (Unclear if used in step_7b)  

4. Generate age distribution  
    **stata-mp** `step_4_age_dist.do` --- Uses legacy dataset (legacy subset of bundle), may need updating or new method  

5. Model case fatality  
    **stata-mp** `step_5_case_fatality.do` --- Uses legacy dataset (check if bundle)  

6. Model expansion factors  
    **stata-mp** `step_6_expansion_factor.do` --- Uses legacy Dengue model raw expansion factors  

#### *Main Model*

  **stata-mp**  `step_7_submitModel.do` --- Master script that launches parallel processing  
    - (child) `step_7_submitProcessModel.sh` --- Shell wrapper for child script  
    - (child) `step_7_processModel.do` --- Main model script, runs out nonfatal draws and fatal draws per location, combining all of the above in custom processes  

#### *Post Main Processing*

1. Run Saves  
  **Ipython CLI** `%run cli.py --acause "ntd_yellowfever" --step 1` --- Run saves for NF, recommend checking draw folders have all locations


## Fatal

### Get COD Data ---> Custom Model Deaths (Inc*CF*Pop.) ---> Save COD

#### *Main Model*

 **stata-mp** `step_7_submitModel.do` Runs out draws in (SAME SCRIPT AS NONFATAL)   
    - (child) `step_7_submitProcessModel.sh` --- Shell wrapper for child script  
    - (child) `step_7_processModel.do` --- Main model script, runs out nonfatal draws and fatal draws per location, combining all of the above in custom processes  

#### *Post Main Processing* (NOTE THESE 4 SCRIPTS NEED REFACTOR, SHOULD BE FOR LOOPS, UNCLEAR WHICH/ALL RAN)

1. Get Cod data for Brazil  
    **stata-mp** `step_8_get_cod_bra_subnats_2017.do` --- Pulls cod dataset for brazilian subnationals to use in later shock  

2. Get Cod data for Brazil  
    **stata-mp** `step_8_aug_28_shocks_brazil.do` --- Applies pulled cod dataset as shock  

1. Get Cod data for Brazil  
    **stata-mp** `step_8_aug_29_shocks_brazil_2018.do` --- Applies a custom deaths calc to brazil  

1. Get Cod data for Brazil  
    **stata-mp** `step_8_sep_10_replace_death_uncertainty.R` --- Replace uncertainty, but just dropping first row  

1. Run Saves
    **Ipython CLI** `%run cli.py --acause "ntd_yellowfever" --step 2` --- Run saves for Fatal, recommend checking draw folders have all locations
