# ntd_schisto execution

## Nonfatal

### Clean/Dedup Data ---> Sex CW (MR-BRT) ---> Diagnostic CW (MR-BRT) ---> Age CW (DISMOD Curve) ---> Coinfection --->  

### ---> (Main) DISMOD ---> Produce PAR (Niche Raster) ---> Adjust w/ PAR ---> Save NF Id 1---> Coinfection&Species GR Split ---> Save NF ID 2, 3--->  

### ---> Sequela Split ---> Save NF (4-11)  

#### *Pre Main Processing* (In /crosswalks/ dir)

1. Data clean  
    **RStudio** `/crosswalks/step_1_2_pre_dismod\step_1_data_cleaning_diagnostics_all_species.R`  

2. Dedup Data  
    **stata-mp** `/crosswalks/step_1_2_pre_dismod\step_2_de_dup_and_aggregate_data.do`  

3. Fit sex crosswalk / Apply (By Species 3x these files)  
    **RStudio** `step_3_sex_crosswalks/sex_matching_hema.R` --- run mr_brt sex crosswalk, run for step2  
    **RStudio** `step_3_sex_crosswalks/00_crosswalker_hema.R` --- apply fit obj to data, run for step2  
    **RStudio** `step_3_sex_crosswalks/decomp_step4/sep_16_step4_sex_split_all_species.R` --- Run for step4, combined all species to one file  

4. Fit diagnostic crosswalk / Apply (By Species 3x these files)  
    **RStudio** `step_4_diagnostic_crosswalks/decomp_step4/Step4_logit_crosswalk_mansoni_kk3_as_ref.R` --- Fit sex BRT, apply to final_sex_split_data (Find variable in other script)  
        (note) End of this mansoni file rbinds 3x species, could be separate  
        (note) Likely redundant to base versions of code files  

5. Save crosswalk / Fit Dismod / Apply Main Age Pattern  
    **RStudio** `bundle_versioning` --- Lines (148-232) save crosswalk versions of cleaned data to use for dismod age curve  
    **Dismod Epi GUI** `ADDRESS (23900 Edit Model)` --- Runs on cluster, monitor  
    **RStudio** `step_5_emma_age_split_schisto.R` --- Lines (1-344) apply custom_functions age_split to sex_split data, using Dismod model id, and used 2010 regional pattern  

6. Save Stage Spec. Age Patterns for Main Dismod Post-Processing  
    **RStudio** `bundle_versioning` --- Lines (239-318) save crosswalk versions of cleaned data to use for dismod age curve  
    **Dismod Epi GUI** `ADDRESS (Edit Models)` --- Runs on cluster, monitor  

7. Model co-infection and some outliering  
    **stata-mp** `step_7_modeling_coinfection.do`  Is final processing prior to main DISMOD model  
    **RStudio** `bundle_versioning` --- Lines (130-144) save crosswalk versions of final data to use for dismod main curve  

(NOTE) Archive (pre_upload_data_management.do)  

#### *Main Model*
  **DISMOD** `ADDRESS ( Edit Model)` --- Runs on cluster, monitor  

#### *Post Main Processing*

1. **RStudio** `ntd_models/ntd_schisto/extract_PAR_non_urban_mask` --- Run PAR rasters using BRT, Extract to threshold csvs, combine props to full .dta file, red/green/pink  
    - (1) **RStudio** `extract_PAR_non_urban_mask/niche_brt/runGBM.R`  --- Run out BRT rasters  
    - (2) **RStudio?**  `extract_PAR_non_urban_mask/extract_par_species.R` --- Will launch parallel script to create prop/threshold from rasters  
        (child) `extract_PAR_non_urban_mask/extract_par_species_parallel.R`  
    - (3) **stata-mp** `extract_PAR_non_urban_mask/combine_par.do`  --- Will combine prop/threshold to par_draws.dta format for later steps, multiple versions  

2. **Ipython CLI** `%run cli.py --acause "ntd_schisto" --step 1` --- Apply Par Adjustment and GR to Dismod model, saves NF meid 1

3. **Ipython CLI** `%run cli.py --acause "ntd_schisto" --step 2` --- Runs coinfection script  

4. **Ipython CLI** `%run cli.py --acause "ntd_schisto" --step 3` --- Uses saved NF ID 1 and coinfection output to then species split, saves NF IDs (2, 3)  

5. **Ipython CLI** `%run cli.py --acause "ntd_schisto" --step 4` --- Runs out sequela splits, saves NF IDs (4-11)  

## Fatal

### Nonfatal Meid ID 1 ---> Build COD/Covariates ---> Save COD

#### *Main Model*

1.  **Ipython CLI** `%run cli.py --acause "ntd_schisto" --step 5` --- Build COD Dataset/Process out to Draws, Save COD Model 352  
    **stata-mp** `schisto_cod.do` (Manual Exec.)  
            - (Child) `custom_functions/build_cod_dataset.ado`  
            - (Child) `custom_functions/process_predictions.ado`  
