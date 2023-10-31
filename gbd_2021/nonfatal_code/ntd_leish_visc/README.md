# ntd_leish_visc execution
___

## Non-Fatal (first)

### Sex CW ---> DisMod Age Model
### Aggregate National Incidence Data ---> UR correction ---> STGPR ---> Age/Sex Split ---> Duration Assumptions ---> Incidence / Prevalence estimates

#### *Crosswalk/Pre-Processing*

1. Fit DisMod age pattern
**RStudio** `crosswalks/step_0_sex_xw_fit.R` - create mrbrt fit sex crosswalk object --- save fit object to crosswalks
**RStudio** `crosswalks/step_0b_sexsplit_bundle_6416.R` - use sex split fit object to sex split any all-sex age-specific data points for age model --- save crosswalk version **DisMod Epi GUI** submit age model 

2. Prep data for ST-GPR model
**RStudio** `crosswalks/step_1_underreporting_model.R` - create underreporting model --- save fit object to interms + write out diagnostics (coefficients to interms) `crosswalks/step_1b_apply_underreporting_model.R` - 1) take case data and aggregate to national level, 2) apply underreporting correction and save dataset `crosswalks/step_2_save_crosswalk_version.R` - saves crosswalk for ST-GPR input

#### *Main Model*
3. Launch STGPR Model
**RStudio** `step_3_launch_stgpr.R` --- write out run id to interms

4. Solve for prevalence using duration scalars
**RStudio** `step_4_sequela_split_parent.R` `step_4b_sequela_split_child.R` --- save out draw files to 1458, 1459, 1460 folders in draws folders (write draws out for 1980 - 2020 for all years for CoD model)
custom subnational proportions disaggregate national level stgpr estimates 

#### *Post Main Processing*
5. Save meids and stgpr model
- examples `saves/save_epi.R` `saves/step_7_save_stgpr_model.R`

## Fatal

1. Prep cfr model inputs -- not used gbd 2019/2020 (used amended David cfr instead)
`step_5_prep_cfr_data.R`

2. Run and apply predicted cfrs to get number of deaths (CFR = Deaths / Inc, Deaths = CFR * Inc)
`step_6_cfr_model_parent.R` `step_6b_cfr_model_child.R`

3. Upload deaths
- examples `saves/save_cod.R`
- qlogin instead of interactive rstudio to have more resources (in the R session use source("<filepath>"))
___
ntd_leish visc updates

- vectorize `step_4_seqeula_split_child.R`
- move cfr draws writing from beginning of `step_6_cfr_model_parent.R` to separate earlier script
- consider a standardized under-reporting correction across NTD causes
- increase data inputs in Africa subnationals
- reconsider calculation of CFR (by region or super-region)