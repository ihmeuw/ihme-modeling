#ntd_leish_visc execution
___
## Non-Fatal (first)

### Sex CW ---> DisMod Age Model
### Aggregate National Incidence Data ---> UR correction ---> STGPR ---> Age/Sex Split ---> Duration Assumptions ---> Incidence / Prevalence estimates

1. Fit DisMod age pattern
**RStudio** step_0_sex_xw_fit.R - create mrbrt fit sex crosswalk object --- save fit object to crosswalks
**RStudio** step_0_crosswalker.R - use sex split fit object to sex split any all-sex age-specific data points for age model --- save crosswalk version
**DisMod** submit age model 

2. Prep data for ST-GPR model
**RStudio** step_1_underreporting_model.R - create underreporting model --- save fit object to interms + write out diagnostics (coefficients to interms)
step_2_vl_stgpr_data_prep.R - 1) take case data and aggregate to national level, 2) apply underreporting correction and save dataset

3. Launch STGPR Model
step_3_launch_stgpr.R --- write out run id to interms

4. Solve for prevalence using duration scalars
step_4_sequela_split_parent.R step_4b_sequela_split_child.R --- save out draw files to folders in draws folders (write draws out for 1980 - 2019 for all years for CoD model)
custom subnational proportions disaggregate national level stgpr estimates 

1. Save meids

## Fatal

1. Prep cfr model inputs -- used amended cfr
step_5_prep_cfr_data.R

2. Run and apply predicted cfrs to get number of deaths (CFR = Deaths / Inc, Deaths = CFR * Inc)
step_6_cfr_model_parent.R step_6b_cfr_model_child.R

63. Upload deaths

