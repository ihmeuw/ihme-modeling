Rabies
-------

1) Fatal Model
Four submodels:
- male global
- male data rich
- female global
- female data rich

-> hybridize to final model versions
-> mark best

2) Non-Fatal Model

Deaths from CodCorrect -> Assume CFR -> Non-Fatal Prevalence/Incidence

a) step_0_create_run_directory.R
b) step_1_submit_deaths2cases.do (calls step_1b_deaths2cases.do) 
- make sure to update with new run directory 
- will make on purpose if directory already exists (to prevent overwriting)
- record codcorrect version used and date at bottom of script
c) step_2_save_epi.R

Technical Updates for GBD 2020:
- Write in R? Issue with rbinomial 
- Removing shell script so is executable from git
