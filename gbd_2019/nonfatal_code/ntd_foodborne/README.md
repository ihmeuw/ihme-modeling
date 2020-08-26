ntd_foodborne execution
-----------------------

Nonfatal only

All-Species Age Data
All-Species Sex CW -> DisMod All-Species Age Model

Species-Specific Data
All-Species Sex CW -> All-Species Age CW -> DisMod Prevalence Models -> Geographic Restrictions 
* EPIC Finishes Sequela Splitting after Geographic Restrictions 

Crosswalk
1. Fit sex crosswalk model               `step_0_sex_crosswalk_model.R`
2. Sex crosswalk age data                `step_0_crosswalker.R`
3. Upload crosswalk version              `step_0_crosswalker.R`
4. Run DisMod age model                  *DisMod*

Main Model
1. Sex crosswalk species-specific bundles `step_0_crosswalker.R`
2. Age crosswalk species-specific bundles `step_0_crosswalker.R`
3. Upload crosswalk versions (5)          `step_0_crosswalker.R`
4. Run species-specific dismod prevalence models *DisMod*

Post-Processing
1. Apply geographic restrictions `step_1_apply_grs_parent.R`
2. Save meids                    `step_2_save_meids_parent.R`

Central Comp EPIC sequela splits and submits final versions

Technical Updates in 2020:
Different array jobs for geographic restrictions for endemic and non-endemic locations
Proper profiling of qsub
formalizing resubmit 
passing descriptions to save child script
single run all script

# ntd_foodborne execution  

___

## Nonfatal

### all-species sex CW ---> all-species age-crosswalk ---> |  all-species age pattern |

### all-species sex CW & all-species age CW ---> species specific analytical bundles  ---> crosswalked species-specific bundles ---> DisMod ---> GR restrict ---> |complete time series|


#### *Crosswalk/Pre-Processing*

1. Fit sex crosswalk  
    **RStudio** `crosswalks/step_0_sex_crosswalk_model.R` 
2. Apply sex crosswalk to age-specific bundle ###
    **RStudio** `crosswalks/step_0_crosswalker.R` 
3. Run DisMod age patterns -- meids ### 
   **Dismod Epi GUI**
4. Apply sex crosswalk and then all-specices age-pattern to species-specific data sets
    **RStudio** `crosswalk/step_0_crosswalker.R` --- apply mr_brt crosswalk to analytical datasets

#### *Main Model*
1. Run DisMod species-specific crosswalk versions 
   **Dismod Epi GUI** 

#### *Post Main Processing*

2. **Ipython CLI** ``%run cli.py --acause "ntd_foodborne" --steps 1 --decomp_step "step2" --gen_rundir "<insert_message>" --mark_best True`` --- Runs all GR (custom) / age restriction
   face.pipe.workflow.execute()

__

# ntd_foodborne methods

## Nonfatal

Pre-Processing
- fit MR-BRT sex crosswalk to adjust both sex data to Male and Female
- fit all-species age pattern to adjust all-age data

Main Model
- model species-specific prevalence data to estimate complete prevalence/incidence time series by location/age/sex/year

Post-Procession
- geographically restrict where there is no *endemic transmission* of FBTs
- age restrict under 2-4 years old
