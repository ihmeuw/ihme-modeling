***************************************************************
*** NEONATAL PRETERM-ENCEPH-SEPSIS FRAMEWORK
*** Meta-Analysis for Mild Impairment and Moderate-Severe Impairment
*** Step 2a/6: 02a_meta_analysis.do
***************************************************************

*** Shared Functions:
	- get_location_metadata
	- get_epi_data

**************************************************************

*********************
** General Prep

*********************


clear all
set more off
set maxvar 32000
ssc install estout, replace 
ssc install metan, replace

local j "FILEPATH"

local acause "`1'"
local grouping "`2'"
local parent_dir "`3'"
local dataset "`4'"
local timestamp "`5'" 

di in red "acause is `acause'" 
di in red "grouping is `grouping'" 
di in red "parent_dir is `parent_dir'"
di in red "dataset is `dataset'" 
di in red "timestamp is `timestamp'" 

local out_dir "`parent_dir'/`acause'"
capture mkdir "`out_dir'"



*********************
** Import & Prep dataset

*********************

di in red "importing data"
use "`dataset'", clear

keep location_id year location_name super_region_id mean cases sample_size

** Prep data for meta-analysis and ensure there is a mean (data_val), upper, lower, and se for all datapoints. All must be within [0,1] bound 

rename mean data_val
gen data_val_se = (data_val * (1-data_val)/sample_size)^0.5
gen data_val_lower = data_val - 1.96*data_val_se
gen data_val_upper = data_val + 1.96*data_val_se
replace data_val_lower=0 if data_val_lower<0
replace data_val_upper=1 if (data_val_upper>1 & data_val_upper!=.)


*********************
** Run meta-analysis with random effects. 
*********************


di in red "performing meta-analysis"
metan data_val data_val_lower data_val_upper, random

** Results for sepsis long_mod 

/*
           Study     |     ES    [95% Conf. Interval]     % Weight
---------------------+---------------------------------------------------
1                    |  0.044       0.022     0.066         72.00
2                    |  0.044       0.009     0.079         28.00
---------------------+---------------------------------------------------
D+L pooled ES        |  0.044       0.026     0.063        100.00
---------------------+---------------------------------------------------
 Heterogeneity calculated by formula
  Q = SIGMA_i{ (1/variance_i)*(effect_i - effect_pooled)^2 }
where variance_i = ((upper limit - lower limit)/(2*z))^2

  Heterogeneity chi-squared =   0.00 (d.f. = 1) p = 1.000
  I-squared (variation in ES attributable to heterogeneity) =   0.0%
  Estimate of between-study variance Tau-squared =  0.0000

  Test of ES=0 : z=   4.69 p = 0.000
*/


*********************
** Add meta-analysis results, which are stored in the 'r' vector to the dataset
** Convert from "Both" sexes to "Male" and "Female" (doubling the dataset)

*********************

gen mean = r(ES)
gen lower = r(ci_low)
gen upper = r(ci_upp)
gen se = (mean - lower)/1.96

expand 2, gen(iscopy)
gen sex=2
replace sex=1 if iscopy==1 




*********************
** Save summary stats (mean, lower, upper)

*********************

preserve
keep location_id year sex location_name super_region_id mean lower upper data_val 
di in red "saving summary stats!"
local summ_out_dir "FILEPATH"
local summ_archive_dir "FILEPATH"
capture mkdir "`summ_out_dir'"
capture mkdir "`summ_archive_dir'"
local summ_fname "`acause'_`grouping'_summary"

save "`summ_out_dir'/`summ_fname'.dta", replace
export delimited using "`summ_out_dir'/`summ_fname'.csv", replace
export delimited using "`summ_archive_dir'/`summ_fname'_`timestamp'.csv", replace
restore

*********************
** Generate & save 1000 draws with normal distribution

*********************

keep location_id year sex mean se 


di in red "generating draws"
forvalues i=1/1000{
	gen draw_`i' = rnormal(mean, se)
}


di in red "saving all draws"
drop mean se

local draw_out_dir = "`out_dir'/draws"
capture mkdir "`draw_out_dir'"
local fname "`acause'_`grouping'_draws"

save "`draw_out_dir'/`fname'.dta", replace
export delimited using "`draw_out_dir'/`fname'.csv", replace



