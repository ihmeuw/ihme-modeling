// Prep parameter distribution data for graphing

**************************************************************
** SET UP
**************************************************************
clear all
set more off
cap restore, not

if (c(os)=="Unix") {
	global root FILEPATH
}

if (c(os)=="Windows") {
	global root FILEPATH
}

// Filepaths
local comp_dir FILEPATH

**************************************************************
** FORMAT IHME MODEL PARAMETERS FOR GRAPHING
**************************************************************
insheet using "`comp_dir'/output/re_age_specific_parameter_bounds.csv", clear comma names

generate source = "IHME Model"
generate log_par_mean = log(par_mean)
generate log_par_lower = log(par_lower)
generate log_par_upper = log(par_upper)

replace cd4 = ">500" if cd4 == "GT500CD4"
replace cd4 = "350-500" if cd4 == "350to500CD4"
replace cd4 = "250-349" if cd4 == "250to349CD4"
replace cd4 = "200-249" if cd4 == "200to249CD4"
replace cd4 = "100-199" if cd4 == "100to199CD4"
replace cd4 = "50-99" if cd4 == "50to99CD4"
replace cd4 = "<50" if cd4 == "LT50CD4"


tempfile ihme
save `ihme', replace

**************************************************************
** FORMAT UNAIDS MODEL PARAMETERS FOR GRAPHING
**************************************************************
insheet using "`comp_dir'/unaids/unaids_pars.csv", clear comma names
rename par par_mean
rename cd4_bin cd4
replace cd4 = ">500" if cd4 == "500"
replace cd4 = "<50" if cd4 == "50"
generate log_par_mean = log(par_mean)
generate source = "UNAIDS"

**************************************************************
** COMBINE AND SAVE
**************************************************************
append using `ihme'
outsheet using "`comp_dir'/unaids/compare_pars_ihme_unaids.csv", replace comma names

