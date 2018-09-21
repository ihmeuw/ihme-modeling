set more off
clear all
pause on

set maxvar 32000
set seed 309221
set odbcmgr unixodbc

** parse args from qsub
global sev_calc_dir = "`1'"
global rei_id = "`2'"
global paf_version_id = "`3'" // used to find where paf draws are stored
global continuous = "`4'" // boolean {1,0} indicating if risk is continuous
global testing = "0"

** set useful globals
global functions_dir = "FILEPATH"
global base_dir = "FILEPATH/sev"
global tmp_dir = "FILEPATH/temp"

** load functions we'll need 
adopath + "$sev_calc_dir/core"
adopath + "$sev_calc_dir/core/readers"
adopath + "$functions_dir"

**logging
cap set rmsg on
cap log close
local log_dir = "$base_dir/$paf_version_id/logs"
cap mkdir "$base_dir/$paf_version_id"
cap mkdir `log_dir'
log using "`log_dir'/log_$rei_id.txt", replace
cap mkdir "$base_dir/$paf_version_id/rrmax"
cap mkdir "$base_dir/$paf_version_id/rrmax/draws"
cap mkdir "$base_dir/$paf_version_id/rrmax/summary"

** pull most detailed demographics
get_demographics, gbd_team("epi") clear
local location_ids `r(location_ids)'
local year_ids 1990 1995 2000 2005 2006 2010 2016
local age_group_ids `r(age_group_ids)'
if $testing local location_ids 8 7 23 14 13 129

** save population for later
get_population, year_id("-1") location_id("-1") sex_id("-1") age_group_id("-1") gbd_round_id("4") location_set_id("35") clear
rename population pop_scaled
drop process_version_map_id
tempfile envelope
save "$tmp_dir/pops_$rei_id.dta", replace

** categorical sevs don't use exposures as inputs, only continuous
if $continuous {
    calc_percentiles
    local exposure_percentiles = r(file_path)
}

** both categoricals and continuous risks use RRs as input
prep_rrs, location_ids(`location_ids') age_group_ids(`age_group_ids') year_ids(`year_ids')
local RRs = r(file_path)

** only continuous actually use tmrel. Categorical risks just need RR/pafs
if $continuous {
    prep_tmrels, rr_file(`RRs') exp_file(`exposure_percentiles') location_ids(`location_ids') ///
        year_ids(`year_ids') age_group_ids(`age_group_ids')
    local tmrels = r(file_path)
    local risks_w_tmrel_draws = r(risks_w_tmrel_draws)
}

** pull pafs and merge on RR file for categ or RR/TMREL for continous risks. 
if $continuous {
    prep_pafs_continuous `tmrels' "`risks_w_tmrel_draws'"
}
else {
    prep_pafs_categorical, rrs(`RRs')  location_ids(`location_ids') age_group_ids(`age_group_ids')
}

cap drop _m
** actually calculate sevs
calc_scalars
local results = r(file_path)

** add age/sex/location aggregates      
clear
use `results'
add_both_sex
local both_sex_results = r(both_sex_file_path)
clear
use `results'
append using `both_sex_results'
add_all_age
local all_age_results = r(all_age_file_path)
clear
use `results'
append using `both_sex_results'
add_large_age
local large_age_results = r(large_age_file_path)
clear
use `results'
append using `both_sex_results'
append using `all_age_results'
append using `large_age_results'
add_loc_hierarchy, location_ids(`location_ids') year_ids(`year_ids')
local loc_results = r(loc_file_path)
clear
use `loc_results'
add_age_std 
local asr_results = r(asr_file_path)
clear
use `loc_results'
append using `asr_results'

** Save single year draws and summaries
local draw_dir = "$base_dir/$paf_version_id/draws"
local summary_dir = "$base_dir/$paf_version_id/summary"
cap mkdir "`draw_dir'"
cap mkdir "`summary_dir'"
cap mkdir "`summary_dir'/to_upload"
sort year_id location_id sex_id age_group_id rei_id 
order year_id location_id sex_id age_group_id rei_id 
save "`draw_dir'/$rei_id.dta", replace
if $rei_id == 134 {
    preserve
        keep if sex_id == 1
        replace rei_id = 245
        save "`draw_dir'/245.dta", replace
    restore
    preserve
        keep if sex_id == 2
        replace rei_id = 244
        save "`draw_dir'/244.dta", replace
    restore
}
fastrowmean sev*, mean_var_name(val)
tempfile all
save `all',replace
fastpctile sev*, pct(2.5 97.5) names(lower upper)
drop sev*
save "`summary_dir'/$rei_id.dta", replace
if $rei_id == 134 {
    preserve
        keep if sex_id == 1
        replace rei_id = 245
        save "`summary_dir'/245.dta", replace
    restore
    preserve
        keep if sex_id == 2
        replace rei_id = 244
        save "`summary_dir'/244.dta", replace
    restore
}

** save csv to upload to dabtabase single year
gen measure_id = 29
gen metric_id = 3
sort year_id location_id sex_id age_group_id rei_id metric_id
order measure_id year_id location_id sex_id age_group_id rei_id metric_id val upper lower
keep measure_id year_id location_id sex_id age_group_id rei_id metric_id val upper lower
export delimited "`summary_dir'/to_upload/single_year_$rei_id.csv", replace
if $rei_id == 134 {
    preserve
        keep if sex_id == 1
        replace rei_id = 245
        export delimited "`summary_dir'/to_upload/single_year_245.csv", replace
    restore
    preserve
        keep if sex_id == 2
        replace rei_id = 244
        export delimited "`summary_dir'/to_upload/single_year_244.csv", replace
    restore
}
** save csv to upload to dabtabase multi year
use `all', clear
keep if inlist(year_id,1990,2006,2016)
gen measure_id = 29
gen metric_id = 3
rename (sev_* val) (sev_*_ val_)
reshape wide sev_* val_, i(measure_id location_id sex_id age_group_id rei_id metric_id) j(year_id)
forvalues i = 0/999 {
    gen double sev_`i'_9005 = (sev_`i'_2006-sev_`i'_1990)/sev_`i'_1990
    gen double sev_`i'_0516 = (sev_`i'_2016-sev_`i'_2006)/sev_`i'_2006
    gen double sev_`i'_9016 = (sev_`i'_2016-sev_`i'_1990)/sev_`i'_1990

}
local years 9005 0516 9016
foreach year of local years {
    fastpctile sev_*_`year', pct(2.5 97.5) names(lower_`year' upper_`year')
}
gen val_9005 = (val_2006-val_1990)/val_1990
gen val_0516 = (val_2016-val_2006)/val_2006
gen val_9016 = (val_2016-val_1990)/val_1990
drop sev_* val_1990 val_2006 val_2016
reshape long val_ lower_ upper_, i(measure_id location_id sex_id age_group_id rei_id metric_id) j(year_id) string
gen year_start_id = 1990 if inlist(year_id,"9005","9016")
replace year_start_id = 2006 if inlist(year_id,"0516")
gen year_end_id = 2016 if inlist(year_id,"0516","9016")
replace year_end_id = 2006 if inlist(year_id,"9005")
drop year_id
rename *_ *

sort year_start_id year_end_id location_id sex_id age_group_id rei_id metric_id
order measure_id year_start_id year_end_id location_id sex_id age_group_id rei_id metric_id val upper lower
keep measure_id year_start_id year_end_id location_id sex_id age_group_id rei_id metric_id val upper lower
export delimited "`summary_dir'/to_upload/multi_year_$rei_id.csv", replace
if $rei_id == 134 {
    preserve
        keep if sex_id == 1
        replace rei_id = 245
        export delimited "`summary_dir'/to_upload/multi_year_245.csv", replace
    restore
    preserve
        keep if sex_id == 2
        replace rei_id = 244
        export delimited "`summary_dir'/to_upload/multi_year_244.csv", replace
    restore
}
log close

// END