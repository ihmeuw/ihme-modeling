** take a set of target names as input
args targets_file
** make sure we are passed an excel file
assert index("`targets_file'", "xlsx")

** save original data
su deaths
local orig_deaths = `r(sum)'

local source = source in 1
tempfile MCCD
save `MCCD', replace

** Set location_set_version_id
local lsvid = 38

** Pull ICD10 data
use "FILEPATH"
tempfile ICD10
save `ICD10', replace

** NOTE:append all ICD10 based sources as well...

** Keep data for countries in southeast asia
preserve
    ** Get all the iso3s from Southeast asia (location_id 9 is the region Southeast Asia)
    odbc load, exec("SELECT DISTINCT map_id as iso3 FROM shared.location_hierarchy_history WHERE path_to_top_parent LIKE '%,9,%' AND location_set_version_id=`lsvid'") dsn(prodcod) clear
    levelsof iso3, local(iso3s) clean
restore
gen keep_this=0 
foreach iso3 of local iso3s {
        replace keep_this = 1 if iso3=="`iso3'"    
}
** need MCCD ages to be present in the dataset for age-based percentages, so drop missing age
** I want this to fail if there are new age formats in the data since time of writing;
** if there are more complex age formats, it might be necessary to do more complex aggregation of ages
** the same does not go for infant format because MCCD has no age detail greater than 0-1 years, so all infant formatting will work
count if !inlist(frmat,0, 1, 2, 9) & keep_this==1
assert `r(N)'==0
** need sex
replace keep_this=0 if !inlist(sex,1,2)
keep if keep_this==1
drop keep_this

replace cause = substr(cause, 1, 3)

** collapse the data on age and sex and cause
collapse (sum) deaths*, by(cause sex) fast
** make age long
reshape long deaths, i(cause sex) j(age_group_id)
** won't use missing age_group_id or infant aggregate; will use all age_group_ids for MCCD where frmat==9
su deaths if age_group_id==26
local missingage = `r(sum)'
drop if inlist(age_group_id, 2, 26)

** now make age_group_id into MCCD format 11
** some replaces are here just so I can see the age_group_id format
replace age_group_id = 3 if inlist(age_group_id, 3, 4, 5, 6)
replace age_group_id = 91 if inlist(age_group_id, 91, 92, 93, 94)
replace age_group_id = 7 if inlist(age_group_id, 7, 8)
replace age_group_id = 9 if inlist(age_group_id, 9, 10)
replace age_group_id = 11 if inlist(age_group_id, 11, 12)
replace age_group_id = 13 if inlist(age_group_id, 13, 14)
replace age_group_id = 15 if inlist(age_group_id, 15, 16)
replace age_group_id = 17 if inlist(age_group_id, 17, 18)
replace age_group_id = 19 if age_group_id==19
replace age_group_id = 20 if inlist(age_group_id, 20, 21, 22, 23, 24, 25)
replace age_group_id = 1 if age_group_id==1
count if !inlist(age_group_id, 3, 91, 7, 9, 11, 13) & !inlist(age_group_id, 15, 17, 19, 20, 1)
assert `r(N)'==0

** now collapse all the age_group_id aggregates we just made
collapse (sum) deaths, by(cause sex age_group_id)

su deaths if age_group_id==1
local ageone = `r(sum)'

su deaths if age_group_id!=1
local rest = `r(sum)'
    
** If this fails, the age_group_id 1 aggregate is broken
assert `ageone'-`missingage'==`rest'
tempfile data
save `data', replace

** get the cause-target mappings
import excel using `targets_file', firstrow clear
isid cause
** ICD10 and ICD9 have slightly different structures
** ICD10 has cases where two 'agg' causes have the same targets, and these are stored in columns agg1 and agg2
** ICD9 does not have this and therefore only has the 'agg' column 
capture gen agg2 = ""
capture rename agg agg1
tempfile targets
save `targets', replace

use `data', clear
** unfortunately can't assert anything in this merge, because all of 1, 2, and 3 are reasonable (just hope for some 3s to work with)
** _merge==2 is possible because USERNAME can add targets that don't happen to have deaths in Southeast asia
** _merge==1 is definitely possible because there is going to be data in ICD10 that don't have 
merge m:1 cause using `targets'
count if _merge==3
assert `r(N)'>0
tempfile merge
save `merge', replace
** keep only the targets that are in the ICD10 data conglomerate (drop _merge==2),
** and don't worry about using data that won't be used for disagg (drop _merge==1)
keep if _merge==3
drop _merge

** make only one column representing aggregate cause
rename deaths deaths1
gen deaths2 = deaths1
reshape long agg deaths, i(cause sex age_group_id) j(agg_num)
** in case of ICD9, this will just drop everything with agg_num==2
drop if agg_num==2 & agg==""

** map the targets to their acause and collapse on that
gen target = "acause_" + target_mapping if target_mapping != "_gc"
replace target = "acause__gc_" + cause if target_mapping =="_gc"
drop cause
collapse (sum) deaths, by(sex age_group_id agg target) fast
rename agg cause

** generate the percentages of deaths within the cause-sex-age
bysort sex age_group_id cause: egen pct_split = pc(deaths), prop

** now make sure that there are no cause-sex-ages with 0 total pct_agg
bysort cause sex age_group_id: egen total_pct = total(pct_split)
bysort cause sex age_group_id: gen num_targets = _N
replace pct_split = 1.0/num_targets if total==0

** will merge all deaths26 observations with the all ages splits
replace age_group_id=26 if age_group_id==1

** don't need these anymore
drop deaths total_pct num_targets
isid cause sex age_group_id target
tempfile splits
save `splits', replace

** write logic to make splits file human-readable and save to a directory for USERNAME to look at
save "FILEPATH", replace

local date = c(current_date)
local today = date("`date'", "DMY")
local year = year(`today')
local month = month(`today')
local day = day(`today')
local length : length local month
if `length' == 1 local month = "0`month'"    
local length : length local day
if `length' == 1 local day = "0`day'"
global timestamp = "`year'_`month'_`day'"
save "FILEPATH", replace

use `MCCD', clear

** this joinby allows the cause-sex-age groups to expand when there are targets to match on
** unmatched(master) preserves the data that does not need to be disaggregated
** I think joinby is actually necessary in this case to expand observations; merge m:m will take a random target and pct_split and add it
joinby cause sex age_group_id using `splits', unmatched(master)
replace deaths = deaths*pct_split if _merge==3
replace cause = target if _merge==3
replace cause_name = "" if _merge==3
drop pct_split target _merge

** now that cause has been replaced with target, there will be duplicates because the acause targets may be the same for some aggregate causes
** first ensure that the ordering is right for a varlist range in the collapse by
order deaths, last
order cause
order age_group_id, before(deaths)
collapse (sum) deaths, by(cause-age_group_id) fast
su deaths
local new_deaths = `r(sum)'
** make sure we didn't change deaths by more than a rounding error
assert abs(`orig_deaths'-`new_deaths')<1

** IF THAT PASSES, ALL DONE!


