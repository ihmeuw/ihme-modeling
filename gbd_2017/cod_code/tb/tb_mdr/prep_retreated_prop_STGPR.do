// Description: prep proportions of previously treated cases for ST-GPR

// Definitions
// c_newinc: Total of new and relapse cases and cases with unknown previous TB treatment history
// ret_nrel: Previously treated patients, excluding relapse cases (pulmonary or extrapulmonary, bacteriologically confirmed or clinically diagnosed) **[available starting from 2013)
// ret_rel: Relapse cases
// ret_taf: Treatment after failure cases
// ret_tad:	Treatment after default cases
// ret_oth:	Other re-treatment cases
// ret_rel_labconf:	Relapse  pulmonary bacteriologically confirmed TB cases (smear positive or culture positive or positive by WHO-recommended rapid diagnostics such as Xpert MTB/RIF) **[available starting from 2013)
// ret_rel_clindx:	Relapse pulmonary clinically diagnosed TB cases (not bacteriologically confirmed as positive for TB, but diagnosed with active TB by a clinician or another medical practitioner who has decided to give the patient a full course of TB treatment) **[available starting from 2013)
// ret_rel_ep: Relapse extrapulmonary cases (bacteriologically confirmed or clinically diagnosed) **[available starting from 2013)

***********************************************************************************************************
// calculate % retreated cases

insheet using "FILEPATH", comma names clear

drop if c_newinc==.

keep iso3 year c_newinc ret_nrel ret_rel ret_taf ret_tad ret_oth ret_rel_labconf ret_rel_clindx	ret_rel_ep

// compute total number of relapse cases for 2013 onward
egen rel_total= rowtotal(ret_rel_labconf ret_rel_clindx ret_rel_ep)

// replace ret_rel with rel_total for 2013 onward
replace ret_rel=rel_total if ret_rel==.

// compute total number of retreated cases without relapse for 2012 and before
egen ret_nrel_total=rowtotal(ret_taf ret_tad ret_oth)

// replace ret_nrel with ret_nrel_total for 2012 and before

replace ret_nrel=ret_nrel_total if ret_nrel==.

// compute total TB cases (new+relapse+retreated)

gen all_tb_cases=c_newinc+ret_nrel

// compute % retreated-no relapse-cases

gen ret_nrel_prop=ret_nrel/all_tb_cases

replace ret_nrel_prop=0 if all_tb_cases==0

drop if ret_nrel_prop==0 | ret_nrel_prop==.


// prep for ST-GPR

gen variance=(ret_nrel_prop*(1-ret_nrel_prop))/all_tb_cases

gen me_name="TB_retreated"
gen nid=126384
gen sample_size=all_tb_cases
gen sex_id=3
gen age_group_id=22
gen data=ret_nrel_prop
gen year_id=year

sort iso3 year_id

rename iso3 ihme_loc_id

merge m:1 ihme_loc_id using "FILEPATH", keepusing(location_id) keep(3)nogen

keep location_id year_id age_group_id sex_id me_name nid data variance sample_size ihme_loc_id

order location_id ihme_loc_id year_id age_group_id sex_id me_name nid data variance sample_size 

outsheet using "FILEPATH", comma names replace  


