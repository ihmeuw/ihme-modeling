// Description: calculate proportions of new cases with MDR and retreated cases with MDR

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


// read in new data
clear all 

set more off

import excel using "FILEPATH", firstrow clear

rename location_name country

rename year_id year

drop iso3

gen iso3="SGP"

drop area_desc

gen area_desc=""

keep country year iso3	area_desc dst_rlt_new  mdr_new	dst_rlt_ret	mdr_ret	dst_rlt_unk	mdr_unk	

tempfile SGP
save `SGP', replace

import excel using "FILEPATH", firstrow clear
rename location_name country
rename year_id year
drop file

gen iso3="MOZ"

drop area_desc

gen area_desc=""

keep country year iso3	area_desc dst_rlt_new mdr_new dst_rlt_ret mdr_ret dst_rlt_unk mdr_unk	

tempfile MOZ
save `MOZ', replace


insheet using "FILEPATH", comma names clear

append using `SGP'

append using `MOZ'


replace iso3="CHN_354" if iso3=="HKG"

replace iso3="CHN_361" if iso3=="MAC"


gen ihme_loc_id=iso3

merge m:1 ihme_loc_id using "FILEPATH", keepusing(location_id region_name super_region_name) keep(3)nogen

rename year year_id


// assign location_ids to RUS subnationals

preserve

keep if iso3=="RUS"

tempfile RUS
save `RUS', replace

restore

drop if iso3=="RUS" 

tempfile no_RUS
save `no_RUS', replace

use `RUS', clear
replace country=area_desc

rename country location_name

drop iso3 ihme_loc_id location_id

// correct naming inconsistency

replace location_name="Republic of Adygeya" if location_name=="Adygea Republic"

replace location_name="Arkhangelsk oblast without Nenets autonomous district" if location_name=="Arkhangelsk Oblast"
	
replace location_name="Belgorod oblast" if location_name=="Belgorod Oblast"
	
replace location_name="Bryansk oblast" if location_name=="Bryansk Oblast"
	
replace location_name="Chukchi autonomous area" if location_name=="Chukotka Autonomous Okrug"
	
replace location_name="Chuvash Republic" if location_name=="Chuvasia Republic"
	
replace location_name="Ivanovo oblast" if location_name=="Ivanovo Oblast"
	
replace location_name="Kaliningrad oblast" if location_name=="Kaliningrad Oblast"
	
replace location_name="Kamchatka kray" if location_name=="Kamchatka Krai Oblast"
	
replace location_name="Republic of Karelia" if location_name=="Karelia Republic"
	
replace location_name="Kemerovo oblast" if location_name=="Kemerovo Oblast"
	
replace location_name="Khabarovsk kray" if location_name=="Khabarovsk Krai"
	
replace location_name="Republic of Khakasia" if location_name=="Khakassia Republic"
	
replace location_name="Kostroma oblast" if location_name=="Kostroma Oblast"
	
replace location_name="Leningrad oblast" if location_name=="Leningrad Oblast"
	
replace location_name="Republic of Mariy El" if location_name=="Mary El Republic"
	
replace location_name="Republic of Mariy El" if location_name=="Mary-El Republic"
	
replace location_name="Murmansk oblast" if location_name=="Murmansk Oblast"
	
replace location_name="Nizhny Novgorod oblast" if location_name=="Nizhni Novgorod Oblast"
	
replace location_name="Novgorod oblast" if location_name=="Novgorod Oblast"
	
replace location_name="Oryol oblast" if location_name=="Orel Oblast"
	
replace location_name="Penza oblast" if location_name=="Penza Oblast"
	
replace location_name="Primorsky kray" if location_name=="Primorsky Krai"
	
replace location_name="Pskov oblast" if location_name=="Pskov Oblast"

replace location_name="Republic of Sakha (Yakutia)" if location_name=="Sakha (Yakutia) Republic"	

replace location_name="Sakhalin oblast" if location_name=="Sakhalin Oblast"	
	
replace location_name="Samara oblast" if location_name=="Samara Oblast"
	
replace location_name="Tambov oblast" if location_name=="Tambov Oblast"
	
replace location_name="Tomsk oblast" if location_name=="Tomsk Oblast"
	
replace location_name="Tula oblast" if location_name=="Tula Oblast"
	
replace location_name="Ulyanovsk oblast" if location_name=="Ulyanovsk Oblast"
	
replace location_name="Vladimir oblast" if location_name=="Vladimir Oblast"
	
replace location_name="Volgograd oblast" if location_name=="Vologda Oblast"

replace location_name="Voronezh oblast" if location_name=="Voronezh Oblast"	
	
	
merge m:m location_name using "FILEPATH", keepusing(location_id ihme_loc_id) keep(3)nogen

gen iso3=ihme_loc_id

append using `no_RUS'

sort location_id year

drop country location_name

tempfile mdr_all
save `mdr_all', replace

// prep for ST GPR 

// prep MDR among new cases of TB

// compute mdr_new proportions

egen mdr_new_total=rowtotal(mdr_new mdr_unk)

egen dst_new_total=rowtotal(dst_rlt_new dst_rlt_unk)

gen mdr_prop_new=mdr_new_total/dst_new_total

gen me_name="mdr_new"
gen nid=292791
gen sample_size=dst_new_total 
gen sex_id=3 
gen age_group_id=22   
gen data=mdr_prop_new
/* gen year_id=year */

gen variance= (data*(1-data))/sample_size

replace variance =(1/sample_size * data * (1 - data) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if data==0 & year_id>=1990

keep location_id year_id age_group_id sex_id me_name nid data variance sample_size data ihme_loc_id

order location_id ihme_loc_id year_id age_group_id sex_id me_name nid data variance sample_size 

drop if data==.

tempfile mdr_new
save `mdr_new', replace

// add zeros for 1970-1979

duplicates drop location_id, force

replace year_id=1970

replace data=0

replace variance=0

expand 2, gen (new)

replace year_id=1980 if new==1

drop new

tempfile zeros
save `zeros', replace

use `mdr_new', clear
append using `zeros'

drop if sample_size==0


// drop one outlier

drop if ihme_loc_id=="KHM" & year_id==2001

drop if data==.

outsheet using "FILEPATH", comma names replace  


** ************ prep MDR among retreated cases of TB ******************************************

use `mdr_all', clear


// drop outliers

drop if ihme_loc_id=="CYP" & year_id==2007

drop if ihme_loc_id=="BHR" & year_id==2012

drop if ihme_loc_id=="ISR" & inlist(year_id,2000,2006,2013)

drop if ihme_loc_id=="OMN" & inlist(year_id,2000,2004)

// combine cases and sample size across years for CYP and ISL

preserve

keep if ihme_loc_id=="CYP" & inlist(year_id,2009, 2011, 2013)

collapse (sum) mdr_ret dst_rlt_ret, by (iso3 ihme_loc_id location_id) fast

gen year_id=2011

tempfile CYP
save `CYP', replace

restore

preserve

keep if ihme_loc_id=="ISL" & inlist(year_id, 2005, 2007, 2009, 2012)

collapse (sum) mdr_ret dst_rlt_ret, by (iso3 ihme_loc_id location_id) fast

gen year_id=2008

tempfile ISL
save `ISL', replace

restore

drop if ihme_loc_id=="CYP" & inlist(year_id,2009, 2011, 2013)

drop if ihme_loc_id=="ISL" & inlist(year_id, 2005, 2007, 2009, 2012)

append using `CYP'

append using `ISL'

// compute mdr_ret proportions
gen mdr_prop_ret=mdr_ret/dst_rlt_ret

gen me_name="mdr_ret"
gen nid=292791
gen sample_size=dst_rlt_ret 
gen sex_id=3 
gen age_group_id=22   
gen data=mdr_prop_ret
/* gen year_id=year */

gen variance= (data*(1-data))/sample_size

replace variance =(1/sample_size * data * (1 - data) + 1/(4 * sample_size^2) * invnormal(0.975)^2) if data==0 & year_id>=1990

keep location_id year_id age_group_id sex_id me_name nid data variance sample_size data ihme_loc_id

order location_id ihme_loc_id year_id age_group_id sex_id me_name nid data variance sample_size 

drop if data==.

tempfile mdr_ret
save `mdr_ret', replace

// add zeros for 1970-1979

duplicates drop location_id, force

replace year_id=1970

replace data=0

replace variance=0

expand 2, gen (new)

replace year_id=1980 if new==1

drop new

tempfile zeros
save `zeros', replace

use `mdr_ret', clear
append using `zeros'

drop if sample_size==0


outsheet using "FILEPATH", comma names replace  


