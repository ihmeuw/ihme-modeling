*** this is specifically for South Africa

clear
set more off
clear matrix
set memory 5000m

    if c(os) == "Unix" {
        global dirs "FILEPATH"
		adopath + "FILEPATH"
		global nn = `1'
		di $nn
		local sex = "`2'"
		local region_id = `3'
		adopath + "FILEPATH"
    }
    else if c(os) == "Windows" {
        global dirs "FILEPATH"
		adopath + "FILEPATH"
		global nn = 399
		local sex = "male"
		local region_id = 192
    }


qui do "FILEPATH/get_locations.ado"
get_locations
tempfile gbds unis
save `gbds'
preserve
keep if region_id == `region_id'
keep ihme_loc_id 
tempfile regs
save `regs', replace
restore
keep ihme_loc_id local_id_2013
rename local iso3
duplicates drop ihme, force
save `unis'



insheet using "FILEPATH/hiv_rr_$nn.csv", clear
destring age hivrr, force replace
replace hivrr=1 if hivrr==.
keep if sim==$nn
reshape wide hivrr, i(ihme sex year) j(age)
forvalues j=85(5)110 {
	cap gen hivrr`j'=hivrr80
}
reshape long hivrr, i(ihme sex year) j(age)
reshape wide hivrr, i(ihme sex age) j(year)
reshape long hivrr, i(ihme sex age) j(year)
replace hivrr=1 if hivrr==.

tempfile hivrrs
save `hivrrs'

count if ihme=="ZAF"

if `r(N)'==0 {
	keep if ihme=="ZAF_488"
	replace ihme="ZAF"
	append using `hivrrs'
	save `hivrrs',replace
}

	

global datadir "FILEPATH"
use "$datadir/ltbase",clear
keep if sex == "`sex'"
tempfile base
save `base'

insheet using "FILEPATH/input_data.txt",clear
keep ihme_loc_id sex year hiv
keep if sex == "`sex'"
duplicates drop ihme sex year, force
rename hiv adult_CDR
replace year=floor(year)
merge 1:m ihme sex year using `base'
drop if _merge==1
drop _merge
drop if adult_CDR>=0.001 & adult_CDR!=.
gen ciso=substr(ihme, 1,3)
save `base',replace


tempfile zafbase
use "$datadir/ZAF_life_tables.dta",clear
keep if sex == "`sex'"
gen ciso=substr(ihme, 1,3)
save `zafbase'


tempfile usabase
use "$datadir/usa_life_tables.dta",clear
keep if sex == "`sex'"
gen ciso=substr(ihme, 1,3)
save `usabase'


tempfile smallbase
use "$datadir/ltbase_small.dta",clear
keep if sex == "`sex'"
gen ciso=substr(ihme, 1,3)
levelsof ihme, local(smallcs)
save `smallbase'


use "FILEPATH/weights_all levels.dta",clear
keep if sex == "`sex'"
replace weights= 0 if lag>15 & lag<-15
tempfile wlevel
save `wlevel'

use `gbds',clear
keep ihme_loc region_name super_region_name
merge 1:1 ihme_loc_id using `regs'
keep if _m == 3
drop _m

gen ciso=substr(ihme,1,3)
merge 1:m ihme using "FILEPATH/entry_$nn.dta"

cap replace v5q0=0.90 if v5q0>=1

keep if sex == "`sex'"
keep if _m == 3
drop _merge
keep ihme sex year v5q0 v45q15 *CDR region_name super ciso pred1wre secsim p5m0
keep if year>1949
replace adult_CDR=0 if adult_CDR==.
replace kid_CDR=0 if kid_CDR==.
gen sim=$nn
merge m:1 sex using "$datadir/hivratio_kid.dta", nogen
drop if sex != "`sex'"
merge m:1 sex using "$datadir/hivratio_adult.dta", nogen
drop if sex != "`sex'"
rename v5q0 obs_v5q0
rename v45q15 obs_v45q15
tempfile changehiv
save `changehiv'

local secsim=secsim[1]

use "$datadir/nohivapplication.dta",clear
keep if sex == "`sex'"
keep ihme sex
merge 1:m ihme sex using `changehiv'
drop if _m == 1
drop _merge


replace adult_CDR=adult_CDR*0.7 if ihme=="GHA" & sex=="male"
replace adult_CDR=adult_CDR*0.7 if ihme=="HTI"
replace adult_CDR=adult_CDR*0.7 if ihme=="IND_43902"
replace adult_CDR=adult_CDR*0.8 if ihme=="KEN"
replace adult_CDR=adult_CDR*0.8 if ihme=="KEN_35618"
replace adult_CDR=adult_CDR*0.8 if ihme=="KEN_35619"
replace adult_CDR=adult_CDR*0.8 if ihme=="KEN_35620" & sex=="female"
replace adult_CDR=adult_CDR*0.8 if ihme=="KEN_35621"
replace adult_CDR=adult_CDR*0.7 if ihme=="KEN_35622"
replace adult_CDR=adult_CDR*0.5 if ihme=="KEN_35623"
replace adult_CDR=adult_CDR*0.8 if ihme=="KEN_35627" & sex=="female"
replace adult_CDR=adult_CDR*0.7 if ihme=="KEN_35630"
replace adult_CDR=adult_CDR*0.7 if ihme=="KEN_35631" & sex=="female"
replace adult_CDR=adult_CDR*0.6 if ihme=="KEN_35632" & sex=="male"
replace adult_CDR=adult_CDR*0.8 if ihme=="KEN_35632" & sex=="female"
replace adult_CDR=adult_CDR*0.7 if ihme=="KEN_35633" & sex=="female"
replace adult_CDR=adult_CDR*0.8 if ihme=="KEN_35634" & sex=="male"
replace adult_CDR=adult_CDR*0.8 if ihme=="KEN_35635" & sex=="male"
replace adult_CDR=adult_CDR*0.8 if ihme=="KEN_35636"
replace adult_CDR=adult_CDR*0.8 if ihme=="KEN_35637" & sex=="male"
replace adult_CDR=adult_CDR*0.9 if ihme=="KEN_35637" & sex=="female"
replace adult_CDR=adult_CDR*0.7 if ihme=="KEN_35638"
replace adult_CDR=adult_CDR*0.6 if ihme=="KEN_35640" & sex=="male"
replace adult_CDR=adult_CDR*0.7 if ihme=="KEN_35641"
replace adult_CDR=adult_CDR*0.7 if ihme=="KEN_35642" & sex=="male"
replace adult_CDR=adult_CDR*0.9 if ihme=="KEN_35643"
replace adult_CDR=adult_CDR*0.8 if ihme=="KEN_35644"
replace adult_CDR=adult_CDR*0.8 if ihme=="KEN_35646"
replace adult_CDR=adult_CDR*0.8 if ihme=="KEN_35647"
replace adult_CDR=adult_CDR*0.7 if ihme=="KEN_35648" & sex=="female"
replace adult_CDR=adult_CDR*0.8 if ihme=="KEN_35650"
replace adult_CDR=adult_CDR*0.6 if ihme=="KEN_35654"
replace adult_CDR=adult_CDR*0.7 if ihme=="KEN_35660" & sex=="male"
replace adult_CDR=adult_CDR*0.8 if ihme=="KEN_44794"
replace adult_CDR=adult_CDR*0.8 if ihme=="KEN_44795"
replace adult_CDR=adult_CDR*0.8 if ihme=="KEN_44796"
replace adult_CDR=adult_CDR*0.6 if ihme=="KEN_44797"
replace adult_CDR=adult_CDR*0.9 if ihme=="KEN_44800"
replace adult_CDR=adult_CDR*0.6 if ihme=="ZAF_484"

replace kid_CDR=kid_CDR*0.7 if ihme=="GHA" & sex=="male"
replace kid_CDR=kid_CDR*0.7 if ihme=="HTI"
replace kid_CDR=kid_CDR*0.7 if ihme=="IND_43902"
replace kid_CDR=kid_CDR*0.8 if ihme=="KEN"
replace kid_CDR=kid_CDR*0.8 if ihme=="KEN_35618"
replace kid_CDR=kid_CDR*0.8 if ihme=="KEN_35619"
replace kid_CDR=kid_CDR*0.8 if ihme=="KEN_35620" & sex=="female"
replace kid_CDR=kid_CDR*0.8 if ihme=="KEN_35621"
replace kid_CDR=kid_CDR*0.7 if ihme=="KEN_35622"
replace kid_CDR=kid_CDR*0.5 if ihme=="KEN_35623"
replace kid_CDR=kid_CDR*0.8 if ihme=="KEN_35627" & sex=="female"
replace kid_CDR=kid_CDR*0.7 if ihme=="KEN_35630"
replace kid_CDR=kid_CDR*0.7 if ihme=="KEN_35631" & sex=="female"
replace kid_CDR=kid_CDR*0.6 if ihme=="KEN_35632" & sex=="male"
replace kid_CDR=kid_CDR*0.8 if ihme=="KEN_35632" & sex=="female"
replace kid_CDR=kid_CDR*0.7 if ihme=="KEN_35633" & sex=="female"
replace kid_CDR=kid_CDR*0.8 if ihme=="KEN_35634" & sex=="male"
replace kid_CDR=kid_CDR*0.8 if ihme=="KEN_35635" & sex=="male"
replace kid_CDR=kid_CDR*0.8 if ihme=="KEN_35636"
replace kid_CDR=kid_CDR*0.8 if ihme=="KEN_35637" & sex=="male"
replace kid_CDR=kid_CDR*0.9 if ihme=="KEN_35637" & sex=="female"
replace kid_CDR=kid_CDR*0.7 if ihme=="KEN_35638"
replace kid_CDR=kid_CDR*0.6 if ihme=="KEN_35640" & sex=="male"
replace kid_CDR=kid_CDR*0.7 if ihme=="KEN_35641"
replace kid_CDR=kid_CDR*0.7 if ihme=="KEN_35642" & sex=="male"
replace kid_CDR=kid_CDR*0.9 if ihme=="KEN_35643"
replace kid_CDR=kid_CDR*0.8 if ihme=="KEN_35644"
replace kid_CDR=kid_CDR*0.8 if ihme=="KEN_35646"
replace kid_CDR=kid_CDR*0.8 if ihme=="KEN_35647"
replace kid_CDR=kid_CDR*0.7 if ihme=="KEN_35648" & sex=="female"
replace kid_CDR=kid_CDR*0.8 if ihme=="KEN_35650"
replace kid_CDR=kid_CDR*0.6 if ihme=="KEN_35654"
replace kid_CDR=kid_CDR*0.7 if ihme=="KEN_35660" & sex=="male"
replace kid_CDR=kid_CDR*0.8 if ihme=="KEN_44794"
replace kid_CDR=kid_CDR*0.8 if ihme=="KEN_44795"
replace kid_CDR=kid_CDR*0.8 if ihme=="KEN_44796"
replace kid_CDR=kid_CDR*0.6 if ihme=="KEN_44797"
replace kid_CDR=kid_CDR*0.9 if ihme=="KEN_44800"
replace kid_CDR=kid_CDR*0.6 if ihme=="ZAF_484"

rename pred1wre p45m15

cap drop group
merge m:1 ihme sex using "$datadir/groups.dta"
replace group="1" if substr(ihme,1,4)=="IND_" | substr(ihme,1,3)=="IND" 

drop if _merge==2
drop _merge
tempfile frr
save `frr'

insheet using "FILEPATH/hiv_deaths_prop_range_update.csv",clear
rename age age_group_name
keep ihme_loc_id sex upper age_group_name
replace age="adultupper" if age=="15-59"
replace age="kidupper" if age=="Under 5"
rename sex sex
tostring sex, force replace
replace sex="male" if sex=="1"
replace sex="female" if sex=="2"
rename upper v_
duplicates drop ihme sex age,force
reshape wide v_, i(ihme sex) j(age) str
merge 1:m ihme sex using `frr'
drop if _merge==1
drop _merge
replace v_kidupper=0.8 if v_kidupper==.
replace v_adultupper=0.8 if v_adultupper==.
replace par_adult_hiv=1 if group!="1" | substr(ihme, 1,4)=="IND_" | substr(ihme, 1,3)=="IND"
replace par_kid_hiv=1 if group!="1" | substr(ihme, 1,4)=="IND_" | substr(ihme, 1,3)=="IND"


gen c_v45q15=1-exp(-45*((ln(1-obs_v45q15)/(-45))-par_adult_hiv*adult_CDR))
replace c_v45q15=1-exp(-45*((ln(1-obs_v45q15)/(-45))-par_adult_hiv*(ln(1-obs_v45q15)/-45)*v_adultupper)) if adult_CDR/(ln(1-obs_v45q15)/-45)>v_adultupper
replace c_v45q15=obs_v45q15 if c_v45q15>obs_v45q15
replace c_v45q15=0.13 if c_v45q15<0.13 & sex=="male" & group=="1"
replace c_v45q15=0.08 if c_v45q15<0.08 & sex=="female" & group=="1"




gen c_v5q0=1-exp(-5*((ln(1-obs_v5q0)/(-5))-par_kid_hiv*kid_CDR))
replace c_v5q0=1-exp(-5*((ln(1-obs_v5q0)/(-5))-par_kid_hiv*(ln(1-obs_v5q0)/-5)*v_kidupper)) if kid_CDR/(ln(1-obs_v5q0)/-5)>v_kidupper
replace c_v5q0=obs_v5q0 if c_v5q0>obs_v5q0
replace c_v5q0=0.005 if c_v5q0<0.005 & group=="1"

duplicates drop ihme sex year, force
keep ihme year c_v5q0 c_v45q15 sex region_name super_region_name ciso obs_*
gen o_logit5q0=-0.5*ln(obs_v5q0/(1-obs_v5q0))
gen o_logit45q15=-0.5*ln(obs_v45q15/(1-obs_v45q15))
expand 22
sort ihme sex year
bysort ihme sex year: gen rank=_n
gen age=(rank-2)*5
replace age=0 if rank==1
replace age=1 if rank==2
sort sex age
drop rank
sort ihme
tempfile tdata
save `tdata'
keep ihme sex year obs_v5q0 obs_v45q15 *name* ciso
duplicates drop ihme sex year, force
tempfile hivs freematched
save `hivs'

use "$datadir/match_specs.dta",clear
keep if sex == "`sex'"
merge 1:1 ihme sex year using `hivs'
replace match=20 if substr(ihme,1,3)=="ZAF"
replace match=3 if substr(ihme,1,4)=="USA_"
keep if _merge==3
drop _merge
egen isosex=group(ihme sex)
save `hivs',replace

sum isosex
global macs=r(max)
qui forvalues y=1/$macs {
    tempfile counterg2_`y' matched_`y'
    use `hivs',clear
    keep if isosex==`y'
    local gbd=region[1]
    local super=super[1]
    save `counterg2_`y''
    local ss=sex[1]
    local cc=ihme[1]
    local ci=ciso[1]
    sort ihme sex year
    keep obs_v5q0 obs_v45q15 year match
    order obs_v5q0 obs_v45q15
    local yn=_N
	replace match=5 if substr("`cc'", 1,3)=="ZAF"
    mkmat obs_v5q0 obs_v45q15 year match, matrix(years)
    tempfile matched
    forvalues t=1/`yn' {
	
	
	local early=years[`t',3]
	
	
		if substr("`cc'",1,3)=="ZAF"{
			use `smallbase',clear
			keep if substr(ihme, 1,3)=="ZAF"
			append using `base'
        }

		else if substr("`cc'",1,4)=="USA_" {
			use `usabase',clear
		}	
	   else if substr("`cc'",1,3)=="BRA" | substr("`cc'",1,3)=="ALB" | substr("`cc'",1,3)=="TKM" | substr("`cc'",1,3)=="EGY" | substr("`cc'",1,3)=="BHR"| substr("`cc'",1,3)=="TUR" |substr("`cc'",1,3)=="KAZ" |substr("`cc'",1,3)=="PAN" | substr("`cc'",1,3)=="CUB" | substr("`cc'",1,3)=="VEN"|substr("`cc'",1,3)=="KWT"|substr("`cc'",1,3)=="HKG" | substr("`cc'",1,3)=="PHL" | substr("`cc'",1,3)=="BIH" | substr("`cc'",1,3)=="BRB" | substr("`cc'",1,3)=="BMU" | substr("`cc'",1,3)=="DMA" | substr("`cc'",1,3)=="GRL" | substr("`cc'",1,3)=="CYP" | substr("`cc'",1,3)=="ISL"| substr("`cc'",1,3)=="MLT" | substr("`cc'",1,3)=="PER" | substr("`cc'",1,3)=="ARM" | substr("`cc'",1,3)=="ATG" | substr("`cc'",1,3)=="IRL" | substr("`cc'",1,3)=="LKA" | substr("`cc'",1,3)=="MDV" | substr("`cc'",1,3)=="CRI" | substr("`cc'",1,3)=="HRV" | substr("`cc'",1,3)=="MKD" | substr("`cc'",1,3)=="LTU" | substr("`cc'",1,3)=="ROU" | "`cc'"=="GUY" | "`cc'"=="LCA" | "`cc'"=="TTO" | "`cc'"=="BRA" |"`cc'"=="SWE_4940" | "`cc'"=="KOR" | "`cc'"=="MDA" | "`cc'"=="GRC"| "`cc'"=="PSE" | substr("`cc'", 1,4)=="GBR_" | substr("`cc'", 1,4)=="JPN_" | substr("`cc'",1,4)=="MEX_" | "`cc'"=="SAU" | substr("`cc'", 1,4)=="SAU_" | "`cc'"=="ECU" | "`cc'"=="GTM" | "`cc'"=="CHN_354" {
        	use `smallbase',clear
			keep if ihme=="`cc'"
			append using `base'
		}
        else {
            use `base',clear
        }
		cap drop if shock==1
        keep if sex=="`ss'"
        local i=_N+1
        set obs `i'
        replace v5q0= years[`t',1] in `i'
        replace v45q15=years[`t',2] in `i'
        local qratio=years[`t',1]/years[`t',2]
        replace logit5q0=-0.5*ln(v5q0/(1-v5q0)) in `i'
        replace logit45q15=-0.5*ln(v45q15/(1-v45q15)) in `i'
        mahascore logit5q0 logit45q15, gen(dist) refobs(`i') compute_invcovarmat
        drop if _n==`i'
        sort dist
        keep if _n<=years[`t',4] | ihme=="`cc'" | ciso=="`ci'"
 

		if substr("`cc'",1,3)=="ZAF" {
            keep if substr(ihme,1,3)=="ZAF"
        }  

		

        gen w=2
        replace w=0 if ihme=="`cc'"
		replace w=1 if ihme!="`cc'" & ciso=="`ci'"
        gen wlag=years[`t',3]-year

		if `early'>1969 {
			drop if (ihme=="`cc'" | ciso=="`ci'") & (wlag>30 | wlag<-30)
		}

        gen abslag=abs(wlag)
        sort w abslag

        
        keep if _n<=years[`t',4]
		
		

        
        gen region="other super region"
        replace region="country" if ihme=="`cc'" | ciso=="`ci'"
		
		replace region="gbd" if ihme!="`cc'" & ciso!="`ci'" & region_name=="`gbd'"

        replace region="super region" if (ihme!="`cc'" & ciso!="`ci'") & region_name!="`gbd'" & super_region=="`super'"



        gen lag=years[`t',3]-year

        keep source ihme sex year lx* region_name super_region_name lag region w
		
		duplicates drop source ihme sex year,force
		
        reshape long lx, i(source ihme sex year) j(age)
        sort source ihme sex year age
        bysort source ihme sex year: gen qx=1-lx[_n+1]/lx 
        gen logitqx=-0.5*ln(qx/(1-qx))
        merge m:1 sex lag region using `wlevel'
        keep if _merge==3
        drop _merge
 
		
:
		replace weights=weights/2 if w==1

		if substr("`cc'",1,3)=="IND" {
            keep if substr(ihme,1,3)=="IND"
			drop if year<1999 & ihme=="IND" & sex=="male"
        }  
	
	   sort ihme sex year age
         
        egen double tweight=sum(weights),by(age)
		
		gen miss=1
		replace miss=0 if qx==.
		gen double mweights=weights*miss
		egen double firstweight=sum(mweights),by(age)
		replace qx=. if firstweight < tweight
		replace logitqx=. if firstweight < tweight
		
        gen w_qx=qx*weights/tweight
        collapse (sum) w_qx, by(sex age)
        gen sqx=w_qx
        drop w_qx
        
        
        reshape wide sqx, i(sex) j(age)
        gen ref_year=years[`t',3]
        cap append using `matched_`y''
        save `matched_`y'', replace
    }
    gen ref_ihme="`cc'"
    save `matched_`y'',replace
    matrix drop years
    erase `counterg2_`y''
    noisily dis "counter-factual : `y' of $macs done"    
}

use `matched_1',clear
forvalues b=2/$macs {
    append using `matched_`b''
}

gen sq5=1-(1-sqx0)*(1-sqx1)
gen sq45=1-(1-sqx15)*(1-sqx20)*(1-sqx25)*(1-sqx30)*(1-sqx35)*(1-sqx40)*(1-sqx45)*(1-sqx50)*(1-sqx55)
reshape long sqx, i(ref_ihme ref_year sex) j(age)
gen logitsqx=-0.5*ln(sqx/(1-sqx))
gen logitsq5=-0.5*ln(sq5/(1-sq5))
gen logitsq45=-0.5*ln(sq45/(1-sq45))
rename ref_ihme ihme_loc_id
rename ref_year year
tempfile smstan
save `smstan'

if `region_id'==159 {
			use `smstan',clear
			
			egen isosex=group(ihme sex)
			cap sum isosex
			global nnis=r(max)
			keep ihme_loc_id year sex age sqx isosex
			reshape wide sqx,i(ihme sex year) j(age)
			save `smstan',replace

			tempfile smstan_GBD
			qui forvalues b=1/$nnis {
				tempfile rawstan_`b' smstan_`b'
				use `smstan',clear
				keep if isosex==`b'
				sort ihme sex year
				save `rawstan_`b''
				local ncy_`b'=_N
				keep if _n<11
				local cyear_`b'_1=year[1]
				collapse (mean) sqx*, by(ihme sex)
				gen year=`cyear_`b'_1'
				cap append using `smstan_`b''
				save `smstan_`b'', replace
				forvalues bb=2/`ncy_`b'' {
					use `rawstan_`b'',clear
					local cyear_`b'_`bb'=year[`bb']
					keep if abs(_n-`bb')<=6
					collapse (mean) sqx*, by(ihme sex)
					gen year=`cyear_`b'_`bb''
					cap append using `smstan_`b''
					save `smstan_`b'',replace
				}
				noisily dis "smoothing GBD standards: `b' of $nnis done"
				use `smstan_`b'',clear
				cap append using `smstan_GBD'
				save `smstan_GBD',replace
				cap erase `smstan_`b''
				cap erase `rawstan_`b''
			}
			use `smstan_GBD',clear
			
			gen sq5=1-(1-sqx0)*(1-sqx1)
			gen sq45=1-(1-sqx15)*(1-sqx20)*(1-sqx25)*(1-sqx30)*(1-sqx35)*(1-sqx40)*(1-sqx45)*(1-sqx50)*(1-sqx55)
			reshape long sqx, i(ihme year sex) j(age)
			gen logitsqx=-0.5*ln(sqx/(1-sqx))
			gen logitsq5=-0.5*ln(sq5/(1-sq5))
			gen logitsq45=-0.5*ln(sq45/(1-sq45))
			tempfile smstan
			cap save `smstan', replace
}




gen sim=$nn
merge m:1 sim sex age using "$datadir/modelpar_sim.dta"
keep if _merge==3
drop _merge
sort ihme sex age year
cap rename ihme ihme_loc_id
save `smstan',replace



use `hivs',clear
cap drop _merge
gen o_logit5q0=-0.5*ln(obs_v5q0/(1-obs_v5q0))
gen o_logit45q15=-0.5*ln(obs_v45q15/(1-obs_v45q15))
expand 22
sort ihme sex year
bysort ihme sex year: gen rank=_n
gen age=(rank-2)*5
replace age=0 if rank==1
replace age=1 if rank==2
cap drop rank
merge 1:1 ihme sex age year using `smstan'
keep if _merge==3
drop _merge
sort  ihme sex year age
gen plqx_o = logitsqx + sim_difflogit5*(o_logit5q0- logitsq5)+ sim_difflogit45*(o_logit45q15- logitsq45)
gen o_pqx = exp(-2*plqx_o)/(1+exp(-2*plqx_o))
keep   ihme year obs_v45q15 sex age obs_v5q0 o_pqx sqx
**** get predicted 45q15 and 5q0 for first stage
gen lx=1
sort  ihme sex year age
bysort  ihme sex year: replace lx=lx[_n-1]*(1-o_pqx[_n-1]) if _n>1
sort  ihme sex year age
bysort  ihme sex year: gen p45q15=1-lx[14]/lx[5]
sort  ihme sex year age
bysort  ihme sex year: gen p5q0=1-lx[3]
drop lx
sort  ihme sex year age
tempfile tpqx tallouts
sort  ihme sex year age
reshape wide  o_pqx sqx, i( ihme sex year) j(age)
save `tpqx'

tempfile v5not v5equal
keep if round(p5q0/obs_v5q0,0.0000000001)==1
local left=_N
if `left'>0 {
    foreach nn of numlist 0 1 5 10 {
        gen qx_adj`nn'=o_pqx`nn'
    }    
    save `v5equal'
}

use `tpqx',clear
keep if round(p5q0/obs_v5q0,0.0000000001)!=1
local left=_N
if `left'>0 {
    local cnum = 10
    local citer = 30
    local ccat=1    
    
    gen crmin = 0.0001
    gen crmax = 0.6
    
    gen cadj = .
    gen adj5q0_min = .
    gen rc_min=.
    gen diffc_min = . 
    
    while `ccat'<=`citer' {
        local cq=0
        while `cq'<=`cnum' {
            cap: gen y`cq' = .
            replace y`cq' = crmin + `cq'*((crmax-crmin)/`cnum')
            local cqq=`cq'+1
            cap: gen rc`cqq'= .
            replace rc`cqq'=y`cq'
            local cq=`cq'+1    
            cap: gen adj5q0`cqq'= .
            replace adj5q0`cqq' = 1-[1-(o_pqx0/o_pqx1)*rc`cqq']*[1-rc`cqq']
            cap: gen diffc`cqq'= . 
            replace diffc`cqq' = abs(adj5q0`cqq'-obs_v5q0) if adj5q0`cqq'!= 0
        }    
        cap: drop diffc_min
        order diffc*
        egen diffc_min = rowmin(diffc1-diffc11)
        
        replace rc_min= . 
        forvalues cqq=1/11 {
            replace rc_min=rc`cqq' if diffc`cqq'==diffc_min
        }
        
        count if rc_min == .
        if (`r(N)'!=0) {
            noisily: dis "`ccat'"
            pause
        }
        
        replace cadj=abs(crmax-crmin)/10
        replace crmax=rc_min+2*cadj
        replace crmin=rc_min-2*cadj
        replace crmin=0.0001 if crmin<0
        
        local ccat=`ccat'+1
    }
    foreach nn of numlist 0 1 5 10 {
        gen qx_adj`nn'=(o_pqx`nn'/o_pqx1)*rc_min
    }
    save `v5not'

}
noisily dis "kid scenario 1 done"

cap append using `v5equal'

tempfile after5
drop y0-y10 rc1-rc11 diffc1-diffc11 adj5q01-adj5q011 *min* *max* crmin crmax cadj
save `after5'


tempfile adultover
use `after5',clear
keep if round(p45q15/obs_v45q15,0.00000001)!=1 
local left=_N
if `left'>0 {
    tempfile adultover
    local num = 20
    local iter = 30
    local cat=1
        
    gen rmin = .0001
    gen rmax = 0.99999
    
    gen adj = .
    gen adj45q15_min = .
    gen r_min = .
    gen diff_min = .
    
    while `cat'<=`iter' {
        local q=0
        while `q'<=`num' {
            cap: gen y`q' = .
            replace y`q' = rmin + `q'*((rmax-rmin)/`num')
            local qq=`q'+1
            cap: gen r`qq' = .
            replace r`qq'=y`q'
            local q=`q'+1    
            
            cap: gen adj45q15`qq' = .
            replace adj45q15`qq' = 1-[1-(o_pqx15/o_pqx55)*r`qq']*[1-(o_pqx20/o_pqx55)*r`qq']*[1-(o_pqx25/o_pqx55)*r`qq']*[1-(o_pqx30/o_pqx55)*r`qq']*[1-(o_pqx35/o_pqx55)*r`qq']*[1-(o_pqx40/o_pqx55)*r`qq']*[1-(o_pqx45/o_pqx55)*r`qq']*[1-(o_pqx50/o_pqx55)*r`qq']*[1-r`qq']
            cap: gen diff`qq' = .
            replace diff`qq' = abs(adj45q15`qq'-obs_v45q15)
        }
        cap: drop diff_min
        order diff*
        egen diff_min=rowmin(diff1-diff21)
        replace r_min=.
        forvalues qq=1/21 {
            replace r_min = r`qq' if diff`qq'==diff_min
        }
        
        count if r_min == .
        if (`r(N)'!=0) {
            noisily: dis "`ccat'"
            pause
        }        

        replace adj=(rmax-rmin)/20
        replace rmax=r_min + 2*adj
        replace rmin=r_min - 2*adj
        replace rmin=0.0001 if rmin<0
        local cat=`cat'+1
    }      
    ** adjust 55 plus as well
    foreach nn of numlist 15(5)100 {
        gen qx_adj`nn'=(o_pqx`nn'/o_pqx55)*r_min

    }
    save `adultover'
}
noisily dis "old guys scenario 1 done"

use `after5',clear
keep if round(p45q15/obs_v45q15,0.00000001) == 1 
local left=_N
if `left'>0 {
    tempfile adultequal
    gen r_min = 1
    
    foreach nn of numlist 15(5)100 {
        gen qx_adj`nn'=o_pqx`nn'
    }    
    save `adultequal'
}

cap append using `adultover'
drop y0-y10 r1-r21 diff1-diff21 adj45q151-adj45q1521  rmin rmax adj
reshape long o_pqx sqx qx_adj, i(ihme sex year) j(age)    
keep   ihme year obs_v45q15 sex obs_v5q0 age qx_adj o_pqx sqx
tempfile tallouts
save `tallouts'


use "$datadir/par_age_110plus_mx.dta",clear
keep if sex == "`sex'"
local mxf=parlnmx[1]

tempfile struc
use `tallouts',clear
keep  ihme sex year
duplicates drop  ihme sex year, force
expand 24
bysort  ihme sex year: gen rank=_n
gen age=0 if rank==1
replace age=1 if rank==2
replace age=(rank-2)*5 if rank>2
drop rank
sort  ihme sex year age
save `struc'
use `tallouts',clear
sort  ihme sex year age
merge  ihme sex year age using `struc'
drop _merge
gen logitqx=ln(qx_adj/(1-qx_adj))
sort sex age  ihme year 
merge m:1 sex age using "$datadir/par_age_85plus_qx_alter.dta"
drop if _merge==2
drop _merge
sort  ihme sex year age
bysort  ihme sex year:gen prediff=par_85cons+par_logitqx80*logitqx[18]+par_vage
sort  ihme sex year age
bysort  ihme sex year:gen pqx=qx_adj if _n==18
sort  ihme sex year age
bysort  ihme sex year:replace pqx=exp(ln(pqx[_n-1]/(1-pqx[_n-1]))+prediff[_n-1])/(1+exp(ln(pqx[_n-1]/(1-pqx[_n-1]))+prediff[_n-1])) if _n>18 & _n<24
replace qx_adj=pqx if age>80 & qx_adj==.
replace qx_adj=1 if age==110
keep  ihme sex year age sqx o_pqx obs_v5q0 obs_v45q15 qx_adj
gen lx=1


replace qx_adj=0.99 if qx_adj>1 & qx_adj!=.


sort  ihme sex year age
bysort  ihme sex year: replace lx=lx[_n-1]*(1-qx_adj[_n-1]) if _n>1
gen nn=5
replace nn=1 if age==0
replace nn=4 if age==1
gen dx=.
sort  ihme sex year age
bysort  ihme sex year: replace dx=lx-lx[_n+1] 
replace dx=lx if age==110
gen ax=.
sort  ihme sex year age
bysort  ihme sex year: replace ax=((-5/24)*dx[_n-1]+2.5*dx+(5/24)*dx[_n+1])/dx if _n>3 & _n<23
replace ax=2.5 if ax<0 & age>5 & age<110
replace ax=2.5 if ax>5 & age>5 & age<110


sort sex age
merge m:1 sex age using "$datadir/ax_par.dta"
drop if _m == 2
drop _m
gen qx_square=qx_adj^2
replace ax=par_qx*qx_adj+par_sqx*qx_square+par_con if age>75 & par_con!=.
drop qx_square par_qx par_sqx par_con

gen k1=.
sort  ihme sex year age
bysort  ihme sex year: replace k1=1.352 if qx[1]>0.01 & sex[1]=="male"
sort  ihme sex year age
bysort  ihme sex year: replace k1=1.361 if qx[1]>0.01 & sex[1]=="female"
sort  ihme sex year age
bysort  ihme sex year: replace k1=1.653-3.013*qx[1] if qx[1]<=0.01 & sex[1]=="male"
sort  ihme sex year age
bysort  ihme sex year: replace k1=1.524-1.627*qx[1] if qx[1]<=0.01 & sex[1]=="female"
gen mx=.
gen double nLx=.
sort  ihme sex year age
bysort  ihme sex year: replace mx=qx/(nn-(nn-ax)*qx) if _n>3 & _n<24
sort  ihme sex year age
bysort  ihme sex year: replace nLx = nn*lx[_n+1]+ax*dx if _n>3 & _n<24
sort  ihme sex year age
bysort  ihme sex year: replace ax=1 if _n>17 & _n<23 & (mx>=1|mx<=0) 
sort  ihme sex year age
bysort  ihme sex year: replace mx=qx/(nn-(nn-ax)*qx) if _n>17 & _n<24
sort  ihme sex year age
bysort  ihme sex year: replace nLx = nn*lx[_n+1]+ax*dx if _n>17 & _n<24

*** get mx in the open age interval 110+
sort  ihme sex year age
bysort  ihme sex year: gen lnmx105=ln(mx[23])

replace mx=exp(`mxf'*lnmx105) if age==110

cap drop lnmx105

replace nLx=lx/mx if age==110
sort  ihme sex year age
bysort  ihme sex year:replace nLx=(0.05+3*qx_adj[1])+(0.95-3*qx_adj[1])*lx[2] if _n==1
sort  ihme sex year age
bysort  ihme sex year: replace nLx=0.35+0.65*lx[2] if _n==1 & qx_adj[1]>0.1
sort  ihme sex year age
bysort  ihme sex year: replace nLx=(k1*lx[2]+(4-k1)*lx[3]) if _n==2
sort  ihme sex year age
bysort  ihme sex year: replace nLx=2.5*(lx[3]+lx[4]) if _n==3
sort  ihme sex year age
bysort  ihme sex year: replace mx=dx/nLx if _n<4
sort  ihme sex year age
bysort  ihme sex year: replace ax=(qx+nn*mx*qx-nn*mx)/(mx*qx) if _n<4

        cap replace ax=0.2 if age==0 & sex=="male" & ax<=0
        cap replace ax=0.2 if age==0 & sex=="male" & ax>=1
        cap replace ax=0.15 if age==0 & sex=="female" & ax<=0
        cap replace ax=0.15 if age==0 & sex=="female" & ax>=1    

        cap replace ax=1.35 if age==1 & sex=="male" & ax<=0
        cap replace ax=1.35 if age==1 & sex=="male" & ax>=4
        cap replace ax=1.36 if age==1 & sex=="female" & ax<=0
        cap replace ax=1.36 if age==1 & sex=="female" & ax>=4    

        cap replace ax=2.5 if age==5 & sex=="male" & ax<=0
        cap replace ax=2.5 if age==5 & sex=="male" & ax>=5
        cap replace ax=2.5 if age==5 & sex=="female" & ax<=0
        cap replace ax=2.5 if age==5 & sex=="female" & ax>=5    
        
egen lid=group( ihme sex year)
cap sum lid
local nlid=r(max)
gen double Tx=nLx
gsort ihme sex year -age
bysort ihme sex year: replace Tx=Tx[_n-1]+nLx if _n>1
gen double ex=Tx/lx
replace ax=ex if age==110
save `tallouts',replace
tempfile newtallouts
save `newtallouts'

*** keep ax values for later use
keep ihme sex year age ax
rename ax ax_hivfree
tempfile axs
save `axs'

use `newtallouts',clear
cap lab drop agelbl
keep  ihme sex year age obs_v5q0 obs_v45q15 qx_adj sqx o_pqx
rename qx_adj o_pqx_adj
tempfile newall
sort  ihme sex year age
save `newall'


tempfile simouts
    local h=`secsim'

use `tdata',clear
gen secsim=`h'
duplicates drop sex ihme_loc_id region_name super_region_name ciso year secsim,force
renpfix obs_v v
	
	cap replace v5q0=0.90 if v5q0>=1

	merge m:1 ihme sex using "$datadir/groups.dta"
	replace group="1" if substr(ihme, 1,3)=="IND" | substr(ihme, 1, 3)=="KEN" 
	replace group="2" if substr(ihme, 1,3)=="GBR" | substr(ihme, 1, 3)=="IDN"
	drop if _merge==2
	drop _merge
	
	replace v45q15=0.13 if sex=="male" & group=="1" & v45q15<0.13
	replace v45q15=0.08 if sex=="female" & group=="1" & v45q15<0.08
	replace v5q0=0.005 if group=="1" & v5q0<0.005

	keep if sex == "`sex'"
	merge m:1 ihme_loc_id using `regs'
	keep if _m == 3
	drop _m
    keep ihme sex year c_v45q15 c_v5q0
    merge 1:m ihme sex year using `newall'
    keep if _merge==3
    drop _merge
	renpfix obs_v v
    gen sim=$nn
    gen secsim=`h'

    merge 1:1 ihme sex year age using `hivrrs'
	keep if sex == "`sex'"
    drop if _merge==2
    drop _merge
    
    replace hivrr=1 if age==40

	replace hivrr=1 if age<80 & hivrr==.


	

    sort secsim ihme sex year age
    bysort secsim ihme sex year: replace v5q0=v5q0[1] if v5q0==.
    bysort secsim ihme sex year: replace c_v5q0=c_v5q0[1] if c_v5q0==.
    bysort secsim ihme sex year: replace v45q15=v45q15[1] if v45q15==.
    bysort secsim ihme sex year: replace c_v45q15=c_v45q15[1] if c_v45q15==.

    
    merge m:1 ihme sex year age using `axs',nogen
    gen nn=5
    replace nn=1 if age==0
    replace nn=4 if age==1
    gen mx_hiv= o_pqx_adj/(nn-(nn-ax)*o_pqx_adj)
    gen amxdiff=abs([ln(1-v45q15)-ln(1-c_v45q15)]/-45)
    gen kmxdiff=abs([ln(1-v5q0)-ln(1-c_v5q0)]/-5)
        
    keep   ihme year v45q15 sex age v5q0 c_v* mx_hiv ax *mxdiff secsim hivrr  nn
    tempfile secpqx secouts
    cap lab drop agelbl
    cap lab drop agelabels
    save `secpqx'

    keep if kmxdiff<=0.00001
    local left=_N
    if `left'>0 {
        tempfile kzerohiv
        gen mx=mx_hiv
        keep if age<15
        save `kzerohiv'
    }

    tempfile sec5over
    use `secpqx',clear
    keep if kmxdiff>0.00001
    local cnum = 10
    local citer = 50
    local ccat=1    

	
    gen crmin = 0.002
	gen rmax=mx_hiv/(hivrr*kmxdiff)
	sort ihme sex year rmax
	bysort ihme sex year: gen crmax=rmax[1]
	gen hardmax=crmax
	

    gen cadj = .
    gen adj5q0_min = .
    gen rc_min=.
    gen diffc_min = . 
    while `ccat'<=`citer' {
        local cq=0
        while `cq'<=`cnum' {
            cap: gen y`cq' = .
            replace y`cq' = crmin + `cq'*((crmax-crmin)/`cnum')
            local cqq=`cq'+1
            cap: gen rc`cqq'= .
            replace rc`cqq'=y`cq'
            local cq=`cq'+1    
            cap: gen adj5q0`cqq'= .
            sort ihme sex year age
            gen qx`cqq'=nn*(mx_hiv-hivrr*kmxdiff*rc`cqq')/[1+(nn-ax)*(mx_hiv-hivrr*kmxdiff*rc`cqq')]
            bysort ihme sex year: replace adj5q0`cqq' = 1-(1-qx`cqq'[1])*(1-qx`cqq'[2])
            cap: gen diffc`cqq'= . 
            replace diffc`cqq' = abs(adj5q0`cqq'-c_v5q0) if adj5q0`cqq'!= 0
        }    
        cap drop qx*
        cap: drop diffc_min
        order diffc*
        egen diffc_min = rowmin(diffc1-diffc11)
        replace rc_min= . 
        forvalues cqq=1/11 {
                replace rc_min=rc`cqq' if diffc`cqq'==diffc_min
        }
        
        count if rc_min == .
        if (`r(N)'!=0) {
            noisily: dis "`ccat'"
            pause
        }
            
        replace cadj=abs(crmax-crmin)/10
        replace crmax=rc_min+2*cadj
        replace crmin=rc_min-2*cadj
        replace crmin=0.000001 if crmin<0
		
		replace crmax=hardmax*0.99 if crmax>hardmax
		
        local ccat=`ccat'+1
    }
    gen mx=mx_hiv-hivrr*rc_min*kmxdiff
	replace mx=mx_hiv*0.1 if mx<0
    keep   ihme year v45q15 sex age v5q0 c_v* mx_hiv ax *mxdiff secsim hivrr nn mx
    keep if age<15
    save `sec5over'                
    
    
    use `secpqx',clear
    keep if amxdiff<=0.00001
    local left=_N
    if `left'>0 {
        tempfile azerohiv
        gen mx=mx_hiv
        keep if age>=15
        save `azerohiv'
    }

    tempfile sec45
    use `secpqx',clear
    keep if amxdiff>0.00001
    local cnum = 100
    local citer = 50
    local ccat=1    
        
    gen crmin = 0.002
	gen rmax=mx_hiv/(hivrr*amxdiff)
	sort ihme sex year rmax
	bysort ihme sex year: gen crmax=rmax[1]
	gen hardmax=crmax
	
	
	
    gen cadj = .
    gen adj45q15_min = .
    gen rc_min=.
    gen diffc_min = . 
    while `ccat'<=`citer' {
        local cq=0
        while `cq'<=`cnum' {
            cap: gen y`cq' = .
            replace y`cq' = crmin + `cq'*((crmax-crmin)/`cnum')
            local cqq=`cq'+1
            cap: gen rc`cqq'= .
            replace rc`cqq'=y`cq'
            local cq=`cq'+1    
            cap: gen adj45q15`cqq'= .
            sort ihme sex year age
            gen qx`cqq'=nn*(mx_hiv-hivrr*amxdiff*rc`cqq')/[1+(nn-ax)*(mx_hiv-hivrr*amxdiff*rc`cqq')]
			
            bysort ihme sex year: replace adj45q15`cqq' = 1-(1-qx`cqq'[5])*(1-qx`cqq'[6])*(1-qx`cqq'[7])*(1-qx`cqq'[8])*(1-qx`cqq'[9])*(1-qx`cqq'[10])*(1-qx`cqq'[11])*(1-qx`cqq'[12])*(1-qx`cqq'[13])
            cap: gen diffc`cqq'= . 
            replace diffc`cqq' = abs(adj45q15`cqq'-c_v45q15) if adj45q15`cqq'!= 0
        }    
        cap drop qx*
        cap: drop diffc_min
        order diffc*
        egen diffc_min = rowmin(diffc*)
        replace rc_min= . 
        forvalues cqq=1/101 {
                replace rc_min=rc`cqq' if diffc`cqq'==diffc_min
        }
        
        count if rc_min == .
        if (`r(N)'!=0) {
            noisily: dis "`ccat'"
            pause
        }
            
        replace cadj=abs(crmax-crmin)/100
        replace crmax=rc_min+2*cadj
        replace crmin=rc_min-2*cadj
        replace crmin=0.000001 if crmin<0
		
		replace crmax=hardmax*0.99 if crmax>hardmax
		
        local ccat=`ccat'+1
    }
    gen mx=mx_hiv-hivrr*rc_min*amxdiff
	replace mx=mx_hiv*0.1 if mx<0
    keep   ihme year v45q15 sex age v5q0 c_v* mx_hiv ax *mxdiff secsim hivrr nn mx
    keep if age>=15
    cap append using `kzerohiv'
    cap append using `azerohiv'
    cap append using `sec5over'
    sort ihme sex year age
    gen qx_adj=nn*mx/(1+(nn-ax)*mx)	
	replace qx_adj=0.1*nn*mx_hiv/(1+(nn-ax)*mx_hiv) if qx_adj<0
    keep   ihme year c_v45q15 sex c_v5q0 age qx_adj secsim nn mx
	rename qx_adj c_qx_adj
	rename mx mx_nohiv
    tempfile secouts                    
    save `secouts'


    gen lx=1
    sort  ihme sex year age
    bysort  ihme sex year: replace lx=lx[_n-1]*(1-c_qx_adj[_n-1]) if _n>1
    gen dx=.
    sort  ihme sex year age
    bysort  ihme sex year: replace dx=lx-lx[_n+1] 
    replace dx=lx if age==110
    merge 1:1 ihme sex year age using `axs'
    drop _merge
    gen nLx=.
    sort  ihme sex year age
    bysort  ihme sex year: replace nLx = nn*lx[_n+1]+ax*dx
    replace nLx=lx/mx if age==105
    replace nLx=lx/mx if age==110
    
    egen lid=group( ihme sex year)
    save `secouts',replace
    tempfile newsecouts
    keep ihme sex year age nLx
    egen double Tx0=sum(nLx), by(ihme sex year)
    reshape wide nLx, i(ihme sex year) j(age)
    gen double Tx1=Tx0-nLx0
    gen double Tx5=Tx1-nLx1
    forvalues j=10(5)110 {
        local jj=`j'-5
        gen double Tx`j'=Tx`jj'-nLx`jj'
    }
    reshape long nLx Tx, i(ihme sex year) j(age)
    drop nLx
    merge 1:1 ihme sex year age using `secouts'
    drop _merge
    gen double ex=Tx/lx
    replace secsim=`h'
    cap append using `simouts'
    save `simouts',replace
    local files="secpqx azerohiv kzerohiv sec5over secouts"
    foreach f of local files {
        cap erase ``f''
    }
	
use `simouts',clear
rename c_qx_adj qx_adj
rename mx_nohiv mx
save `simouts',replace

use `newtallouts',clear
renpfix obs_v v
save `newtallouts',replace

compress
assert qx != .
assert qx > 0
save "FILEPATH/zaf_reverse_LT_sim_withhiv_withsim_`region_id'_`sex'_$nn.dta",replace

use `simouts',clear
compress
assert qx != .
assert qx > 0
save "FILEPATH/ZAF_reverse_LT_sim_nohiv_`region_id'_`sex'_$nn.dta",replace

import delimited  "FILEPATH/estimate_locs.csv", clear
keep if region_id == `region_id' 
levelsof location_id, local(locs_for_pop)
keep ihme_loc_id location_id
tempfile ihme_loc_for_pop
save `ihme_loc_for_pop'

import delimited "FILEPATH/agemap.csv", clear
tempfile agemap
save `agemap'

import delimited "FILEPATH/population.csv", clear
rename population pop
rename year_id year
gen sex = "male" if sex_id==1
replace sex = "female" if sex_id==2
replace sex = "both" if sex_id==3
keep if sex == "`sex'"

merge m:1 location_id using `ihme_loc_for_pop'
assert _m!=2
keep if _m==3
assert ihme_loc_id != ""

drop _m

merge m:1 age_group_id using `agemap'
keep if _m==3
drop _m

drop age_group_id

gen age=0 if age_group=="<1 year"
replace age =1 if age_group=="1 to 4"
forvalues j=5(5)90 {
	local jj=`j'+4
	replace age=`j' if age_group=="`j' to `jj'"
}
replace age=95 if age_group=="95 plus"
drop if age==.
keep ihme sex year age pop

tempfile pop100
save `pop100'

use "$datadir/par_age_95plus_mx.dta",clear
keep if sex == "`sex'"
local mxold=parmx[1]

use `newtallouts',clear
replace mx=lx/Tx if age==95
drop if age>=100
keep ihme sex year age mx
rename mx hiv_mx
tempfile combined
save `combined'

use `simouts',clear
replace mx=lx/Tx if age==95
drop if age>=100
keep secsim ihme sex year age mx
rename mx nohiv_mx
merge m:1 ihme sex year age using `combined'
drop _merge


sort ihme secsim sex year age
bysort ihme secsim sex year: replace hiv_mx=hiv_mx[20]*`mxold' if _n==21
bysort ihme secsim sex year: replace nohiv_mx=nohiv_mx[20]*`mxold' if _n==21


merge m:1 ihme sex year age using `pop100'
keep if _merge==3
drop _merge
gen envelope_nohiv=nohiv_mx*pop
gen envelope_withhiv=hiv_mx*pop
collapse (sum) envelope* pop  hiv_mx nohiv_mx, by(secsim ihme sex year age)
save `combined',replace

cap drop obsmx

keep ihme sex year age secsim envelope_withhiv
gen id="_"+string($nn)+"_"+string(secsim)
drop secsim
reshape wide  envelope_withhiv, i(ihme sex year age) j(id) str
cap lab drop agelbl
tempfile almost_env
save `almost_env', replace

renpfix enve renve
keep if _n==1
reshape long r, i(ihme) j(vars) str
levelsof vars, local(names)

local w=$nn

use "FILEPATH/noshock_u5_deaths_sim_`w'.dta",clear
keep if sex == "`sex'"
merge m:1 ihme_loc_id using `regs'
keep if _m == 3
drop _m
replace age="1" if age=="2"
replace age="1" if age=="3"
replace age="1" if age=="4"
collapse (sum) deaths, by(ihme sex year age)
tempfile u5s
save `u5s'
drop if age=="1"
collapse (sum) deaths, by(ihme sex year)
gen age="0"
append using `u5s'
foreach v of local names {
	gen `v'=deaths
}
drop deaths
save `u5s',replace
use `almost_env', clear
drop if age<5
tostring age, force replace
append using `u5s'
save "FILEPATH/zaf_reverse_envelope_`region_id'_`sex'_sim_$nn.dta",replace

exit,clear
