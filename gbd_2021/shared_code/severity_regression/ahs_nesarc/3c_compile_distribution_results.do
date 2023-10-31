

cap restore, not

clear all
set mem 300m
set maxvar 30000
set more off


// rename variables 
foreach survey of global surveys {

	insheet using "./FILEPATH.csv", comma clear
	/*
	foreach var of varlist  dist_mean dist_lci dist_uci {
		rename `var' `survey'`var'
	}
	*/
	drop code
	tempfile `survey'
	save ``survey'', replace
	
}

use `meps', clear
rename (dist_mean dist_lci dist_uci) (mepsdist_mean mepsdist_lci mepsdist_uci)
local list ahs1mo ahs12mo nesarc
foreach survey of local list {
	merge 1:1 cause severity using ``survey'',nogen
}
drop ahs1mo_dw ahs12mo_dw nesarc_dw
order  cause    severity hhseqid js_seq meps* ahs1* ahs12* nesar*

outsheet using "./FILEPATH${date}.csv", comma replace


// compile a mean distribution - use AHS12month only
preserve

local list meps ahs12mo nesarc
foreach survey of local list {
	use  "./FILEPATH.dta", clear
	keep cause severity dist*
	forvalues i = 1/1000 {
		rename dist`i' `survey'dist`i'

	}
	tempfile `survey'
	save ``survey'',replace
}
clear
gen cause = ""
gen severity = .
foreach survey of local list {
	merge 1:1 cause severity using ``survey'',nogen
}
keep cause severity meps* ahs* nesarc*

// need to know how many surveys used each variable to get mean
gen surveys = 0
replace surveys = surveys + 1 if  mepsdist1 != .
replace surveys = surveys + 1 if  ahs12modist1 != .
replace surveys = surveys + 1 if  nesarcdist1 != .

// gen mean distribution
forvalues i = 1/1000 {
	foreach survey of local list {
		replace `survey'dist`i' = 0 if `survey'dist`i' == .
	}
	gen dist`i' = (mepsdist`i' + nesarcdist`i' + ahs12modist`i')/(surveys)
}

keep cause severity dist*

egen MEAN_meandist = rowmean(dist*)
egen MEAN_lcidist  = rowpctile(dist*), p(2.5)
egen MEAN_ucidist  = rowpctile(dist*), p(97.5)

order cause severity MEAN* dist*
save "./FILEPATH${date}.dta", replace

keep cause severity MEAN*

tempfile mean
save `mean', replace
restore

merge 1:1 cause severity using `mean', nogen

order cause  severity hhseqid js_seq MEAN* meps* ahs1* ahs12* nesar*

rename MEAN_meandist MEANdist_mean
// get other half of distirbution for ones with only asymptomatic
bys cause: egen max = max(severity)
replace hhseqid = 999 if max == 0 // just for display reasons, this is already the case
preserve
keep if max == 0
keep cause
tempfile asyms
save `asyms', replace
insheet using "$gbd_dws", comma clear // get mean weight
egen js_seq_dw = rowmean(meandw*)
keep hhseqid js_seq_dw
drop if hhseqid == .
tempfile dws
save `dws', replace
insheet using "./FILEPATH${causename}.csv", comma clear
merge m:1 hhseqid using `dws'
keep cause severity hhseqid js_seq_dw
merge m:1 cause using `asyms', keep(3) nogen
gen symmarker = 1
tempfile syms
save `syms', replace
restore
append using `syms'
bys cause: carryforward max, replace
sort cause severity
foreach var of varlist MEANdist_mean-nesarcdist_uci {
	bys cause: carryforward `var', replace
	replace `var' = 1-`var' if symmarker== 1 // inverse distribution for asym to sym 
	replace `var' = . if max == 0 & hhseqid == 999 & symmarker== 1 // 
}



// get mean weights - sumproduct mean distributions over DWs
local list meps ahs1mo ahs12mo nesarc MEAN

levelsof cause, local(causes)
foreach dist of local list {
	gen `dist'weight = js_seq_dw*`dist'dist_mean
	bys cause: egen `dist'_finalweight = total(`dist'weight )
	drop `dist'weight
	order cause `dist'_finalweight
	replace `dist'_finalweight = . if `dist'_finalweight == 0
}
		
// bring in sequalae titles
preserve
insheet using "./FILEPATH.csv", comma clear
keep hhseqid hs_name

set obs 223
replace hs_name = "Back pain, acute, without leg pain" in 218
replace hhseqid = 44 in 218
replace hs_name = "Back pain, acute, with leg pain" in 219
replace hhseqid = 45 in 219
replace hs_name = "Back pain, chronic, without leg pain"  in 220
replace hhseqid =46 in 220
replace hs_name = "Back pain, chronic, with leg pain"  in 221
replace hhseqid = 47 in 221
replace hs_name = "Other MSK, mild – upper limbs"  in 222
replace hhseqid = 226 in 222

replace hhseqid = 999 in 223
replace hs_name = "UNSYMPTOMATIC" in 223
tempfile seqnames
save `seqnames', replace
restore

merge m:1 hhseqid using `seqnames', keep(3) nogen
order  cause MEAN_finalweight nesarc_finalweight ahs12mo_finalweight ahs1mo_finalweight meps_finalweight severity hhseqid hs_name


// names proper to GBD -- based on lancet papers
replace cause = "Other musculoskeletal disorders" if regex(cause,"MSK")
replace cause = "Amphetamine use disorders" if regex(cause,"Amphetamine Use")
replace cause = "Alcohol use disorders" if regex(cause,"Alcohol Use Disorder")
replace cause = "Ischemic heart disease" if regex(cause,"Angina Ischemic heart disease") 
replace cause = "Anxiety disorders" if regex(cause,"Anxiety")
replace cause = "Atrial fibrillation" if regex(cause,"Atrial Fibrillation")
replace cause = "Benign prostatic hyperplasia" if regex(cause,"BPH")
replace cause = "Back pain (with leg)" if regex(cause,"Back P w Leg")
replace cause = "Back pain (no leg)" if regex(cause,"Back Pain")
replace cause = "Cannabis use disorders" if regex(cause,"Cannibis Use")
replace cause = "Cocaine use disorders" if regex(cause,"Cocaine Use")
replace cause = "Chagas disease - Digestive Symptoms (Proxy)" if regex(cause,"Colon Rectal Cancer")
replace cause = "Major depressive disorder" if regex(cause,"Depression")
replace cause = "Dysthymia" if regex(cause,"Dysthymia")
replace cause = "Endometriosis" if regex(cause,"Endometriosis")
replace cause = "Uterine fibroids" if regex(cause,"Fibroids")
replace cause = "Genital prolapse" if regex(cause,"Genital Prolapse")
replace cause = "Tubulointerstitial nephritis, pyelonephritis, and urinary tract infections" if regex(cause,"Infections of kidney")
replace cause = "Idiopathic proctocolitis, Other non-infective gastro-enteritis and colitis, Regional enteritis" if regex(cause,"Idiopathic proctocolitis, Other non-infective gastro-enteritis and colitis, Regional enteritis") // not sure what cause this is ask theo
replace cause = "Ischemic heart disease" if regex(cause,"Ischemic heart disease heart failure")
replace cause = "Ischemic stroke" if regex(cause,"Ischemic stroke long term")
replace cause = "Neck pain" if regex(cause,"Neck Pain")
replace cause = "Opioid use disorders" if regex(cause,"Opiod Use")
replace cause = "Premenstrual syndrome" if regex(cause,"Premenstrial Syndrome")
replace cause = "Acne vulgaris" if regex(cause,"SKIN_Acne")
replace cause = "Alopecia areata" if regex(cause,"SKIN_Alopecia")
replace cause = "Fungal skin diseases" if regex(cause,"SKIN_Dermatophytosis_Fungal")
replace cause = "Pruritus" if regex(cause,"SKIN_Pruritus")
replace cause = "Psoriasis" if regex(cause,"SKIN_Psoriasis")
replace cause = "Urticaria" if regex(cause,"SKIN_Urticaria")
replace cause = "Eczema" if regex(cause,"SKIN_dermatitis")
replace cause = "Viral skin diseases" if regex(cause,"SKIN_Viral_Wart")
replace cause = "Abscess, impetigo, and other bacterial skin diseases" if regex(cause,"SKIN_bacterial")
replace cause = "Other skin and subcutaneous diseases" if regex(cause,"SKIN_residual")
replace cause = "Decubitus ulcer" if regex(cause,"SKIN_ulcer")
replace cause = "Asthma" if regex(cause,"asthma")
replace cause = "Chronic obstructive pulmonary disease" if regex(cause,"COPD")
replace cause = "Other chronic respiratory diseases" if regex(cause,"other interstitial lung diseases")
replace cause = "Pneumoconiosis" if regex(cause,"pneumoconiosis")


// drop if unused
drop if cause == "Cardiomyapathy"
drop if cause == "Atrial fibrillation"
drop if cause == "Cystitis"
drop if cause == "Other Gynecological"

sort cause severity

outsheet using "./FILEPATH${date}.csv", comma replace
outsheet using "./FILEPATH.csv", comma replace
