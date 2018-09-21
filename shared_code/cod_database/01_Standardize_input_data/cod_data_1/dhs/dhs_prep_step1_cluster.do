**********************************************************************************
**Purpose: Reformats the DHS sibling histories so that each observation is a death in a woman of reproductive age (15-49).
**********************************************************************************

clear all
set more off
cap restore, not
pause off

if c(os) == "Windows" {
	global prefix "J:"
}
if c(os) == "Unix" {
	global prefix "/home/j"
}

** Working directories
global raw_dir "$prefix/DATA/"
global log_dir "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/logs"
global data_dir "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/data"
global temp_error_dir "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/temp"

** set locals from arguments
set obs 1
gen arg = "$arg"
split arg, parse(-) generate(a) destring
global newflag              = a1
global filename				= a2																			
global st_country			= a3		
global st_year				= a4
global st_dir               = a5

** start log
cap log close
cd "$log_dir"
local f = subinstr("$filename",".DTA","",.)
global filename = subinstr("$filename",".DTA","",.)
log using "dp1_`f'.smcl"
    
di in red "newflag: $newflag"
di in red "filename: $filename"
di in red "st_country: $st_country"
di in red "st_year: $st_year"
di in red "st_dir: $st_dir"

local year = substr("$filename",19,4)
if "$filename"=="CRUDE_INT_BMMS_BGD_2001-2001_IND_v05152008" {
	local year = "2001"
}

** NOTE: If you just need to ADD new files this will just add new datasets, not redo old ones
if ($newflag == 1) {
capture confirm file `"$data_dir/bycountry/$st_country `year'.DTA"'
** " 	
dis _rc
if (_rc != 0) local rc = 1
 }
else {
	local rc = 1
}
pause
if `rc'~=0 {	
	** Some modules have custom variables. Clean up so all variables are consistent 
	** BMMS from BanglUSER 
	if "$filename" == "CRUDE_INT_BMMS_BGD_2001-2001_IND_v05152008" {

		use qunion qcluster qnumber qline qweight qintm qinty qintc q105m q105y q105c q106 q201a q204* q205* q206* q207* q208* q209* q210* q211* q212* using "$prefix/Crude/Survey - Interview/BMMS/$filename.DTA", clear
		
		rename qweight v005
		rename qintm v006
		rename qinty v007
		rename qintc v008
		rename q105m v009
		rename q105y v010
		rename q105c v011
		rename q106 v012
		renpfix q205 mm1
		renpfix q206 mm2
		renpfix q207c mm4
		renpfix q207 mm3
		renpfix q208c mm8
		renpfix q208 mm6
		renpfix q209 mm7
		renpfix q204 mmidx
		renpfix q210_0 q210_
		renpfix q211_0 q211_
		renpfix q212_0 q212_
		
		gen caseid=string(qunion)+string(qcluster)+string(qnumber)+string(qline)
		bysort caseid: gen n=_n
		replace caseid=caseid+string(n)
		gen v013=1 if v012>=15 & v012<=19
		replace v013=2 if v012>=20 & v012<=24
		replace v013=3 if v012>=25 & v012<=29
		replace v013=4 if v012>=30 & v012<=34
		replace v013=5 if v012>=35 & v012<=39
		replace v013=6 if v012>=40 & v012<=44
		replace v013=7 if v012>=45 & v012<=49
		gen v014=.
		gen v000="BGD"
		forvalues i=1/9 {
			gen mm15_0`i'=.
			replace mm2_0`i'=0 if mm2_0`i'==2
			gen mm9_0`i' = 2 if (q210_`i'==1 | q211_`i'==1 | q212_`i'==1)
			}
		forvalues i=10/20 {
			gen mm15_`i'=.
			replace mm2_`i'=0 if mm2_`i'==2
			gen mm9_`i' = 2 if q210_`i'==1 | q211_`i'==1 | q212_`i'==1
			}
		rename q201a mmc1
		qui count if mmc1 ==.
		local nosib = r(N)
		display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"
		drop if mmc1 == . 								
		* Dropping females who did not respond to sibling history module
		drop mmc1 qunion qcluster qnumber qline q210* q211* q212*
        
	}
    
    else if "$filename" == "BGD_SP_DHS4_2001_WN_Y2008M11D03" {    
        ** BanglUSER 2001 - format custom variable names 
        use qunion qcluster qnumber qline qweight qintm qinty qintc q105m q105y q105c q106 q201a q204* q205* q206* q207* q208* q209* q210* q211* q212* using "$raw_dir/$st_dir/$st_country//$st_year/$filename.DTA", clear
		rename qweight v005
		rename qintm v006
		rename qinty v007
		rename qintc v008
		rename q105m v009
		rename q105y v010
		rename q105c v011
		rename q106 v012
        renpfix q205 mm1
		renpfix q206 mm2
		renpfix q207c mm4
		renpfix q207 mm3
		renpfix q208c mm8
		renpfix q208 mm6
		renpfix q209 mm7
		renpfix q204 mmidx
		renpfix q210_0 q210_
		renpfix q211_0 q211_
		renpfix q212_0 q212_
		
        gen caseid=string(qunion)+string(qcluster)+string(qnumber)+string(qline)
		bysort caseid: gen n=_n
		replace caseid=caseid+string(n)
		gen v013=1 if v012>=15 & v012<=19
		replace v013=2 if v012>=20 & v012<=24
		replace v013=3 if v012>=25 & v012<=29
		replace v013=4 if v012>=30 & v012<=34
		replace v013=5 if v012>=35 & v012<=39
		replace v013=6 if v012>=40 & v012<=44
		replace v013=7 if v012>=45 & v012<=49
		gen v014=.
		gen v000="BGD"
		forvalues i=1/9 {
			gen mm15_0`i'=.
			replace mm2_0`i'=0 if mm2_0`i'==2
			gen mm9_0`i' = 2 if (q210_`i'==1 | q211_`i'==1 | q212_`i'==1)
			}
		forvalues i=10/20 {
			gen mm15_`i'=.
			replace mm2_`i'=0 if mm2_`i'==2
			gen mm9_`i' = 2 if q210_`i'==1 | q211_`i'==1 | q212_`i'==1
			}
		rename q201a mmc1
		qui count if mmc1 ==.
		local nosib = r(N)
		display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"
		drop if mmc1 == . 								
		* Dropping females who did not respond to sibling history module
		drop mmc1 qunion qcluster qnumber qline q210* q211* q212*
        
    }

	else if "$filename" == "BRA_DHS3_1996_WN_Y2008M09D23" {		
        ** Brazil 1996 - include sstate code for subdividing Brazil into North and South
		use sstate caseid v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* using "$raw_dir/$st_dir/$st_country//$st_year/$filename.DTA", clear 
		capture keep mmc1 sstate caseid v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mmidx* mm1_* mm2_* mm3* mm4*  mm6* mm7* mm8* mm9* mm15*
		qui count if mmc1 ==.
		local nosib = r(N)
		display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"
        ** Dropping females who did not respond to sibling history module 
		drop if mmc1 == . 								
		drop mmc1
	}
	
	else if "$filename" == "NPL_DHS3_1996_WN_Y2008M09D23" {						
        ** Nepal 1996 - need to fix dates 
		use caseid v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* using "$raw_dir/$st_dir/$st_country//$st_year/$filename.DTA", clear 
		capture keep caseid mmc1 v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mmidx* mm1_* mm2_* mm3* mm4*  mm6* mm7* mm8* mm9* mm15*
		qui count if mmc1 ==.
		local nosib = r(N)
		display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"
		** Dropping females who did not respond to sibling history module 
        drop if mmc1 == . 								
		drop mmc1
	}
	
	else if "$filename" == "DOM_DHS4_2002_WN_Y2008M09D23" {   
        ** Dominican Republic 2002 - only half the respondents answered sib history modules so we drop the rest 
		use caseid v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* using "$raw_dir/$st_dir/$st_country//$st_year/$filename.DTA", clear 
		capture keep caseid mmc1 v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mmidx* mm1_* mm2_* mm3* mm4*  mm6* mm7* mm8* mm9* mm15* 
		qui count if mmc1 ==.
		local nosib = r(N)
		display as error "Dropping `nosib' observations from survey `id' (DOM 2001) because they did not respond to question MMC1"
        ** Dropping females who were not asked sibling history module in Dominican Republic 2001 survey 
		drop if mmc1 == . 								
		drop mmc1
	}
	
	else if "$filename" == "CIV_AIS5_2005_WN_Y2011M02D08" {	
        ** Cote d'Ivoire 2005 - AIS survey with male respondents included. Need to drop males 
		use aidsex caseid v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* using "$raw_dir/$st_dir/$st_country//$st_year/$filename.DTA", clear 
		capture keep mmc1 aidsex caseid v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mmidx* mm1_* mm2_* mm3* mm4*  mm6* mm7* mm8* mm9* mm15*
		** Dropping male respondents from Cote d'Ivoire 2005 AIS 
        drop if aidsex == 1 							
		drop aidsex
		qui count if mmc1 ==.
		local nosib = r(N)
		display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"
		** Dropping females who did not respond to sibling history module 
        drop if mmc1 == . 								
		drop mmc1
	}

	else if "$filename" == "NPL_DHS5_2006_WN_Y2008M09D23" {		
        ** Nepal 2006 - need to fix Nepali dates 
		use caseid v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* using "$raw_dir/$st_dir/$st_country//$st_year/$filename.DTA", clear 
		local seconds_change = - ( 56*365*24*3600 + 8*30*24*3600 + 15*24*3600 )
		foreach var of varlist v007 v008 v011 mm4* mm8* mm15* {
			qui replace `var' = trunc( (`var'*30*24*3600 + `seconds_change' ) / ( 30*24*3600 ) )
			}
		capture keep caseid mmc1 v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mmidx* mm1_* mm2_* mm3* mm4*  mm6* mm7* mm8* mm9* mm15*
		qui count if mmc1 ==.
		local nosib = r(N)
		display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"
		** Dropping females who did not respond to sibling history module 
        drop if mmc1 == . 								
		drop mmc1
        ** v007 for this survey is wacky - therefore just taking the year of interview from v008 
		replace v007 = int((v008 - 1)/12) + 1900		
	}
	
	else if "$filename" == "GHA_SP_DHS5_2007_2008_WN_PH2_Y2009M06D01" {
		use qcluster qnumhh qline qweight qintm qinty qintc q103m q103y q103c q104 q801 q804* q805* q806* q807* q808* q809* q810* q811* q812* using "$raw_dir/$st_dir/$st_country//$st_year/$filename.DTA", clear
		rename qweight v005
		rename qintm v006
		rename qinty v007
		rename qintc v008
		rename q103m v009
		rename q103y v010
		rename q103c v011
		rename q104 v012
		renpfix q805 mm1
		renpfix q806 mm2
		renpfix q807c mm4
		renpfix q807 mm3
		renpfix q808c mm8
		renpfix q808 mm6
		renpfix q809 mm7
		renpfix q804 mmidx
		gen caseid=string(qcluster)+string(qnumhh)+string(qline)
		bysort caseid: gen n=_n
		replace caseid=caseid+string(n)
		gen v013=1 if v012>=15 & v012<=19
		replace v013=2 if v012>=20 & v012<=24
		replace v013=3 if v012>=25 & v012<=29
		replace v013=4 if v012>=30 & v012<=34
		replace v013=5 if v012>=35 & v012<=39
		replace v013=6 if v012>=40 & v012<=44
		replace v013=7 if v012>=45 & v012<=49
		gen v014=.
		gen v000="GHA"
		forvalues i=1/9 {
			gen mm15_0`i'=.
			replace mm2_0`i'=0 if mm2_0`i'==2
			gen mm9_0`i' = 2 if q810_0`i'==1 | q811_0`i'==1 | q812_0`i'==1
			}
		forvalues i=10/17 {
			gen mm15_`i'=.
			replace mm2_`i'=0 if mm2_`i'==2
			gen mm9_`i' = 2 if q810_`i'==1 | q811_`i'==1 | q812_`i'==1
			}
		rename q801 mmc1
		qui count if mmc1 ==.
		local nosib = r(N)
		display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"
		drop if mmc1 == . 								
		** Dropping females who did not respond to sibling history module
		drop mmc1 qcluster qnumhh qline
	}
	
	else if "$filename" == "SDN_DHS1_1989_1990_WN_Y2008M09D23" {	
        ** Sudan 1989-1990 survey. Sibling history variables coded differently. Need to standardize. 
		use caseid v000* v005* v006* v008* v007* v009* v010* v011* v012* v013* v014* s91co* s803* s804* s805* s807* s808* s810* s811* using "$raw_dir/$st_dir/$st_country//$st_year/$filename.DTA", clear
		renpfix s91co mmidx
		renpfix s803 mm1
		renpfix s804 mm2
		renpfix s805 mm3
		renpfix s807 mm6
		renpfix s808 mm7
		forvalues i=1/9 {
			gen mm4_0`i'=.
			gen mm5_0`i'=.
			gen mm8_0`i'=.
			gen mm9_0`i'=2 if s810_0`i'==1 | s811_0`i'==1
			gen mm15_0`i'=.
			replace mm2_0`i'=0 if mm2_0`i'==2
			}
		forvalues i=10/16 {
			gen mm4_`i'=.
			gen mm5_`i'=.
			gen mm8_`i'=.
			gen mm9_0`i'=2 if s810_`i'==1 | s811_`i'==1
			gen mm15_`i'=.
			replace mm2_`i'=0 if mm2_`i'==2
			}
		drop s810* s811*	
	}
	
	**  ZAF must be broken up by province
	else if "$filename" == "ZAF_DHS3_1998_WN_Y2008M09D23.DTA" {
		use caseid v000* v005* v006* v008* v021 v024 v007* v009* v010* v011* v012* v013* v014* mm* using "$raw_dir/$st_dir/$st_country//$st_year/ZAF_DHS3_1998_WN_Y2008M09D23.DTA", clear 
		// put the province name in the caseid variable
		replace caseid = caseid + "p" + string(v024)
		drop v024
		capture keep caseid mmc1 v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mmidx* mm1_* mm2_* mm3* mm4*  mm6* mm7* mm8* mm9* mm15*
		qui count if mmc1 ==.
		local nosib = r(N)
		display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"
		// Dropping females who did not respond to sibling history module 
		drop if mmc1 == . 								
		drop mmc1
	}
	// Using Kenya provinces 
	else if "$filename" == "KEN_DHS3_1998_WN_Y2008M09D23.DTA" | "$filename" == "KEN_DHS4_2003_WN_Y2008M09D23.DTA" | "$filename" == "KEN_DHS5_2008_2009_WN_Y2010M08D25.DTA"{
		di in red "Kenya subnational here, $filename"
		cap use caseid v000* v001* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* sdist mm* using "$raw_dir/$st_dir/$st_country//$st_year/$filename.DTA", clear
		** district codes are not consistent across datasets 
		replace caseid = caseid + "p" + string(sdist) + " f" + "$st_year"
	}
	else if "$filename" == "AFG_SP_DHS6_2010_WN_Y2011M12D01" { 
        ** Afghanistan 2010 Sibling history variables coded differently. Need to standardize, also it's missing sibid 
		use qweight qintcg qintyg qintmg q102m q102yg q102cg q103 q605* q606* q607* q607c* q610* q610c* q611* q614* using "$raw_dir/$st_dir/$st_country//$st_year/$filename.DTA", clear
		drop  q607cg_*  q610cg_*
		gen caseid = _n
		tostring caseid, replace
		gen v013 = "15-19" if q103>=15 & q103 <=19
		replace v013 = "20-24" if q103>=20 & q103 <=24
		replace v013 = "25-29" if q103>=25 & q103 <=29
		replace v013 = "30-34" if q103>=30 & q103 <=34
		replace v013 = "35-39" if q103>=35 & q103 <=39
		replace v013 = "40-44" if q103>=40 & q103 <=44
		replace v013 = "45-49" if q103>=45 & q103 <=49
		drop if v013 == ""
		gen v014 = .
		foreach i of numlist 1/30 {
			gen mmidx_`i' = `i'
		}
		** convert from persian to gregorian calendar. the only method i have for doing this is basing it off of variables q102cg (dob cmc gregorian) and q102c (dob cmc persian). These are always different by exactly 255 
		foreach i of numlist 1/30 {
			if `i' > 9 {
				replace q607c_`i' = q607c_`i' + 255
				replace q610c_`i' = q610c_`i' + 255
			}
			else {
				replace q607c_0`i' = q607c_0`i' + 255
				replace q610c_0`i' = q610c_0`i' + 255			
			}
		}
		    
		rename qweight v005	
		rename qintcg v008
		rename qintyg v007
		rename qintmg v006
		rename q102m v009
		rename q102yg v010
		rename q102cg v011
		rename q103 v012
		renpfix q605_ mm1_
		renpfix q606_ mm2_
		foreach var of varlist mm2* {
			replace `var' = 0 if `var' == 2
		}
		renpfix q607_ mm3_	
		renpfix q607c_ mm4_
		renpfix q610_ mm6_
		renpfix q611_ mm7_ 	
		renpfix q610c_ mm8_
		foreach i of numlist 1/30 {
			if `i' > 9 {
				gen mm9_`i' = 1 if q614_`i' == 2
				replace mm9_`i' = 2 if q614_`i' == 1
				replace mm9_`i' = q614_`i' if mm9_`i' == .
				drop q614_`i'
			}
			else {
				gen mm9_0`i' = 1 if q614_0`i' == 2
				replace mm9_0`i' = 2 if q614_0`i' == 1
				replace mm9_0`i' = q614_0`i' if mm9_0`i' == .
				drop q614_0`i'
			}
		}
		
		foreach i of numlist 1/30 {
			if `i' > 9 {
				gen mm15_`i' = 2010 -  mm6_`i'
				replace mm15_`i' = 99 if mm6_`i'==99 | mm6_`i'==98
			}
			else {
				gen mm15_0`i' = 2010 -  mm6_0`i' 
				replace mm15_0`i' = 99 if mm6_0`i'==99 | mm6_0`i'==98
			}
		}	
	}
	** If the variables in the modules are not custom, they should follow one of the formats below 
	else {
		cap use caseid v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mm* using "$raw_dir/$st_dir/$st_country//$st_year/$filename.DTA", clear
		if (_rc != 0) {
            cap use CASEID V000* V005* V006* V008* V021 V007* V009* V010* V011* V012* V013* V014* MM* using "$raw_dir/$st_dir/$st_country//$st_year/$filename.DTA", clear
            if (_rc != 0) {
				capture use caseid V000* V005* V006* V008* V021 V007* V009* V010* V011* V012* V013* V014* MM* using "$raw_dir/$st_dir/$st_country//$st_year/$filename.DTA", clear
	            if (_rc != 0) {
					clear 
					set obs 1
					gen data_name = "$filename"
					gen iso3 = "$st_country"
					gen year = "$st_year"
					gen read_fail = 1
					gen no_maternal = 0
					gen note = "Failed to read in data: either no maternal questions, or variables are formatted differently (or both) - investigate"
					cd "$temp_error_dir"
					saveold "failed_DHS_$filename.dta", replace
					exit
				}
			else {
                capture keep caseid MMC1 V000* V005* V006* V008* V021 V007* V009* V010* V011* V012* V013* V014* MMIDX* MM1_* MM2_* MM3* MM4*  MM6* MM7* MM8* MM9* MM15*
				capture rename caseid CASEID
                renpfix V v
                renpfix MMIDX mmidx
                renpfix MMC mmc
                renpfix MM mm
                qui count if mmc1 ==.
                local nosib = r(N)
                display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"
                ** Dropping females who did not respond to sibling history module 
                drop if mmc1 == . 								
                drop mmc1 
            }

			}
            else {
                capture keep CASEID MMC1 V000* V005* V006* V008* V021 V007* V009* V010* V011* V012* V013* V014* MMIDX* MM1_* MM2_* MM3* MM4*  MM6* MM7* MM8* MM9* MM15*
                renpfix V v
                renpfix MMIDX mmidx
                renpfix MMC mmc
                renpfix MM mm
                qui count if mmc1 ==.
                local nosib = r(N)
                display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"
                ** Dropping females who did not respond to sibling history module 
                drop if mmc1 == . 								
                drop mmc1 
            }
        }
        else {
            capture keep caseid mmc1 v000* v005* v006* v008* v021 v007* v009* v010* v011* v012* v013* v014* mmidx* mm1_* mm2_* mm3* mm4*  mm6* mm7* mm8* mm9* mm15*
            qui count if mmc1 ==.
            local nosib = r(N)
            display as error "Dropping `nosib' observations from survey `id' because they did not respond to question MMC1"
            ** Dropping females who did not respond to sibling history module 
            drop if mmc1 == . 								
            drop mmc1
        }
	}
if (_N == 0) {
    clear 
    set obs 1
    gen data_name = "$filename"
    gen iso3 = "$st_country"
    gen year = "$st_year"
    gen read_fail = 0
    gen no_maternal = 1
    gen note = "All maternal questions missing: other variables have might have necessary info - investigate"
    cd "$temp_error_dir"
    saveold "failed_DHS_$filename.dta", replace
    exit
}
else {	
	pause

	if v007>99 {				
		replace v007=v007-1900
	}		

		
		replace v005 = v005/1000000
		gen country = "$st_country"
		gen filename="$filename"	   
		gen surveyyear = substr("$st_year",1,4)
			** Manual fix for the different length of the BGD BMMS filename
		replace surveyyear = "2001" if "$filename"=="CRUDE_INT_BMMS_BGD_2001-2001_IND_v05152008"
		destring surveyyear, replace	
			
       
		** ***************************************
		** BASIC INPUTS FOR MATERNAL MORTALITY***
		** ***************************************
				
		** Prepare to reshape data to sibling level
										
			forvalues x=1/9 {
				*Generate empty variables for datasets that don't have them, for reshape
				capture gen mm15_0`x' = .
				}
			
		capture renpfix mmidx_0 mmidx_
		capture renpfix mm1_0 mm1_
		capture renpfix mm2_0 mm2_
		capture renpfix mm3_0 mm3_
		capture renpfix mm4_0 mm4_
		capture renpfix mm5_0 mm5_
		capture renpfix mm6_0 mm6_
		capture renpfix mm7_0 mm7_
		capture renpfix mm8_0 mm8_
		capture renpfix mm9_0 mm9_
		capture renpfix mm15_0 mm15_

		
			
			** The following applies to the respondent - since they are not reported in the sib history, we generate their information using the information that they provide about themselves (yob, etc.)
			*id
			gen mmidx_0 = 0    
			*sex
			gen mm1_0 = 2      
			*alive
			gen mm2_0 = 1      
			*current age
			gen mm3_0 = v012   
			*CMC date of birth
			gen mm4_0 = v011   
			*years ago died
			gen mm6_0 = .      
			*age at death
			gen mm7_0 = .      
			*CMC date of death
			gen mm8_0 = . 
			*year of death
			gen mm15_0 = .
			
			di in red "$filename"
			capture rename CASEID caseid
			label drop _all
			label values mm*
			reshape long mmidx_ mm1_ mm2_ mm3_ mm4_ mm6_ mm7_ mm8_ mm9_ mm15_, i(caseid) j(mm)
			
			** Drop observations with no sibling data 
			drop if mm1_ ==. & mm2_ == . & mm3_==. & mm4_==.		
			
			rename caseid id
			rename v007 yr_interview
			rename v009 month_of_birth
			rename v010 yob_resp
			rename v011 cmc_dob
			rename v012 ageresp
			rename v013 ageresp_cat
			rename v014 data_completeness
			rename mm1_ sex
			replace sex = 0 if sex == 2
			rename mm2_ alive
			rename mm3_ age_sib_ifalive
			rename mm4_ cmc_dob_sib
			rename mm6_ years_since_dead
			rename mm7_ age_sib_ifdead
			rename mm8_ cmc_dod
			rename mm15_ yod_sib
			rename mmidx sibid
			
			replace yr_interview = yr_interview-1900 if yr_interview>1900
			
		* ***CALCULATE BASIC INPUTS****
		* **Year of death
		capture drop yod
			* Primary input: mm8_ : CMC date of death of sibling
			* Secondary input: v007 - mm6_ : year of interview - number of years ago respondent's sibling died
			* Tertiary input: mm15_: year of death of sibling (not in all surveys)
		gen yod = int((cmc_dod-1)/12)
		replace yod = yr_interview-years_since_dead if yod==. | (country == "NPL" & yr_interview == 1996)
		capture replace yod = yod_sib if yod==.

		* ***Year of birth
		capture drop yob
			* Primary input: mm4_: CMC date of birth of sibling
			* Secondary input: year of interview - sibling's current age [if alive]
			* Tertiary input: year of interview - (age at death of sibling + years ago sibling died) [if dead]
		gen yob=int((cmc_dob_sib-1)/12)
		replace yob=yr_interview-age_sib_ifalive if (alive==1 & yob==. )  | (country == "NPL" & yr_interview == 1996)
		replace yob=yr_interview-(age_sib_ifdead+years_since_dead) if alive==0 & yob==.  | (country == "NPL" & yr_interview == 1996)

		replace yob = . if yob < 0
                ** PERU 2004 - no CMC date of death (mm04)
				if "$filename"=="" {
				replace yod=mm15_
				replace yod=. if yod>surveyyear
				replace yod=. if yod==9998
				replace yod=surveyyear-mm6_ if yod==.
				**ASSUME ON AVERAGE PEOPLE DIED IN JUNE (6) TO CALCULATE APPROXIMATE CMC DATE OF DEATH
				replace	mm8_=(yod*12)+6
			}
			

			capture drop mmc*
			

		****DEAL WITH CALENDARS: ETHIOPIA****
			*ETHIOPIA DATE CONVERSION - ANYTHING BEFORE MAY: +7 YEARS, AFTER MAY: + 8 YEARS
			replace yob = yob + 7 if "$st_country" == "ETH" & v006 < 5 
			replace yob = yob + 8 if "$st_country" == "ETH" & v006 >= 5 
			replace yod = yod + 7 if "$st_country" == "ETH" & v006 < 5 
			replace yod = yod + 8 if "$st_country" == "ETH" & v006 >= 5 

			

		*SEX dummies 
		gen male = 1 if sex == 1
		replace male = 0 if sex != 1
		gen female = 1 if sex == 0 
		replace female = 0 if sex != 0 

		*** DATA PREP: Age categories for deaths, survivors, and exploration of unknown sexes

        ** Generate indicator variable for deaths 
		gen death = 1 if yod ~=.						
		replace death = 0 if yod ==.
        ** converting 2 digit years into 4 digits 
		replace yob = yob + 1900						
		replace yod = yod + 1900
		gen aged = yod - yob
        ** Some ages at death are negative? 
		replace aged = . if aged < 0						
        ** Generating categories of age at death 
		gen agedcat = 0 if aged < 1						
		replace agedcat = 1 if aged >= 1 & aged < 5

		forvalues a = 5(5)70 {
			local apn = `a' + 5
			replace agedcat = `a' if aged >= `a' & aged <`apn'
		}
		replace agedcat = 75 if aged>= 75

        ** Some values of sex are coded as 9 or are missing 
		tab sex, miss						
        ** Who are unknowns? Sibs or respondents? 
		tab sex sibid, miss					
        ** Missing sexes are disproportionately deaths 
		tab sex death, miss r				
        ** More likely to be child deaths but still majority are deaths over age 15 
		tab sex agedcat, miss r					
        ** Code all unknown sex the same 
		replace sex = . if sex!=0 & sex!=1				

		** Generating Age Blocks for survivors (age the sib was or would have been at the time of the survey):
        ** Age at the time of the interview 
		gen age = yr_interview -  yob								
        ** age blocks:  5 year categories 15-19, 20-24, etc. 
		gen ageblock = . 						
		forvalues age = 0(5)60   {
			replace ageblock =`age' if age - `age'  < 5 & age - `age' >= 0
			}
        ** Technically, the respondent is aged 15-49. However, because the specific month calculation cannot be done in our analysis, there are a couple of ages that fall in the 50-54 category. Since these are very few (anywhere from 10-100), especially in each  country survey, include these into the 45-49 age category 
		replace ageblock = 45 if ageblock == 50 & sibid==0	

		*** Redistribute sibs of unknown sex to males and females

		*** For alive sibs of unknown sex, redistribute according to sex distribution of alive sibs within each age group, by survey

		bysort ageblock: egen males = total(male) if death==0
		sort ageblock males
		by ageblock: replace males = males[1] if males==.

		bysort ageblock: egen females = total(female)
		sort ageblock females
		by ageblock: replace females = females[1] if females==.

		bysort ageblock: gen pctmale = males/(males+females)

		gen rnd = runiform()

		bysort ageblock: replace sex = 1 if sex==. & death==0 & rnd <= pctmale
		bysort ageblock: replace sex = 0 if sex==. & death==0 & rnd > pctmale

		drop males females rnd pctmale

		*** For dead sibs of unknown sex, redistribute according to sex distribution of dead sibs within age group of death, pooling over all surveys (not enough deaths to do within survey)

		bysort agedcat: egen males = total(male) if death==1
		sort agedcat males
		by agedcat: replace males = males[1] if males==.

		bysort agedcat: egen females = total(female) if death==1
		sort agedcat females
		by agedcat: replace females = females[1] if females==.

		bysort agedcat: gen pctmale = males/(males+females)

		gen rnd = runiform()
		replace sex = 1 if sex==. & death==1 & rnd <= pctmale
		replace sex = 0 if sex==. & death==1 & rnd > pctmale

		drop males females rnd pctmale male female death aged agedcat age ageblock


		*** Drop siblings for whom alive/dead status is unknown
		drop if alive > 1

		compress

			
			***************************************
			***DUMMY FOR MATERNAL CAUSE OF DEATH***
			***************************************

			gen matdeath=0
			replace matdeath=1 if mm9==2 | mm9_==3 | mm9_==4 | mm9_==5 | mm9_==6
			
			gen matdeathmiss=matdeath
			replace matdeathmiss=. if mm9_==. | mm9_==98 | mm9_==99
		
					
			*keep only women
			keep if sex==0
			
			************************************
			***GENERATE SURVEY-SPECIFIC FILES***
			************************************
			
			gen source = "DHS sibling history"
			gen type = "Sibling history"
			
			local surveyyear = surveyyear
			
			** Drop duplicate survey years and surveys with recall bias
			drop if filename == "COD_DHS6_2013_WN_Y2014M10D08" & (yod >= 1999 & yod <= 2007)
			drop if filename == "LBR_DHS6_2013_WN_Y2014M09D24" & (yod >= 1999 & yod <= 2006)
			drop if filename == "NAM_DHS6_2013_WN_Y2015M01D08" & (yod >= 1999 & yod <= 2006)
			drop if filename == "NGA_DHS6_2013_WN_Y2014M06D17" & (yod >= 1999 & yod <= 2008 )
			drop if filename == "SLE_DHS6_2013_WN_Y2014M10D01" & (yod >= 1999 & yod <= 2008 )

			save "$data_dir/bycountry/$st_country `surveyyear'.dta", replace
			saveold "$data_dir/bycountry/$st_country `surveyyear'.dta", replace

}


}


log close

 