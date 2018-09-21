**********************************************************************************
**Purpose: Loops through all DHS and AIS women's modules and creates a codebook of these files
**********************************************************************************

clear all			
cap restore, not	
capture log close  
set more off  
     
if c(os) == "Windows" {
	global prefix "J:"
}
if c(os) == "Unix" {
	global prefix "/home/j"     
}

** Working directories
global raw_dir "$prefix/DATA/"
global log_dir "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/logs"
global cb_file "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/data/DHS_codebook_current.csv"
global cb_file_archive "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/data/archive/DHS_codebook_$S_DATE.csv"
global cb_file_compare "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/data/archive/DHS_codebook_comparison.dta"
global analysis_file "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/data/DHS_analysis_file.csv"
global temp_error_dir "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/temp"
global error_file "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/data/failed_DHS.dta"
global error_file_archive "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/data/archive/failed_DHS_$S_DATE.dta"

** create file with all DHS and AIS women's modules
** Generate local that contains filenames of all possibly relevant datasets to loop through.
global dhsdir "$prefix/DATA/MACRO_DHS"
global aisdir "$prefix/DATA/MACRO_AIS"

** start a new log
cd "$log_dir"
cap log close
log using "dp0.smcl"

** New data option (0 means run all DHS, 1 means just run new DHS
set obs 1
gen a = "$arg"
destring a, replace
global newflag  = a

di in red "newflag $newflag"

** create tempfile
tempfile cb

** loop through all DHS folders
local jdirs: dir "$dhsdir" dirs "*", respectcase
di `"`jdirs'"'
** "
local i=1

foreach iso of local jdirs {
qui {
	noi di "`iso'"
    ** these folders don't have any DHS's in them
    if ("`iso'" == "CRUDE" | "`iso'" == "DHS_REPORTS" | "`iso'" == "QUESTIONNAIRES") continue
    
    ** get the years in the DHS-country folder
    local jdir`iso': dir "$dhsdir/`iso'" dirs "*", respectcase
    noi di `"    `jdir`iso''"' 
    ** "	
    foreach yr of local jdir`iso' {
        noi di "    `yr'"
        
        ** get the files in the DHS-country-year folder
        local jdir`iso'_`yr': dir "$dhsdir/`iso'/`yr'" files "*_WN_*.DTA", respectcase
        
        cd "$dhsdir/`iso'/`yr'"
        noi di `"      `jdir`iso'_`yr''"'
        ** "
        if `"`jdir`iso'_`yr''"'=="" { 
        ** "
            continue	
        }
        
        ** Loop through all of the files with _WN_ in the crude DHS folder. 
        foreach file of local jdir`iso'_`yr' { 
            clear 
            set obs 1
            gen data_name = "`file'"
            gen survey = "MACRO_DHS"
            gen country = "`iso'"
            gen year = "`yr'"
            gen maternal = 1
            
            ** specify DHS's that we know have no maternal questions or unusable maternal questions [remember, each inlist() can only contain 9 files] -must be done manually
			** the following are old files:
            replace maternal = 0 if regexm(data_name,"IND_DHS4") & regexm(data_name,"1998_2000")
			replace maternal = 0 if regexm(data_name,"IND_DHS2") & regexm(data_name, "1992_1993")
            replace maternal = 0 if inlist(data_name,"BFA_DHS4_2003_WN_Y2008M09D23.DTA","CIV_DHS3_1998_1999_WN_Y2008M09D23.DTA","CMR_DHS2_1991_WN_Y2008M09D23.DTA","GHA_DHS4_2003_WN_Y2008M09D23.DTA","NPL_DHS3_1996_WN_Y2008M09D23.DTA","NPL_DHS4_2001_WN_Y2008M09D23.DTA","PAK_DHS5_2006_2007_WN_Y2008M09D23.DTA","PHL_DHS4_2003_WN_Y2008M09D23.DTA","ARM_DHS6_2010_WN_Y2012M04D30.DTA")
			replace maternal = 0 if inlist(data_name,"JOR_DHS5_2007_WN_Y2008M09D23.DTA","JOR_DHS6_2012_WN_Y2013M12D16.DTA","PAK_DHS6_2012_2013_WN_Y2014M01D22.DTA","TJK_DHS6_2012_WN_Y2013M12D04.DTA","HTI_DHS6_2012_WN_Y2013M07D15.DTA","HND_DHS6_2011_2012_WN_Y2013M06D26.DTA","BGD_DHS6_2011_2012_WN_Y2013M02D11.DTA","NPL_DHS6_2011_WN_Y2012M04D05.DTA","COL_DHS6_2009_2010_WN_Y2011M03D17.DTA")
            replace maternal = 0 if inlist(data_name,"JOR_ITR_DHS6_2009_WN_Y2010M11D03.DTA","GUY_DHS5_2009_WN_Y2011M08D25.DTA","MDV_DHS5_2009_WN_Y2010M12D16.DTA","ALB_DHS5_2008_2009_WN_Y2010M11D12.DTA","GHA_DHS5_2008_WN_Y2009M10D09.DTA","EGY_DHS5_2008_WN_Y2009M06D19.DTA","BRA_DHS1_1986_WN_Y2013M05D13.DTA","KEN_DHS1_1988_1989_WN_Y2012M04D24.DTA","PHL_DHS5_2008_WN_Y2011M03D23.DTA")
            replace maternal = 0 if inlist(data_name,"DOM_SP_DHS5_2007_WN_Y2010M04D02.DTA","MDA_DHS5_2005_WN_Y2008M09D23.DTA","ARM_DHS5_2005_WN_Y2008M09D23.DTA","BGD_DHS4_2004_WN_Y2008M09D23.DTA","TUR_DHS4_2003_2004_WN_Y2008M09D23.DTA","DOM_DHS4_1999_WN_Y2008M09D23.DTA","COM_DHS3_1996_WN_Y2008M09D23.DTA","HND_DHS5_2005_2006_WN_Y2008M09D23.DTA","JOR_DHS4_2002_WN_Y2008M09D23.DTA")
            replace maternal = 0 if inlist(data_name,"PER_DHS1_1986_WN_Y2011M02D09.DTA","RWA_ITR_DHS5_2007_2008_WN_Y2010M08D25.DTA","PER_DHS5_2003_2005_WN_Y2008M09D23.DTA","GHA_SP_DHS5_2007_2008_WN_Y2010M04D02.DTA","IDN_DHS_2012_WN_AGES_15_24_Y2013M10D02.DTA","KAZ_DHS4_1999_WN_Y2008M09D23.DTA","UKR_DHS5_2007_WN_Y2008M09D23.DTA","BGD_DHS5_2007_WN_Y2009M05D11.DTA","BGD_DHS4_1999_2000_WN_Y2008M09D23.DTA")
			replace maternal = 0 if inlist(data_name,"AZE_DHS5_2006_WN_Y2008M09D23.DTA","IND_DHS5_2005_2006_WN_Y2008M09D23.DTA","EGY_DHS5_2005_WN_Y2008M09D23.DTA","COL_DHS5_2004_2005_WN_Y2008M09D23.DTA","EGY_ITR_DHS4_2003_WN_Y2008M09D23.DTA","NGA_DHS4_2003_WN_Y2008M09D23.DTA","ERI_DHS4_2002_WN_Y2008M09D23.DTA","VNM_DHS4_2002_WN_Y2008M09D23.DTA","UZB_SP_DHS4_2002_WN_Y2008M09D23.DTA")
			replace maternal = 0 if inlist(data_name,"BEN_DHS4_2001_WN_Y2008M09D23.DTA","NIC_DHS4_2001_WN_Y2008M09D23.DTA","EGY_DHS4_2000_WN_Y2008M09D23.DTA","COL_DHS4_2000_WN_Y2008M09D23.DTA","ARM_DHS4_2000_WN_Y2008M09D23.DTA","TZA_DHS4_1999_WN_Y2008M09D23.DTA","SEN_DHS4_1999_WN_Y2008M10D03.DTA","BGD_DHS3_1996_1997_WN_Y2008M09D23.DTA","BGD_DHS3_1993_1994_WN_Y2008M09D23.DTA")
            replace maternal = 0 if inlist(data_name,"GHA_DHS4_1998_1999_WN_Y2008M09D23.DTA","GTM_ITR_DHS4_1998_1999_WN_Y2008M09D23.DTA","NER_DHS3_1998_WN_Y2008M09D23.DTA","KHM_SP_DHS3_1998_WN_Y2010M04D02.DTA","BOL_DHS3_1998_WN_Y2008M09D23.DTA","TUR_DHS4_1998_WN_Y2008M09D23.DTA","NIC_DHS3_1997_1998_WN_Y2008M09D23.DTA","KGZ_DHS3_1997_WN_Y2008M09D23.DTA","VNM_DHS3_1997_WN_Y2008M09D23.DTA")
			replace maternal = 0 if inlist(data_name,"SEN_DHS3_1997_WN_Y2008M09D23.DTA","EGY_IN_DHS3_1996_1997_WN_Y2010M04D02.DTA","UZB_DHS3_1996_WN_Y2008M09D23.DTA","DOM_DHS3_1996_WN_Y2008M09D23.DTA","UGA_IN_DHS3_1995_1996_WN_Y2010M04D02.DTA","EGY_DHS3_1995_1996_WN_Y2008M09D23.DTA","COL_DHS3_1995_WN_Y2008M09D23.DTA","MAR_SP_DHS3_1995_WN_Y2008M11D03.DTA","KAZ_DHS3_1995_WN_Y2008M09D23.DTA")
			replace maternal = 0 if inlist(data_name,"HTI_DHS3_1994_1995_WN_Y2008M09D23.DTA","GHA_DHS3_1993_1994_WN_Y2008M09D23.DTA","TUR_DHS3_1993_WN_Y2008M09D23.DTA","KEN_DHS3_1993_WN_Y2008M09D23.DTA","EGY_DHS2_1992_1993_WN_Y2008M09D23.DTA","BFA_DHS2_1992_1993_WN_Y2008M09D23.DTA","ZMB_DHS2_1992_WN_Y2008M09D23.DTA","RWA_DHS2_1992_WN_Y2008M09D23.DTA","TZA_DHS2_1991_1992_WN_Y2008M09D23.DTA")
			replace maternal = 0 if inlist(data_name,"YEM_DHS2_1991_1992_WN_Y2008M09D23.DTA","IDN_DHS2_1991_WN_Y2008M09D23.DTA","DOM_DHS2_1991_WN_Y2008M09D23.DTA","BRA_DHS2_1991_WN_Y2008M09D23.DTA","PAK_DHS2_1990_1991_WN_Y2008M09D23.DTA","NGA_DHS2_1990_WN_Y2008M09D23.DTA","COL_DHS2_1990_WN_Y2008M09D23.DTA","JOR_DHS2_1990_WN_Y2008M09D23.DTA","PRY_DHS2_1990_WN_Y2008M09D23.DTA")
			replace maternal = 0 if inlist(data_name,"BOL_DHS1_1989_WN_Y2008M09D23.DTA","UGA_DHS1_1988_1989_WN_Y2008M09D23.DTA","EGY_DHS1_1988_1989_WN_Y2008M09D23.DTA","ZWE_DHS1_1988_1989_WN_Y2008M09D23.DTA","BWA_DHS1_1988_WN_Y2008M09D23.DTA","TGO_DHS1_1988_WN_Y2008M09D23.DTA","GHA_DHS1_1988_WN_Y2008M09D23.DTA","TUN_DHS1_1988_WN_Y2008M09D23.DTA","TTO_DHS1_1987_WN_Y2008M09D23.DTA")
			replace maternal = 0 if inlist(data_name,"LKA_DHS1_1987_WN_Y2008M09D23.DTA","MLI_DHS1_1987_WN_Y2008M09D23.DTA","MAR_DHS1_1987_WN_Y2008M09D23.DTA","NPL_IN_DHS1_1987_WN_Y2008M11D03.DTA","THA_DHS1_1987_WN_Y2008M09D23.DTA","ECU_DHS1_1987_WN_Y2008M09D23.DTA","MEX_DHS1_1987_WN_Y2008M09D23.DTA","IDN_DHS1_1987_WN_Y2008M09D23.DTA","GTM_DHS1_1987_WN_Y2008M09D23.DTA")
			replace maternal = 0 if inlist(data_name,"BDI_DHS1_1987_WN_Y2008M09D23.DTA","NGA_SP_DHS1_1986_1987_WN_Y2010M04D02.DTA","LBR_DHS1_1986_WN_Y2008M09D23.DTA","SEN_DHS1_1986_WN_Y2008M09D23.DTA","COL_DHS1_1986_WN_Y2008M09D23.DTA","DOM_DHS1_1986_WN_Y2008M09D23.DTA","SLV_DHS1_1985_WN_Y2008M09D23.DTA","YEM_DHS6_2013_WN_Y2015M07D13")
			** add new surveys that get found that don't have maternal questions and answers here:
			** replace maternal = 0 if inlist(data_name,"")
            cap append using `cb'
            save `cb', replace
        }
    }
}
}

** loop through all AIS folders
local jdirs: dir "$aisdir" dirs "*", respectcase
di `"`jdirs'"'
** "
local i=1

foreach iso of local jdirs {
qui {
	noi di "`iso'"
    ** these folders don't have any DHS's in them
    if ("`iso'" == "CRUDE" | "`iso'" == "DHS_REPORTS" | "`iso'" == "QUESTIONNAIRES") continue
    
    ** get the years in the DHS-country folder
    local jdir`iso': dir "$dhsdir/`iso'" dirs "*", respectcase
    noi di `"    `jdir`iso''"' 
    ** "	
    foreach yr of local jdir`iso' {
        noi di "    `yr'"
        
        ** get the files in the DHS-country-year folder
        local jdir`iso'_`yr': dir "$dhsdir/`iso'/`yr'" files "*_WN_*.DTA", respectcase
        
        cd "$dhsdir/`iso'/`yr'"
        noi di `"      `jdir`iso'_`yr''"'
        ** "
        if `"`jdir`iso'_`yr''"'=="" { 
        ** "
            continue	
        }
        
        ** Loop through all of the files with _WN_ in the crude DHS folder. 
        foreach file of local jdir`iso'_`yr' { 
            clear 
            set obs 1
            gen data_name = "`file'"
            gen survey = "MACRO_AIS"
            gen country = "`iso'"
            gen year = "`yr'"
            gen maternal = 1
            cap append using `cb'
            save `cb', replace
        }
    }
}
}

** drop AIS-DHS duplicates
duplicates tag data_name, g(d)
drop if d == 1 & survey == "MACRO_AIS"
drop d

sort survey country year

** add in non-DHS, non-AIS surveys that are formatted in the same way as DHS/AIS
** NOTE:  These will need to be specially added in DHS prep step 1 in order to deal with the different file locations
tempfile survs
save `survs', replace

** set number of surveys to add (add 1 to this when you add a new survey)
clear
set obs 1

** set variables
gen data_name = ""
gen survey = ""
gen country = ""
gen year = ""
gen maternal = 1

** add in surveys
replace data_name = ""              if _n == 1
replace survey = ""                 if _n == 1
replace country = ""                if _n == 1
replace year = ""                   if _n == 1

** add these onto the DHS and AIS surveys
append using `survs'
drop if data_name == ""

** move the old current version to an archived (comparison) version and save the new version as a current and archived version
preserve
insheet using "$cb_file", clear 
saveold "$cb_file_compare", replace
restore
outsheet using "$cb_file", c replace
outsheet using "$cb_file_archive", c replace

** create a flag for new DHS's by merging on the comparison file and findeing the ones that are new
merge 1:1 data_name survey country year using "$cb_file_compare"
gen new = 0
replace new = 1 if _m == 1
drop _m

** Get only maternal dataset, and create a local that specifies the number of rows in the dataset
keep if maternal == 1

** erase the file that contains DHS's that don't run (as well as temp files), so that the new file created on this run is a blank slate
cap erase "$error_file"
cd "$temp_error_dir"
local files: dir "$temp_error_dir" files "*", respectcase
foreach file of local files {
    erase "`file'"
}

** save file for looping
sort survey country year
outsheet using "$analysis_file", c replace	

log close
