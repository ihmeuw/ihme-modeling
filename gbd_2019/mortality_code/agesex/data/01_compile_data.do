** Description: compiles the data to be used for the age/sex model

clear all
capture cleartmp
set more off
capture restore, not

set odbcmgr unixodbc
global j "FILEPATH"
local user "`c(username)'"
global homeprefix= "FILEPATH"
local code_dir  "FILEPATH"
qui do "FILEPATH/get_locations.ado"

local new_run_id = "`1'"
local ddm_est_run_id = "`2'"
local 5q0_est_run_id = "`3'"
local 5q0_data_run_id = "`4'"
local death_data_run_id = "`5'"

global input_dir = "FILEPATH"
global output_dir = "FILEPATH"

** directories
global cbh_dir "$FILEPATH"
global new_cbh_dir "FILEPATH"
global ddm_dir "FILEPATH"

** vr files
global cod_vr_file "FILEPATH"
global mort_vr_file "FILEPATH"
                                                 
** population/births files
global all_pop_file "FILEPATH"

** 5q0 files
global raw5q0_file "FILEPATH"
global estimate5q0_file "FILEPATH"

import delimited "$input_dir/loc_map.csv", clear
keep if level_all == 1
keep ihme_loc_id local_id_2013 region_name
replace local_id_2013 = "CHN" if ihme_loc_id == "CHN_44533"
tempfile codes
save `codes', replace

import delimited "$input_dir/loc_map.csv", clear
keep location_id ihme_loc_id
tempfile codes2
save `codes2', replace
                                                 
** Get births
import delimited "$input_dir/births_cov.csv", clear
tempfile birth_estimates
save `birth_estimates', replace

** 4 and 5 star VR locations
import delimited "$input_dir/stars_by_iso3_time_window.csv", clear
keep if stars >= 4
keep if time_window == "full_time_series"
keep  ihme_loc_id stars
tempfile four_five_star
save `four_five_star', replace
                                                 
** ********************
** Compile CBH data
** ********************

local surveys: dir "$cbh_dir" dirs "*", respectcase

local non_data_folders = "Skeleton FUNCTIONS archive"

local missing_files ""
tempfile temp
local count = 0
foreach survey of local surveys {
    di in white "  `survey'"

    local non_dat = 0
    foreach folder of local non_data_folders {
        if (regexm("`survey'","`folder'")) local non_dat = 1
    }

    if (`non_dat' == 0) {
        foreach sex in males females both {
            if ("`survey'" == "DHS-OTHER") {
                foreach fold in "/In-depth" "/Special" {
                    local survey "DHS-OTHER`fold'"
                    cap use "$cbh_dir/`survey'/results/by survey/q5 - `sex' - 5.dta", clear
                    if _rc != 0 {
                        local missing_files "`missing_files' `survey'-`sex'"
                    }
                    else {
                        local count = `count' + 1
                        gen source2 = "`survey'"
                        gen sex = subinstr("`sex'", "s", "", 1)
                        if (`count' > 1) append using `temp'
                        save `temp', replace
                    }
                }
      
            }
            else {
                cap use "$cbh_dir/`survey'/results/by survey/q5 - `sex' - 5.dta", clear
                if _rc != 0 {
                    local missing_files "`missing_files' `survey'-`sex'"
                }
                else {
        l           local count = `count' + 1
                    gen source2 = "`survey'"
                    gen sex = subinstr("`sex'", "s", "", 1)
                    if (`count' > 1) append using `temp'
                    save `temp', replace
                }
            }
        }
    }
}

import delimited "FILEPATH", clear
keep if source_type == "CBH" & used == 1
gen survey_name = subinstr(filename, "_V5Q0", "", .)
replace survey_name = subinstr(survey_name, "_v5q0", "", .)
replace survey_name = subinstr(survey_name, "_v5Q0", "", .)
replace survey_name = subinstr(survey_name, "_V5q0", "", .)
replace survey_name = subinstr(survey_name, "_IHME", "", .)
replace survey_name = subinstr(survey_name, "EST_", "", .)
replace survey_name = subinstr(survey_name, "_BY_SURVEY", "", .)
replace survey_name = subinstr(survey_name, ".dta", "", .)
replace survey_name = subinstr(survey_name, "_22125", "", .)

levelsof survey_name, local(new_surveys)
foreach survey of local new_surveys {
    if !`: list survey in surveys' { 
        di "`survey'"
        foreach sex in males females both {
            cap use "$new_cbh_dir/`survey'/results/by survey/q5 - `sex' - 5.dta", clear
            if _rc != 0 {
            }
            else {
                local count = `count' + 1
                gen source2 = "`survey'"
                gen sex = subinstr("`sex'", "s", "", 1)
                if (`count' > 1) append using `temp'
                save `temp', replace
            }
        }
    }
}


drop if q5 == .
gen source_y = source
replace source = source2

gen survey_year = substr(source_y,-4,4)
destring survey_year, replace

drop if source == "COD_DHS_2014" & country == "PER"

di "`missing_files'"
preserve
clear
local numobs = wordcount("`missing_files'")
set obs `numobs'
gen file = ""
local count = 1
foreach missfile of local missing_files {
    replace file = "`missfile'" if _n == `count'
    local count = `count' + 1
}
if (`numobs' == 0) {
    set obs 1
    replace file = "no input folders missing files"
}
outsheet using "$output_dir/input_folders_missing_files.csv", comma replace
restore

** format source variable
replace source = "DHS IN" if source == "DHS-OTHER/In-depth"
replace source = "DHS SP" if source == "DHS-OTHER/Special"
replace source = "DHS" if source == "DHS_TLS"
replace source = "IFHS" if source == "IRQ IFHS"
replace source = "IRN HH SVY" if source == "IRN DHS"
replace source = "TLS2003" if source == "East Timor 2003"
replace source = "MICS3" if source == "MICS"

** keep appropriate variables and convert everything to q-space
replace p_nn = p_enn*p_lnn if p_nn == .
gen p_inf = p_nn*p_pnn
gen p_ch = p_1p1*p_1p2*p_1p3*p_1p4
gen p_u5 = 1-q5
egen deaths_u5 = rowtotal(death_count*)
keep country year sex source source_y p_enn p_lnn p_nn p_pnn p_inf p_ch p_u5 deaths_u5 survey_year
rename country iso3
order iso3 year sex source source_y p_enn p_lnn p_nn p_pnn p_inf p_ch p_u5 deaths_u5

foreach age in enn lnn nn pnn inf ch u5 {
    replace p_`age' = 1-p_`age'
}
rename p_* q_*

** make consistent the data for males, females, and both
duplicates drop
bysort iso3 year source_y: egen count = count(year)
assert count <= 3
gen withinsex = 1 if count < 3
replace withinsex = 0 if withinsex == .
drop count
isid iso3 source_y year sex
tempfile cbh
save `cbh', replace

use `codes', clear
rename local_id_2013 iso3
drop if iso3 == ""
drop if iso3=="NA"
merge 1:m iso3 using `cbh'
drop if _m == 1
drop if _m == 1
drop _m
replace ihme_loc_id = iso3 if ihme_loc_id == ""
save `cbh', replace
merge m:1 ihme_loc_id using `codes'
drop if ihme_loc_id == "IND_4637" | ihme_loc_id == "IND_4638"

preserve
keep if _m == 1
if (_N == 0) {
    set obs 1
    replace ihme_loc_id = "ALL IHME_LOC_ID MERGED FROM CBH"
}
outsheet using "$output_dir/input_data_no_ihme_loc_id.csv", comma replace
restore

keep if _m == 3
drop _m local_id_2013 iso3 region_name

** save CBH data
tempfile cbh
save `cbh', replace

** *************************
** Compile VR deaths
** *************************

** prep COD VR file
    import delimited using "$cod_vr_file", clear
    gen subdiv = ""

    gen deaths_age = .
    ** <1
    replace deaths_age = 2 if age_group_id == 28
    ** NN
    replace deaths_age = 90 if age_group_id == 42
    ** EN
    replace deaths_age = 91  if  age_group_id == 2
    ** LN
    replace deaths_age = 93 if  age_group_id == 3
    ** PN
    replace deaths_age = 94 if  age_group_id == 4
    ** 1-4
    replace deaths_age = 3 if  age_group_id == 5
    forvalues i = 6/20 {
        replace deaths_age = `i' + 1 if age_group_id == `i'
    }
    ** 80-84
    replace deaths_age = 22 if age_group_id == 30
    ** 85-89
    replace deaths_age = 23 if age_group_id == 31
    ** 90-94
    replace deaths_age = 24 if age_group_id == 32
    ** 95+
    replace deaths_age = 25 if age_group_id == 235
    drop if missing(deaths_age)
    drop age_group_id

    rename nid NID
    rename year_id year
    rename sex_id sex
    reshape wide deaths, i(NID source location_id year sex subdiv) j(deaths_age)

    collapse (sum) deaths*, by(location_id year source subdiv sex)
    tempfile cod_vr_col
    save `cod_vr_col'

    keep location_id sex subdiv source year deaths2 deaths3 deaths91 deaths93 deaths94 deaths90

    summ year
    local yearmax = r(max)

    merge m:1 location_id using `codes2', keep(3) assert(2 3) nogen
    replace location_id = 152 if ihme_loc_id == "SAU"
    drop if substr(ihme_loc_id, 1, 4) == "SAU_"

    merge m:1 ihme_loc_id using `codes'

    keep if _m == 3
    drop _m

    gen source_type = "VR"
    replace source_type = "DSP" if substr(ihme_loc_id,1,3) == "CHN" & (source == "China_2004_2012" | source == "China_1991_2002")
    drop if inlist(subdiv, "A30","A10","A30.","A70","A80")
    drop if ihme_loc_id == "QAT" & (subdiv != "" & subdiv != ".")
    drop if inlist(source,"UK_deprivation_1981_2000","UK_deprivation_2001_2012")
    drop if ihme_loc_id == "MAR" & source != "ICD10"
    replace subdiv = "" if inlist(subdiv,"A20",".","..")
    gen loc_substr = substr(ihme_loc_id,-4,4)
    replace subdiv = "" if loc_substr == subdiv
    replace loc_substr = substr(ihme_loc_id,-3,3)
    replace subdiv = "" if loc_substr == subdiv
    replace subdiv = "" if inlist(subdiv,"East Midlands","Scotland","South East","South West" ,"Stockholm county","Sweden excluding Stockholm county","Wales","East of England") & strlen(ihme_loc_id) > 3
    replace subdiv = "" if inlist(subdiv,"London","North East","North West","Northern Ireland","West Midlands","Yorkshire and The Humber") & strlen(ihme_loc_id) > 3
    replace subdiv = "" if inlist(subdiv,"Aichika","Akita","Aomori","Chiba","Eastern Cape", "Ehime", "Free State") & strlen(ihme_loc_id) > 3
    replace subdiv = "" if inlist(subdiv, "Fukui", "Fukuoka","Fukushima","Gauteng") & strlen(ihme_loc_id) > 3
    replace subdiv = "" if inlist(subdiv,"Gifu","Gumma","Hiroshima","Hokkaido","Hyogo","Ibaraki","Ishikawa") & strlen(ihme_loc_id) > 3
    replace subdiv = "" if inlist(subdiv,"Iwate","Kagawa","Kagoshima","Kanagawa") & strlen(ihme_loc_id) > 3
    replace subdiv = "" if inlist(subdiv,"Kochi","Kumamoto","KwaZulu-Natal","Kyoto","Limpopo","Mie") & strlen(ihme_loc_id) > 3
    replace subdiv = "" if inlist(subdiv,"Miyagi","Miyazaki","Mpumalanga","Nagano") & strlen(ihme_loc_id) > 3
    replace subdiv = "" if inlist(subdiv,"Nagasaki","Nara","Niigata","Northern Cape","Oita","Okayama") & strlen(ihme_loc_id) > 3
    replace subdiv = "" if inlist(subdiv,"Okinawa","Osaka","Saga","Saitama","Shiga","Shimane","Shizuoka") & strlen(ihme_loc_id) > 3
    replace subdiv = "" if inlist(subdiv,"Tochigi","Tokushima","Tokyo","Tottori","Toyama","Wakayama") & strlen(ihme_loc_id) > 3
    replace subdiv = "" if inlist(subdiv,"Western Cape","Yamagata","Yamaguchi","Yamanashi") & strlen(ihme_loc_id) > 3

    preserve
    keep if subdiv != ""
    if (_N == 0) {
        set obs 1
        replace subdiv = "NO NEW SUBDIV VALUES TO RESOLVE"
    }
    outsheet using "$output_dir/cod_vr_new_subdiv.csv", comma replace
    restore

    drop if subdiv != ""
    drop subdiv loc_substr

    duplicates tag ihme_loc_id year sex, gen(dup)
    drop if dup == 1 & (ihme_loc_id == "GEO" | ihme_loc_id == "VIR" | ihme_loc_id== "PHL")
    drop if source == "Russia_FMD_2012_2013" & year >=2012
    drop if source == "Russia_FMD_1999_2011" | source == "Russia_ROSSTAT"
    drop if ihme_loc_id =="MNG" & (source=="ICD9_BTL" | source=="Mongolia_2004_2008" | source=="Other_Maternal") & (year==1994 | year==2006 | year==2007)
    drop if dup==1 & source == "Iran_collaborator_ICD10"

    drop dup

    sort ihme_loc_id year sex
    quietly by ihme_loc_id year sex:  gen dup = cond(_N==1,0,_n)
    drop if dup !=0
    drop dup

    save "$output_dir/as_debug.dta", replace
    isid ihme_loc_id year sex

    replace deaths2 = deaths91 + deaths93 + deaths94 if deaths91!=. & deaths93!=. & deaths94!=.
    gen deaths_enn = deaths91 
    gen deaths_lnn = deaths93 
    gen deaths_pnn = deaths94

    ** neonatal-post-neonatal split
    gen deaths_nn = deaths91 + deaths93
    replace deaths_nn = deaths90 if missing(deaths91) | missing(deaths93)

    ** infant-child split
    gen deaths_inf = deaths2
    gen deaths_ch  = deaths3

    ** format sex variable
    tostring sex, replace
    replace sex = "male" if sex == "1"
    replace sex = "female" if sex == "2"
    keep ihme_loc_id year sex deaths_* source_type source
    rename source cod_source
    compress

    replace source_type = "DSP3" if source_type == "DSP" & year >=2004
    replace source_type = "VR1" if source_type == "VR" & ihme_loc_id == "TUR" & year < 2009
    replace source_type = "VR2" if source_type == "VR" & ihme_loc_id == "TUR" & year >= 2009

    summ year
    local yearmax = r(max)
    tempfile cod_vr
    save `cod_vr', replace

    ** aggregate
    tempfile add_nats
    local count = 0
    foreach lev in 6 5 4 {
        di "`lev'"
        import delimited "$input_dir/subnat_loc_map.csv", clear
        local exp = `yearmax' - 1949
        expand `exp'
        bysort ihme_loc_id: gen year = 1949 + _n

        merge 1:m ihme_loc_id year using `cod_vr'
        drop if _m == 2
        assert deaths_ch != . if deaths_inf != .
        assert deaths_inf != . if deaths_ch != .

        keep if level == `lev'
        levelsof parent_id, local(parents)

        forvalues i = 1950/`yearmax' {
          foreach j of local parents {
            cap assert deaths_ch != . if parent_id == `j' & year == `i'
                if (_rc != 0) drop if parent_id == `j' & year == `i'
            }
        }

        foreach age in enn lnn pnn nn inf ch {
            gen miss_`age' = 1 if deaths_`age' == .
        }

        duplicates tag ihme_loc_id year sex source_type, gen(dup)
        drop if dup > 0 & cod_source != "Collapsed Subnat" & regexm(ihme_loc_id, "GBR")
        isid ihme_loc_id year sex source_type
        drop dup
            
        if `lev' != 4{
            collapse (sum) deaths* miss*, by(parent_id year sex source_type cod_source)
        }
        if `lev' ==4{
            collapse (sum) deaths* miss*, by(parent_id year sex source_type)
        }
            
        rename parent_id location_id
        foreach age in enn lnn pnn nn inf ch {
            replace deaths_`age' = . if miss_`age' != 0
        }

        if `lev'==4 gen cod_source = "Collapsed Subnat"
        if `lev' != 4 replace cod_source = "Collapsed Subnat"
        keep year cod_source sex location_id source_type deaths_enn deaths_lnn deaths_pnn deaths_nn deaths_inf deaths_ch
        tempfile tmp
        save `tmp', replace
        
        merge m:1 location_id using `codes2'
        keep if _m == 3
        drop location_id
        keep year cod_source sex ihme_loc_id source_type deaths_enn deaths_lnn deaths_pnn deaths_nn deaths_inf deaths_ch

        append using `cod_vr'
        save `cod_vr', replace
        local count = `count' + 1
        
    }
            
    duplicates tag ihme_loc_id year sex source_type, gen(dup)
    drop if dup > 0 & cod_source == "Collapsed Subnat"
    drop dup
    isid ihme_loc_id year sex source_type

    merge m:1 ihme_loc_id using `codes'
    keep if _m == 3
    drop _m
    keep year cod_source sex ihme_loc_id source_type deaths_enn deaths_lnn deaths_pnn deaths_nn deaths_inf deaths_ch
    
    ** save COD VR file
    rename deaths_inf deaths_inf_cod
    rename deaths_ch deaths_ch_cod
    save `cod_vr', replace

    ** load demographics VR data    
    use "$mort_vr_file", clear
    drop if ihme_loc_id == "GHA" & year == 2010 & source_type == "SURVEY"
    merge m:1 ihme_loc_id using `codes'
    keep if _m == 3 & year >= 1950
    drop _m region_name
    drop if regexm(deaths_footnote, "Fake number") == 1

    ** keep VR and SRS
    gen source_type1 = "SRS" if regexm(source_type, "SRS") == 1
    replace source_type1 = "VR" if regexm(source_type, "VR") == 1
    replace source_type1 = "DSP" if regexm(source_type, "DSP") == 1
    keep if source_type1 == "VR" | source_type1 == "SRS" | source_type1 == "DSP"
    drop source_type1

    ** get infant and child deaths
    gen deaths_inf = DATUM0to0
    gen deaths_ch = DATUM1to4
    drop if deaths_inf == . | deaths_ch == .
    keep ihme_loc_id year sex source_type deaths_source deaths_inf deaths_ch
    rename deaths_source mortality_source
    compress
    gen neonatal = 0

    ** merge better
    replace source_type = "VR" if source_type== "IRN_VR_post2003"  | source_type== "IRN_VR_pre2003"
    replace source_type = "VR" if source_type == "MEX_VR_post2011" | source_type== "MEX_VR_pre2011"
    replace source_type = "VR" if source_type == "VR_post2002" | source_type == "VR_pre2002"
    replace source_type = "VR2" if source_type=="VR3"

    ** merge COD and demographics VR files
    merge 1:1 ihme_loc_id year sex source_type using `cod_vr'
    drop if _m == 2 & (regexm(ihme_loc_id, "IND_") | regexm(ihme_loc_id, "CHN_"))
    replace deaths_nn = deaths_enn + deaths_lnn if deaths_enn != . & deaths_lnn != . & (deaths_nn <= ((deaths_lnn + deaths_enn)*1.02) | deaths_nn >= ((deaths_lnn + deaths_enn)*.98))

    assert deaths_inf_cod <= ((deaths_nn + deaths_pnn)*1.02) if deaths_nn != . & deaths_pnn != .
    assert deaths_inf_cod >= ((deaths_nn + deaths_pnn)*.98) if deaths_nn != . & deaths_pnn != .
    replace deaths_inf_cod = deaths_nn + deaths_pnn if deaths_nn != . & deaths_pnn != .

    replace deaths_enn =  . if (deaths_inf_cod > deaths_inf*1.05 | deaths_inf_cod < deaths_inf*.95) | (deaths_ch_cod > deaths_ch*1.05 | deaths_ch_cod < deaths_ch*.95)
    replace deaths_lnn =  . if (deaths_inf_cod > deaths_inf*1.05 | deaths_inf_cod < deaths_inf*.95) | (deaths_ch_cod > deaths_ch*1.05 | deaths_ch_cod < deaths_ch*.95)
    replace deaths_pnn =  . if (deaths_inf_cod > deaths_inf*1.05 | deaths_inf_cod < deaths_inf*.95) | (deaths_ch_cod > deaths_ch*1.05 | deaths_ch_cod < deaths_ch*.95)
    replace deaths_nn = . if (deaths_inf_cod > deaths_inf*1.05 | deaths_inf_cod < deaths_inf*.95) | (deaths_ch_cod > deaths_ch*1.05 | deaths_ch_cod < deaths_ch*.95)
    replace deaths_inf = deaths_inf_cod if deaths_nn != .
    replace deaths_ch = deaths_ch_cod if deaths_nn != .
            
    ** generate both sexes
    keep ihme_loc_id sex year source_type deaths_enn deaths_lnn deaths_pnn deaths_inf deaths_ch deaths_nn
    drop if sex == "both"
    tempfile temp
    save `temp', replace
    gen count = 1
    foreach var of varlist deaths* {
        replace `var' = -99999999 if `var' == .
    }
    collapse (sum) deaths* count, by(ihme_loc_id year source_type)
    keep if count == 2
    foreach var of varlist deaths* {
        replace `var' = . if `var' < 0
    }

    drop count
    gen sex = "both"
    append using `temp'
    bysort ihme_loc_id year: egen count = count(year)
    keep if count >= 3
    drop count
            
    ** identify age split
    gen type = 1 if deaths_enn != . & deaths_lnn !=. & deaths_pnn != . & deaths_ch != .
    replace type = 2 if deaths_nn !=. & deaths_pnn != . & deaths_ch != . & type == .
    replace type = 3 if deaths_inf != . & deaths_ch != . & type == .
    
    ** make consistent across sex
    bysort ihme_loc_id year sex source_type: egen temp = max(type)
    replace type = temp
    drop temp
    replace deaths_enn = . if type > 1
    replace deaths_lnn = . if type > 1
    replace deaths_nn = . if type > 2
    replace deaths_pnn = . if type > 2
    gen deaths_u5 = deaths_inf + deaths_ch
    
    ** format
    sort ihme_loc_id source_type sex year
    order ihme_loc_id source_type sex year type deaths*
    compress
    tempfile vr_deaths
    save `vr_deaths'


** *************************
** Compile populations
** *************************

    run "FILEPATH/get_population.ado"
    get_population, location_id(-1) sex_id("1 2 3") age_group_id("28 5") year_id("-1") location_set_id(22) status("latest") decomp_step("iterative") gbd_round_id("6") clear

    keep location_id sex_id year_id age_group_id population
    rename year_id year
    gen sex = "male" if sex_id==1
    replace sex = "female" if sex_id ==2
    replace sex = "both" if sex_id==3
    drop sex_id
    gen age_group_name = "ch" if age_group_id == 5
    replace age_group_name = "inf" if age_group_id == 28
    drop age_group_id
    rename pop pop_
    merge m:1 location_id using `codes2'
    keep if _m ==3
    drop _m
    drop location_id
    reshape wide pop_, i(ihme_loc_id sex year) j(age_group_name, string)
    tempfile natl_pop
    save `natl_pop', replace

    use "`birth_estimates'", clear
    drop if mi(ihme_loc_id)
    keep ihme_loc_id year sex births
    merge 1:1 ihme_loc_id sex year using `natl_pop'
    drop if _m!=3
    drop _m
    gen source_type = "VR"
    save `natl_pop', replace

    use "$all_pop_file", clear
    keep if source_type == "SRS" | ((ihme_loc_id == "PAK" | ihme_loc_id == "BGD") & source_type == "IHME") |  source_type == "DSP"
    replace source_type = "SRS" if (ihme_loc_id == "PAK" | ihme_loc_id == "BGD" & source_type == "IHME")
    gen pop_inf = c1_0to0
    gen pop_ch = c1_1to4
    keep ihme_loc_id year sex source_type pop*
    append using `natl_pop'
    gen mergesource = source_type
    tempfile all_pop
    save `all_pop', replace

** *************************
**  Calculate rates from VR/SRS
** *************************

** merge deaths and population
    use `vr_deaths', clear
    gen mergesource = source_type
    replace mergesource = "DSP" if regexm(source_type,"DSP") == 1
    replace mergesource = "SRS" if regexm(source_type,"SRS") == 1
    replace mergesource = "VR" if regexm(source_type,"VR") == 1
    drop if ihme_loc_id == "ZAF" & source_type == "VR-SSA"
    drop if ihme_loc_id == "TUR" & (source_type== "VR1" | source_type=="VR2")
    drop if ihme_loc_id == "KOR" & (source_type== "VR1" | source_type=="VR2")
    merge 1:1 ihme_loc_id year sex mergesource using `all_pop'

    keep if _m == 3
    drop _m mergesource

** calculate qx
    g q_enn = deaths_enn/births
    g q_lnn = deaths_lnn/(births-deaths_enn)
    g q_nn = 1-(1-q_enn)*(1-q_lnn)

    g m_inf = deaths_inf/pop_inf
    g m_ch = deaths_ch/pop_ch

** 1a0 (from Preston/Heuveline/Guillot demography book)
    gen ax_1a0=.
    replace ax_1a0 = 0.330 if sex=="male" & m_inf>=0.107
    replace ax_1a0 = 0.350 if sex=="female" & m_inf>=0.107
    replace ax_1a0 = (0.330 + 0.350)/2 if sex=="both" & m_inf>=0.107
    replace ax_1a0 = 0.045 + 2.684*m_inf if sex=="male" & m_inf<0.107
    replace ax_1a0 = 0.053 + 2.800*m_inf if sex=="female" & m_inf<0.107
    replace ax_1a0 = (0.045 + 2.684*m_inf + 0.053 + 2.800*m_inf)/2 if sex=="both" & m_inf<0.107

** 4a1 (from Preston/Heuveline/Guillot demography book)
    gen ax_4a1=.
    replace ax_4a1 = 1.352 if sex=="male" & m_inf>=0.107
    replace ax_4a1 = 1.361 if sex=="female" & m_inf>=0.107
    replace ax_4a1 = (1.352+1.361)/2 if sex=="both" & m_inf>=0.107
    replace ax_4a1 = 1.651-2.816*m_inf if sex=="male" & m_inf<0.107
    replace ax_4a1 = 1.522-1.518*m_inf if sex=="female" & m_inf<0.107
    replace ax_4a1 = (1.651-2.816*m_inf + 1.522-1.518*m_inf)/2 if sex=="both" & m_inf<0.107

    gen q_inf = 1*m_inf/(1+(1-ax_1a0)*m_inf)
    g q_ch = 4*m_ch/(1+(4-ax_4a1)*m_ch)
    g q_u5 = 1-(1-q_inf)*(1-q_ch)

    g q_pnn = 1 - (1-q_inf)/(1-q_nn)

    gen pop_5 = pop_inf + pop_ch
    keep ihme_loc_id sex year source_type q_enn q_lnn q_nn q_pnn q_inf q_ch q_u5 deaths_* pop_5
    order ihme_loc_id sex year source_type q_enn q_lnn q_nn q_pnn q_inf q_ch q_u5 deaths_* pop_5

** center year for VR and then save
    replace year = floor(year) + 0.5
    rename source_type source
    drop if q_inf == . & q_ch == . & q_u5 == .
    tempfile vr
    save `vr', replace

    use "FILEPATH", clear
    foreach var in q_enn q_lnn q_nn q_pnn q_inf q_ch q_u5 {
        cap gen `var' = .
    }
    foreach var in deaths_inf deaths_ch deaths_enn deaths_lnn deaths_pnn deaths_nn deaths_u5 pop_5 {
        cap gen `var' = 999999
    }
    tempfile add_agg_ests
    save `add_agg_ests', replace


** *************************
** Combine all sources
** *************************

    use `vr', clear
    append using `cbh'
    append using `add_agg_ests'
    drop if year < 1950

** *************************
** Mark exclusions
** *************************

    gen exclude = 0

** exclude CBH estimates more than 15 years before the survey
    destring year, replace
    destring survey_year, replace force
    replace exclude = 11 if year < survey_year - 15 & survey_year != .
    drop survey_year

    gen broadsource = source
    replace broadsource = "DSP" if regexm(source,"DSP") == 1
    replace broadsource = "VR" if regexm(source,"VR") == 1
    replace broadsource = "SRS" if regexm(source,"SRS") == 1

    preserve

    import delimited "$raw5q0_file", clear
    drop if ihme_loc_id == "BGD" & source == "SRS" & (year == 2002.5 | year == 2001.5)
    gen broadsource = source
    replace broadsource = "DSP" if regexm(source,"DSP") == 1
    replace broadsource = "VR" if regexm(source,"VR") == 1
    replace broadsource = "SRS" if regexm(source,"SRS") == 1
    keep if broadsource == "VR" | broadsource == "SRS" | broadsource == "DSP"
    duplicates drop ihme_loc_id year broadsource, force
    drop if outlier == 1 | shock == 1
    keep ihme_loc_id year source broadsource
    tempfile gpr_vr
    save `gpr_vr', replace

    ** find CBH we want to exclude because it's in a shock year
    insheet using "$raw5q0_file", clear
    keep if indirect == "direct"
    keep if shock == 1
    keep ihme_loc_id year source
    rename source source_y
    tempfile gpr_cbh_shock
    save `gpr_cbh_shock', replace

    ** find CBH surveys we want to exclude
    insheet using "$raw5q0_file", clear
    keep if indirect == "direct"
    keep if outlier == 1
    keep ihme_loc_id source
    rename source source_y
    duplicates drop
    tempfile gpr_cbh_survey
    save `gpr_cbh_survey', replace

    insheet using "$raw5q0_file", clear
    keep if indirect == "direct"
    keep if outlier != 1 & shock != 1
    keep ihme_loc_id source
    rename source source_y
    duplicates drop
    tempfile gpr_cbh_keep
    save `gpr_cbh_keep', replace
    restore

    merge m:1 ihme_loc_id year broadsource using `gpr_vr'
    drop if _m == 2
    replace exclude = 2 if _m == 1 & (broadsource == "VR" | broadsource == "SRS" | broadsource == "DSP")
    drop _m

    merge m:1 ihme_loc_id year source_y using `gpr_cbh_shock'
    drop if _m == 2
    replace exclude = 2 if _m == 3
    drop _m

    merge m:1 ihme_loc_id source_y using `gpr_cbh_survey'
    drop if _m == 2
    replace exclude = 2 if _m == 3
    drop _m

    merge m:1 ihme_loc_id source_y using `gpr_cbh_keep'
    drop if _m == 2
    replace exclude = 2 if _m == 1 & !regexm(source,"VR") & !regexm(source,"SRS") & !regexm(source,"DSP")
    drop _m
    compress

** Exclude any VR/SRS that is incomplete (calculate directly by comparing to GPR estimates)
    preserve
    keep if (broadsource == "VR" | broadsource == "SRS" | broadsource == "DSP") & sex == "both"
    keep ihme_loc_id year source broadsource q_u5
    tempfile incomplete
    save `incomplete', replace

    insheet using "$estimate5q0_file", clear
    keep if estimate_stage_id == 3
    merge m:1 location_id using `codes2', keep(3) assert(2 3) nogen
    rename mean q5med
    rename viz_year year
    cap destring q5med, replace force
    keep ihme_loc_id year q5med
    merge 1:m ihme_loc_id year using `incomplete'

    drop if _m == 1
    drop _m

    replace year = floor(year)
    summ year
    local min = `r(min)'
    local max = `r(max)'
    reshape wide q5med q_u5, i(ihme_loc_id source) j(year)
    order ihme_loc_id q5med* q_u5*
    forvalues y=`min'/`max' {
        local lower = `y' - 4
        if (`lower' < `min') local lower = `min'
        local upper = `y' + 4
        if (`upper' > `max') local upper = `max'
        egen temp1 = rowmean(q5med`lower'-q5med`upper')
        egen temp2 = rowmean(q_u5`lower'-q_u5`upper')
        gen avg_complete`y' = temp2 / temp1
        drop temp*
    }
    reshape long q5med q_u5 avg_complete, i(ihme_loc_id source) j(year)
    replace year = year + 0.5
    drop if q_u5 == .

    ** completenes for each individual year
    gen complete = q_u5/q5med

    gen keep = complete < 0.85
    replace keep = 0 if (avg_complete > 0.85)
    replace keep = 1 if (complete > 1.5 | complete < 0.5)

    keep if keep == 1
    keep ihme_loc_id year source
    save `incomplete', replace
    restore

    merge m:1 ihme_loc_id year source using `incomplete'
    replace exclude = 3 if _m == 3 & (broadsource == "VR" | broadsource == "SRS" | broadsource == "DSP")
    drop _m

** Manual exclusions
replace exclude = 8 if ihme_loc_id == "NGA" & year < 1975 & year > 1974
replace exclude = 8 if ihme_loc_id == "IND_43920" & year >= 1990 & year <= 1991
replace exclude = 8 if ihme_loc_id == "NGA" & exclude == 0 & year >= 1970 & year <= 1975
replace exclude = 8 if strmatch(source, "IND_human_dev*")

foreach age in enn lnn nn pnn inf ch u5 {
    gen exclude_`age' = exclude
    replace exclude_`age' = 12 if exclude==0 & q_`age' < 0.0001
}

replace exclude_lnn= 8 if ihme_loc_id == "GAB" & year ==1988.5 & (sex=="male" | sex=="both")
replace exclude_lnn= 8 if ihme_loc_id == "KAZ" & year ==1993 & (sex=="male" | sex=="both")
replace exclude_enn = 8 if exclude_enn==0 & ihme_loc_id=="BWA"
replace exclude_enn = 8 if exclude_enn==0 & ihme_loc_id=="ECU" & year<=1990
replace exclude_enn = 8 if exclude_enn==0 & ihme_loc_id=="IND_43885" & year<=1990 & sex=="female"
replace exclude_lnn = 8 if sex=="female" & ihme_loc_id =="IND_43900" & exclude_lnn==0 & source== "IND DHS urban rural"
replace exclude_ch = 8 if sex=="female" & ihme_loc_id =="IND_43900" & exclude_ch==0 & source=="VR" & year<1999
replace exclude_enn = 8 if ihme_loc_id =="MEX_4669" & year<1990 & year>1978 & exclude_enn ==0
replace exclude_enn = 8 if ihme_loc_id == "PAK" & year>=1985 & year<=2000 & exclude_enn==0 & (source== "PAK_IHS_1998-1999" | source=="PAK IHS 2001-2002")

gen exclude_sex_mod = 0
merge m:1 ihme_loc_id using `four_five_star'

replace exclude_sex_mod = 1 if _m==3 & source!="VR" & !regexm(source, "DSP")
drop _m
drop star

local identify = "ihme_loc_id year source source_y"

tempfile almost
save `almost', replace

keep if sex == "both"
tempfile tomerge
save `tomerge', replace

** ADD completeness variable by source comparing q_u5 to the 5q0 model estimate
insheet using "$input_dir/5q0_estimates.csv", clear
gen sex = "both"
rename year_id year
merge 1:m ihme_loc_id year using `tomerge'
sort ihme_loc_id sex year
by ihme_loc_id: ipolate mean year, gen(mean_interp) epolate
gen s_comp = q_u5/mean_interp
drop if _m == 1
drop mean_interp mean sex _m
keep `identify' s_comp
replace source_y = "999" if source_y == ""
isid `identify'
replace source_y = "" if source_y == "999"
expand 3
sort `identify'
by `identify': gen sex = _n
tostring sex, replace
replace sex = "male" if sex == "1"
replace sex = "female" if sex == "2"
replace sex = "both" if sex == "3"
merge 1:1 `identify' sex using `almost'
keep if _m==3 | (ihme_loc_id == "IND" & year > 2017)
drop _m

drop if q_enn == . & q_lnn == . & q_nn == . & q_pnn == . & q_inf == . & q_ch == . & q_u5 == .
replace exclude = 10 if withinsex == 1

** Label exclusions
label define exclude 0 "keep" 1 "low population" 2 "excluded from gpr" 3 "incomplete" 4 "implausible sex ratio" 5 "all zero" 6 "vr available" 7 "too few deaths" 8 "manual" 9 "implausible deaths/births" 10 "inadequate sex data" 11 "CBH before range" 12 "under 0.0001"
label values exclude exclude

** probability of dying in the early neonatal period; conditional on dying in the first five years
gen prob_enn = q_enn/q_u5

** probability of dying in the late neonatal period; conditional on dying in the first five years
gen prob_lnn = (1-q_enn)*q_lnn/q_u5

** probability of dying in the post-neonatal period; conditional on dying in the first five years
gen prob_pnn = (1-q_nn)*q_pnn/q_u5

** probability of dying in the infant period; conditional on dying in the first five years
gen prob_inf = q_inf/q_u5

** probability of dying in the child period; conditional on dying in the first five years
gen prob_ch = (1-q_inf)*q_ch/q_u5


** *************************
** Mark data types
** *************************

gen age_type = "inf/ch" if q_enn == . & q_lnn == . & q_pnn == .
replace age_type = "nn/pnn/ch" if q_enn == . & q_lnn == . & q_nn != . & q_pnn != .
replace age_type = "enn/lnn/pnn/ch" if q_enn != . & q_lnn !=. & q_pnn != .
count if age_type == ""
assert `r(N)' == 0
assert q_nn != . & q_pnn != . & q_ch != . if age_type == "nn/pnn/ch"
        
** *************************
** Save
** *************************

merge m:1 ihme_loc_id using `codes'
drop if _m == 2
drop _m local_id_2013

gen real_year = year
replace year = floor(year)
merge m:1 ihme_loc_id year sex using `natl_pop'
    keep if _m == 3
    drop _m year source_type
    rename real_year year
    
    order region_name ihme_loc_id year sex source age_type exclude q_* prob_* pop_* births
    replace source_y = "not CBH" if source_y == ""
    isid ihme_loc_id year sex source source_y
    replace source = source + "___" + source_y
    drop source_y

    use "$ddm_dir/data/d10_45q15.dta", clear
    keep ihme_loc_id year sex source_type comp
    keep if source_type == "VR"
    drop if comp == .
    isid ihme_loc_id year sex comp
    collapse (mean) comp, by(ihme_loc_id year)
    keep if comp >= .95
        drop comp
    replace year = year + .5
    tempfile adult_comp
    save `adult_comp', replace
restore

merge m:1 ihme_loc_id year using `adult_comp'
    
replace exclude = 0 if (exclude == 1 | exclude == 7) & regexm(source,"VR") & _m == 3
drop _m

drop if (ihme_loc_id == "IND_4841" | ihme_loc_id == "IND_4871") & source == "India SRS Statistical Reports 1995-2013___not CBH"

replace exclude_enn = 8 if ihme_loc_id == "ALB"
replace exclude_lnn = 8 if ihme_loc_id == "BHS"

foreach var of varlist exclude* {
    replace `var' = 0 if s_comp >= .75 & ihme_loc_id == "BLR" & s_comp != .
}

foreach var of varlist exclude* {
    replace `var' = 0 if broadsource == "VR" & ihme_loc_id == "BLZ"
}

foreach var of varlist exclude* {
    replace `var' = 8 if ihme_loc_id == "DMA"
}

replace exclude_lnn = 8 if ihme_loc_id == "ECU" & source == "ECU_ENSANUT_2012___ENSANUT 2012" & year >= 2010 & year <= 2011
replace exclude_enn = 8 if ihme_loc_id == "FJI"
replace exclude_lnn = 8 if ihme_loc_id == "FJI"
replace exclude_pnn = 8 if ihme_loc_id == "FJI"
replace exclude_lnn = 8 if ihme_loc_id == "UKR"
replace exclude_enn = 8 if ihme_loc_id == "GBR" & year >= 1980 & year < 1986
replace exclude_lnn = 8 if ihme_loc_id == "GBR" & year >= 1980 & year < 1986
replace exclude_pnn = 8 if ihme_loc_id == "GBR" & year >= 1980 & year < 1986
replace exclude_enn = 8 if ihme_loc_id == "PSE" & strmatch(source, "*DHS*") & exclude == 0
replace exclude_enn = 8 if ihme_loc_id == "UKR" & strmatch(source, "*CDC*") & exclude == 0 & year >= 1980 & year <= 2000
replace exclude_lnn = 8 if ihme_loc_id == "BRA_4751" & strmatch(source, "*DHS*") & exclude == 0 & year >= 1987 & year <= 1990
replace exclude_enn = 8 if ihme_loc_id == "BRA_4752" & strmatch(source, "*DHS*") & exclude == 0 & year >= 1990 & year <= 2000
replace exclude_enn = 8 if ihme_loc_id == "BRA_4759" & exclude == 0 & year >= 1980 & year <= 1986
replace exclude_enn = 8 if ihme_loc_id == "BRA_4762" & exclude == 0 & year >= 1986 & year <= 1990
replace exclude_enn = 8 if ihme_loc_id == "BRA_4769" & exclude == 0 & year >= 1987 & year <= 1990
replace exclude_enn = 8 if ihme_loc_id == "IND_43874" & exclude == 0 & year >= 1980 & year <= 1982
replace exclude_enn = 8 if ihme_loc_id == "IND_43901" & exclude == 0 & year >= 2000 & year <= 2005
replace exclude_lnn = 8 if ihme_loc_id == "IND_43906" & exclude == 0 & year >= 1998 & year <= 2004
replace exclude_enn = 8 if strmatch(ihme_loc_id, "MEX_*") & exclude == 0 & strmatch(source, "*ENADID*")
replace exclude_enn = 8 if ihme_loc_id == "NGA" & exclude == 0 & q_enn < 0.02
replace exclude = 8 if source == "SRS___not CBH" & year >=1997.5 & year <= 2016.5 & ihme_loc_id != "IND"
replace exclude = 8 if source == "SRS___not CBH" & year >= 2013.5 & year <= 2016.5 & ihme_loc_id == "IND"
replace exclude = 0 if ihme_loc_id == "IND" & year == 2017.5

foreach age in enn lnn nn pnn inf ch u5 {
  replace exclude_`age' = 8 if source == "SRS___not CBH" & year >=1997.5 & year <= 2016.5 & ihme_loc_id != "IND"
        replace exclude_`age' = 8 if source == "SRS___not CBH" & year >=2013.5 & year <= 2016.5 & ihme_loc_id == "IND"
        replace exclude_`age' = 0 if year == 2017.5 & ihme_loc_id == "IND"

    }

    replace exclude_enn = 8 if ihme_loc_id =="NIC" & year > 2000 & source=="NIC_DHS_ENDESA_2011-2012___ENDESA 2011-2012" 
    replace exclude = 8 if ihme_loc_id =="BOL" & year >2000 & source == "BOL_EDSA_2016_2016___BOL_EDSA_2016_2016"

    foreach age in enn lnn nn pnn inf ch u5 {
        replace exclude_`age' = 8 if ihme_loc_id =="BOL" & year >2000 & source == "BOL_EDSA_2016_2016___BOL_EDSA_2016_2016"
}

replace exclude_lnn = 8 if ihme_loc_id == "ZAF" & year==2004.25 & source=="ZAF_DHS_2016_2016___ZAF_DHS_2016_2016"

compress
saveold "$output_dir/input_data.dta", replace
export delimited "$output_dir/input_data.csv", replace
exit, clear STATA