/******************************************************************************\    
    Zika Model

    Background: As a newly emerging infectious disease with relatively little data, Zika is a poor fit to be modeled in Dismod. 

    Purpose:    This script pulls toghether and manages the Zika data,
                and estimates incidence and proportions

    Pathway to run on cluster: do FILEPATH/ntd_zika/02_ageSexCurve.do

\******************************************************************************/

*** BOILERPLATE ***
clear all
set more off, perm
set maxvar 10000

if c(os) == "Unix" {
    local ADDRESS "FILEPATH"
    set odbcmgr ADDRESS
    }
else if c(os) == "Windows" {
    local ADDRESS "FILEPATH"
    }


*** LOAD SHARED FUNCTIONS ***
adopath + FILEPATH
adopath + FILEPATH

*** CREATE CONNECTION STRING TO SHARED DATABASE ***
run FILEPATH
create_connection_string, database(ADDRESS)
local shared = r(conn_string)
 
*** ESTABLISH LOCALS & TEMPFILES ***
local inFile FILEPATH     // incidence age-specific
local outFile FILEPATH    // age distribution

tempfile skeleton




/******************************************************************************\
                       CREATE AGE/SEX SKELETON
\******************************************************************************/

*** BRING IN LIST OF COD ESTIMATION AGE GROUPS ***
    odbc load, exec("SELECT age_group_id FROM age_group_set_list WHERE age_group_set_id = 12") `shared' clear
    levelsof age_group_id, local(ageList) clean
    save `skeleton'

*** BRING IN AGE GROUP DETAILS ***
    odbc load, exec("SELECT age_group_id, age_group_name, age_group_years_start AS age_start, age_group_years_end AS age_end FROM age_group") `shared' clear
    merge 1:1 age_group_id using `skeleton', keep(3) nogenerate

*** CREATE MALE & FEMALE OBSERVATIONS ***
    expand 2, generate(sex)
    replace sex = sex + 1
    generate sample_size = 1
    save `skeleton', replace




/******************************************************************************\
                    PULL IN AUTOCHTHONOUS ZIKA CASE DATA
\******************************************************************************/

*** IMPORT THE DATA & CLEAN UP ***
    import excel "`inFile'", sheet("Sheet1") firstrow clear

    egen study = group(value_filepath)

    replace sample_size = original_sample_size if missing(sample_size) & round(value_case/original_sample_size, 0.0001)==round(value_incidence_calculation, 0.0001)

    drop rei_id sequela_id covariate_id original_sample_size source_type iso3 not_population_data value_zotero_citation value_filepath
    keep measure location_id study sex year_start-age_end sample_size representative_name urbanicity_type case_diagnostics value_*

    gen incidence = value_case / sample_size

*** APPEND TO THE AGE/SEX SKELETON ***
    append using `skeleton'




/******************************************************************************\
                    PREP DATA FOR MODELLING 
\******************************************************************************/

    egen age_mid = rowmean(age_start age_end)
    gen sexC = (-1 * (sex==1)) + ((sex==2))

    egen group = group(location_id year_start year_end)
    replace group = 999 if missing(group)


    mkspline ageS = age_mid, cubic nknots(3)




/******************************************************************************\
            RUN MODEL AND GENERATE PREDICTED AGE/SEX DISTRIBUTION
\******************************************************************************/    

    menbreg value_case c.sexC##c.ageS* if incidence<0.1, exp(sample_size) || group:

    predict ageSexCurve, xb
    predict ageSexCurveSe, stdp




/******************************************************************************\
          EXPORT THE DATASET WITH THE PREDICTED AGE/SEX DISTRIBUTION
\******************************************************************************/

    keep if group==999    
    keep age_group_id sex ageSexCurve* 
    rename sex sex_id

    save "`outFile'", replace
