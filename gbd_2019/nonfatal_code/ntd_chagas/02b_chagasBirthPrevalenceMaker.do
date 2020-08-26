* do FILEPATH/ntd_chagas/01d_hfPrByAge.do

*** BOILERPLATE ***
    clear all
    set more off  

    if c(os) == "Unix" {
        local ADDRESS "FILEPATH"
        set odbcmgr ADDRESS
        }
        
    else if c(os) == "Windows" {
        local ADDRESS "FILEPATH"
        }

    run FILEPATH
    run FILEPATH
    
*** SET ENVIRONMENTAL LOCALS (PATHS, FILENAMES MODEL NUBMERS, ETC) ***
    local model ADDRESS
    
    tempfile locMeta pregTemp pregTempMaster mergingTemp appendTemp grs
    import delimited FILEPATH, clear
    
    gen nPreg = npreg
    keep if nPreg > 0
    levelsof age_group_id, local(ages) clean
    
    replace year_id = year_id + 1
    keep if inlist(year_id, 1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019)
    save `pregTemp'

    get_location_metadata, location_set_id(35) clear
    levelsof location_id if (strmatch(lower(region_name), "*latin america*") | inlist(ihme_loc_id, "BLZ", "GUY", "SUR")), local(locations) clean
    save `locMeta'
    
*** Georestrictions
    import delimited FILEPATH, clear
    drop nid year_end
    rename year_start year_id
    duplicates drop
    save `grs'
    
    local years 1990 1995 2000 2005 2010 2015 2017 2019
    get_draws, source(epi) age_group_id(`ages') gbd_id_type(modelable_entity_id) gbd_id(ADDRESS) year_id(`years') location_id(`locations') decomp_step(iterative) sex_id(2) measure_id(5) version_id(`model') clear
    
    merge 1:1 location_id year_id age_group_id using `pregTemp', gen(prPregMerge)
    *stata for left join
    keep if prPregMerge == 3
    
    merge m:1 location_id using `locMeta'
    keep if _merge == 3
    
    merge m:1 location_id year_id using `grs', gen(grMerge)
    keep if grMerge == 3
    
    keep if value_endemicity == 1 | is_estimate == 0 
    levelsof location_id, local(endemicLocations)

    // mean and SD here are from meta-analysis by Howard et al (doi: 10.1111/1471-0528.12396)
*** DERIVE PARAMETERS OF BETA DISTRIBUTION FOR RATE OF VERTICAL TRANSMISSION ***
    local mu    = 0.047  
    local sigma = (0.056 - 0.039) / (invnormal(0.975) * 2)
    local alpha = `mu' * (`mu' - `mu'^2 - `sigma'^2) / `sigma'^2 
    local beta  = `alpha' * (1 - `mu') / `mu' 
      
    forvalues i = 0/999 {
        local vertical    = rbeta(`alpha', `beta')  
        quietly gen bPrev_`i' = nPreg * draw_`i' * `vertical'
        }
    
    collapse (sum) nPreg bPrev_*, by(location_id ihme_loc_id location_name year_id) fast
    
    forvalues i = 0/999 {
        quietly replace bPrev_`i' = bPrev_`i' / nPreg
        }
 
*** FORMAT FOR EPI DATABASE ***
    egen mean  = rowmean(bPrev_*)
    egen upper = rowpctile(bPrev_*), p(97.5)
    egen lower = rowpctile(bPrev_*), p(2.5)

    generate int year_start = year_id
    rename year_id year_end

    drop bPrev_* nPreg 

    generate age_start = 0
    generate age_end = 0
    generate long nid = NID
    generate underlying_nid = .

    generate acause = "ntd_chagas"

    generate source_type = "Mixed or estimation"
    generate data_type = "Other"

    generate sample_size = .
    generate effective_sample_size = .
    generate standard_error = .
    generate cases = .

    generate sex = "Both"

    generate unit_type = "Person"
    generate unit_value_as_published = 1
    generate measure_adjustment = 0
    generate uncertainty_type_value = 95
    generate urbanicity_type = "Mixed/both"
    generate representative_name = "Nationally representative only"
    replace  representative_name = "Representative for subnational location only" if strmatch(ihme_loc_id, "*_*")
    generate recall_type = "Point"
    generate extractor = "modeled_NAME"
    generate is_outlier = 0
    generate cv_diag_mixed = 0
    generate cv_passive = 0
    generate smaller_site_unit = 0
    generate sex_issue = 0
    generate year_issue = 0
    generate age_issue = 0
    generate age_demographer = 0
    generate measure = "prevalence"
    generate measure_issue = 0
    generate response_rate = .
    generate note_SR = "estimated source"
    generate sampling_type = ""
    generate recall_type_value = .
    generate uncertainty_type = ""
    generate input_type = ""
    generate design_effect = .

    generate cv_blood_donor = 0
    generate cv_subn_endemic = 0


    generate seq = .

    export FILEPATH
    save  FILEPATH, replace
