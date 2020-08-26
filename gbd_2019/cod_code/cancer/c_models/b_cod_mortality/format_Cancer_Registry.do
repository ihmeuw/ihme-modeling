set more off

// Load and run set_common function based on the operating system
    local dir `c(pwd)'
    global cancer_repo = substr("`dir'", 1, strpos("`dir'", "cancer_estimation")-1) + "cancer_estimation"
    global staging_name = "cod_mortality"
    run "${cancer_repo}/stata_utils/set_common_roots.do" "staging" 
    local username = c(username)

// location of cod prep code
    **local cod_prep_code_location = <FILE PATH>
    local cod_prep_code_location = <FILE PATH>
 
** *****************************************************************************
** ESTABLISH DIRECTORIES
** *****************************************************************************
// input dataset
    global in_file "$cod_mortality_storage/final_output.csv"
// data_name (same as folder name)
    global data_name = "Cancer_Registry"
// input error folder
    global error_dir "$cod_mortality_staging_workspace/errors"
    make_directory_tree, path("$error_dir")
// output directory
    global out_dir <FILE PATH>
    // map directory
    global list_dir <FILE PATH>

// load and format data
    import delimited using "${in_file}", clear
    capture *_strip_labels_
    //
    capture rename nid NID
    keep iso3 subdiv location_id national NID sex year cause age deaths
    reshape wide deaths, i(iso3 subdiv location_id national NID sex year cause) j(age)
    
    // Verify that there are no duplicates 
    duplicates tag iso3 location_id year sex cause, gen(tag)
    count if tag != 0
    assert r(N) == 0
    drop tag

    // Source information. source refers to the CoD directory source folder, 
    // source_type is translated to a data_type_id in the CoD database 
    gen source = "Cancer_Registry"  
    gen source_type = "Cancer Registry"
    gen source_label = "Cancer_Registry"
    gen list =  "Cancer_Registry"
    gen cause_name = cause 
    gen frmat = 0
    gen im_frmat = 9
    gen deaths91 = deaths2
    foreach n of numlist 92/94 {
        gen deaths`n' = .
    }

  ** *****************************************************************************
** Run cod finalization scripts
** *****************************************************************************
// STANDARDIZE AGE GROUPINGS
    do "`cod_prep_code_location'/code/format_gbd_age_groups.do"
// EXPORT
    do "`cod_prep_code_location'/code/export.do" "$data_name"

 exit, STATA clear 
