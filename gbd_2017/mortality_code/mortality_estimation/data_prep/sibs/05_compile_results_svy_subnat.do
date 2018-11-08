capture program drop compile_results_subnat
program define compile_results_subnat   
 
    ** *****************************************************************************************
    ** SET UP STATA                         
    ** *****************************************************************************************

    clear
    capture clear matrix
    set mem 50m
    set more off
    pause on

    local date = c(current_date)
    syntax , [iso3(string) year(string) nid(string) clear] 

    import delimited using "FILEPATH", clear

    levelsof svy, local(svys)
    tempfile all

    ** *****************************************************************************************
    ** 1. Append male and female results together                   
    ** *****************************************************************************************

    foreach s of local svys {

    use "FILEPATH", clear
    rename male_45q15 sib45q15
    rename male_lgt45q15 lgt_sib45q15
    rename sd_lgt45q15_1 sd_lgt45q15
    rename lb_lgt45q15_1 lb_lgt45q15
    rename ub_lgt45q15_1 ub_lgt45q15
    renpfix male_ ""
    tempfile male
    save `male', replace

    use "FILEPATH", clear
    rename female_45q15 sib45q15
    rename female_lgt45q15 lgt_sib45q15
    rename sd_lgt45q15_0 sd_lgt45q15
    rename lb_lgt45q15_0 lb_lgt45q15
    rename ub_lgt45q15_0 ub_lgt45q15
    renpfix female_ ""

    append using `male'
    gen svy = substr("`s'",1,.)
    capture append using `all'
    save `all', replace
    }

    ** *****************************************************************************************
    ** 2. Destring cy into iso code and year                        
    ** *****************************************************************************************

    gen yr = substr(svy_yr,-4,.)                                // Beginning of the 5 year period (ex. 1990: 1990-1994) 
    gsort sex svy -yr

    by sex svy: gen period = _n

    gen female = 1 - sex
    order svy female period sib45q15 lb_45q15 ub_45q15
    keep svy female period sib45q15 lb_45q15 ub_45q15

    dis "`nid'"
    generate nid = "`nid'"

    save "FILEPATH", replace 
    local date = c(current_date)
    save "FILEPATH", replace
end
