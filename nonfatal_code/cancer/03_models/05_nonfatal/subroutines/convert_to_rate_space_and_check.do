*****************************************
** Description: Reformats final incidence and prevalence estimates, then
**          converts to rate space (#events/population) for upload into Epi

*****************************************
// accept arguments
    args data_col

// add population
    drop if inlist(age, 22, 27)
    merge m:1 location_id year sex age using "$population_data", keep(1 3) assert(2 3) nogen

// convert and check results
foreach i of numlist 0/999 {
    // Generate "draw" variable
    rename `data_col'`i' input_data_`i'
    gen draw_`i' = input_data_`i'/pop
    capture count if draw_`i' > 1 | draw_`i' < 0
    if r(N) {
        capture count if draw_`i' > 1
        if r(N)  noisily display in red "ALERT! rate greater than 1 has been detected in draw_`i'"
        capture count if draw_`i' < 0
        if r(N) noisily display in red "ALERT! rate less than 0 has been detected in draw_`i'"
        ** BREAK
        capture replace draw_`i' = 1 if draw_`i' > 1
        capture replace draw_`i' = 0 if draw_`i' < 0
    }
}

** *********************************************************
**
** *********************************************************
