/*
Purpose:	Much quicker version of collapsing, including just appending summary rows
*/


capture program drop fastcollapse
program define fastcollapse

	syntax varlist [if], type(name) by(varlist) [append flag(name)]

	// check to make sure it's one of the supported collapse types
	if !inlist("`type'", "sum", "max", "min", "mean") {
		di as error "Type must be one of sum, max, min, or mean."
		break
	}

	// recast byte or int to float
	qui ds `varlist', has(type byte int)
	foreach var in `r(varlist)' {
	_recast float `var' 
	} 
	
	// store starting number of rows
	local fastcollapse_original_rows = _N
	mata {
		fastcollapse_original_rows = `fastcollapse_original_rows'
	}

	// create key variable
	tempvar fastcollapse_id
	egen `fastcollapse_id' = group(`by'), missing
	sort `fastcollapse_id'

	// find which rows to include
	mata fastcollapse_rows = range(1, fastcollapse_original_rows, 1)
	tempvar fastcollapse_include
	if "`if'" != "" {
		quietly generate `fastcollapse_include' = 1 `if'
		mata fastcollapse_tmp = .
		mata st_view(fastcollapse_tmp, ., "`fastcollapse_include'")
		mata fastcollapse_rows = select(fastcollapse_rows, fastcollapse_tmp :== 1)
	}

	mata {
	// load keys into mata
	fastcollapse_keys = .
	st_view(fastcollapse_keys, fastcollapse_rows, "`fastcollapse_id'")

	// load draws into mata
	fastcollapse_vars = .
	st_view(fastcollapse_vars, fastcollapse_rows, "`varlist'")

	// create panels
	fastcollapse_panel = panelsetup(fastcollapse_keys, 1)
	fastcollapse_num_panels = panelstats(fastcollapse_panel)[1]

	// create a new matrix to hold the results
	fastcollapse_out = J(panelstats(fastcollapse_panel)[1], cols(fastcollapse_vars), .)

	// loop through panels
	fastcollapse_subview = .
	for (i = 1; i <= fastcollapse_num_panels; i++) {
		// subset rows that have this set of keys
		panelsubview(fastcollapse_subview, fastcollapse_vars, i, fastcollapse_panel)
		// loop through columns
		c = .
		// sum up within column and assign to collapsed
		if ("`type'" == "sum") fastcollapse_out[i, c] = quadcolsum(fastcollapse_subview[., c])

		// find min within column and assign to collapsed
		if ("`type'" == "min") fastcollapse_out[i, c] = colmin(fastcollapse_subview[., c])

		// find max within column and assign to collapsed
		if ("`type'" == "max") fastcollapse_out[i, c] = colmax(fastcollapse_subview[., c])

		// find mean within column and assign to collapsed
		if ("`type'" == "mean") fastcollapse_out[i, c] = mean(fastcollapse_subview[., c])

	}

	// put the results back into stata
		// add blank observations to stata
		st_addobs(fastcollapse_num_panels)
		// update key view to include new keys
		st_view(fastcollapse_keys, ., "`fastcollapse_id'")
	} // switch back from mata to stata for the foreach loop...
		// fill in the keys
		foreach v of local by {
			mata {
				if (st_isnumvar("`v'")) {
					fastcollapse_id_var = .
					st_view(fastcollapse_id_var, fastcollapse_rows, "`v'")
					fastcollapse_this_id = .
					for (i = 1; i <= fastcollapse_num_panels; i++) {
						// find id variable for this key
						panelsubview(fastcollapse_this_id, fastcollapse_id_var, i, fastcollapse_panel)
						// store in stata
						st_store(fastcollapse_original_rows+i, "`v'", fastcollapse_this_id[1])
					}
				}
				else {
					fastcollapse_id_var = ""
					st_sview(fastcollapse_id_var, fastcollapse_rows, "`v'")
					fastcollapse_this_id = .
					for (i = 1; i <= fastcollapse_num_panels; i++) {
						// find id variable for this key
						panelsubview(fastcollapse_this_id, fastcollapse_id_var, i, fastcollapse_panel)
						// store in stata
						st_sstore(fastcollapse_original_rows+i, "`v'", fastcollapse_this_id[1])
					}
				}
			}
		}
		// fill in the collapsed values
		mata st_store((fastcollapse_original_rows+1..fastcollapse_original_rows+fastcollapse_num_panels)', st_varindex(tokens("`varlist'")), fastcollapse_out)
		// add a flag for collapsed if desired
		if "`flag'" != "" {
			generate `flag' = (_n > `fastcollapse_original_rows')
		}

	// not appending, clear out other observations and keep only used variables
	if "`append'" == "" {
		drop if _n <= `fastcollapse_original_rows'
		keep `varlist' `by' `flag'
	}
end

