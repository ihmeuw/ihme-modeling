capture program drop calc_paf_categ 							
program define calc_paf_categ									
** Start with a dataset with exposure, rrs and tmreds and calculate PAFs. Dataset must be in long
** format for the category variables, and will collapse to a single row for the other id variables.
** Takes four stubs, one each to indicate the beginning of the exposure, rr and tmred  vars and one to 
** indicate the variable you want to store the PAFs in. Also needs a by statement, which tells the id
** variables (besides the variable indicating the category).
** PAF = (SUM(exp_i * RR_i) - SUM(tmred_i * RR_i)) / SUM(exp_i * RR_i)
syntax namelist(min=4 max=4), BY(varlist)
quietly {
	** Set up stubs
	local exp_stub : word 1 of `namelist'
	local rr_stub : word 2 of `namelist'
	local tmred_stub : word 3 of `namelist'
	local paf_stub : word 4 of `namelist'
	
	** Check that there are enough variables (and no paf variables already defined)
	unab looplist: `exp_stub'*
	local looplist = subinstr("`looplist'", "`exp_stub'", "", .)
	foreach varend of local looplist {
		confirm numeric var `rr_stub'`varend'
		confirm numeric var `tmred_stub'`varend'
		confirm new var `paf_stub'`varend'
	}

	** Calculate exp*RR and tmred*RR. Do this by replacing exposure and tmred to save time. (at the
	** expense of being able to give user back data in the event of error)
	foreach varend of local looplist {
		replace `exp_stub'`varend' = `exp_stub'`varend' * `rr_stub'`varend'
		replace `tmred_stub'`varend' = `tmred_stub'`varend' * `rr_stub'`varend'
	}
	drop `rr_stub'*
	
	** Add together the categories exp*rr and tmred*rr
	noisily display "Beginning exp*rr and tmred*rr addition collapse at `c(current_time)'"
	fastcollapse `exp_stub'* `tmred_stub'*, type(sum) by(`by')
	** collapse (sum) `exp_stub'* `tmred_stub'*, by(`by') fast
	noisily display "Finished  exp*rr and tmred*rr addition collapse at `c(current_time)'"
	
	** Calculate PAF
	foreach varend of local looplist {
		gen `paf_stub'`varend' = (`exp_stub'`varend' - `tmred_stub'`varend') / `exp_stub'`varend'
	}
	drop `exp_stub'* `tmred_stub'*
}

end


