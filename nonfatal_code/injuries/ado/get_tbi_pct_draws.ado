
cap program drop gen_tbi_pct_draws
program define gen_tbi_pct_draws
	version 12
	syntax , In_path(string) Out_dir(string)

	clear
	set more off, perm
	cap restore, not

	import delimited using "`in_path'", delimit(",") clear case(preserve)
	collapse (sum) n_N27 n_N28, by(healthstate) fast
	drop if healthstate == ""

	local tot_obs_N27 0
	local tot_obs_N28 0
	local healthstates
	forvalues x = 1/3 {
		local healthstate_`x' = healthstate[`x']
		local healthstates `healthstates' `healthstate_`x''
		foreach sev in N27 N28 {
			local n_`sev'_`x' = n_`sev'[`x']
			local tot_obs_`sev' = `tot_obs_`sev'' + `n_`sev'_`x''
		}
	}

	foreach sev in N27 N28 {
		clear
		set obs `tot_obs_`sev''
		gen healthstate = .
		local start_obs 1
		forvalues x = 1/3 {
			local end_obs = `start_obs' + `n_`sev'_`x'' - 1
			replace healthstate = `x' in `start_obs'/`end_obs'
			local start_obs = `end_obs' + 1
		}
		mlogit healthstate
		drawnorm `healthstates', n(1000) means(e(b)) cov(e(V)) clear seed(0)
		foreach x of local healthstates {
			gen value_`x' = exp(`x')
		}
		egen tot_val = rowtotal(value_*)
		foreach x of local healthstates {
			gen pred_`x' = value_`x' / tot_val
		}
		egen tot_pct = rowtotal(pred_*)
		assert tot_pct > .999 & tot_pct < 1.001

		keep pred_*
		gen drawnum = _n - 1
		tostring drawnum,replace
		replace drawnum = "draw_" + drawnum
		gen tmp = 1
		rename pred* _pred*
		reshape long _, i(drawnum) j(healthstate) string
		reshape wide _, i(healthstate) j(drawnum) string
		rename _* *
		replace healthstate = subinstr(healthstate,"pred_","",.)
		format draw* %16.0g
		export delimited using "`out_dir'/`sev'.csv", delimit(",") replace
	}
end
