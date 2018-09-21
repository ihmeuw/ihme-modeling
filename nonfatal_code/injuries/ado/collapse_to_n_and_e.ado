// ADO file that is used in the long-term matrices step. creates long-term matrices, e-code, and n-code prevalence
capture program drop collapse_to_n_and_e
program define collapse_to_n_and_e
	version 13
	syntax, code_dir(string) prefix(string) n_draw_outdir(string) e_draw_outdir(string) summ_outdir(string) output_name(string) ages(string) [LONGterm matrix_draw_outfile(string) matrix_summ_outfile(string) SHORTterm]

	set type double, perm
	
	preserve
		insheet using "`code_dir'/convert_to_new_age_ids.csv", comma names clear
		rename age_start age
		tostring age, replace force format(%12.2f)
		destring age, replace
		tempfile age_ids 
		save `age_ids', replace
	restore

// Collapse across platform
	fastcollapse draw*, type(sum) by(ecode ncode age inpatient)

// Add in parent categories
	//add_parent_ecode_draws ecode, by(ncode age)
	tempfile all_draws_by_plat
	save `all_draws_by_plat', replace

	fastcollapse draw*, type(sum) by(ecode ncode age)
	//add_parent_ecode_draws ecode, by(ncode age)
	tempfile all_draws
	save `all_draws', replace

// collapse over n_codes and save
	fastcollapse draw*, type(sum) by(age ncode)		
	
	** 0's are needed for N-codes that don't have outcomes for the timeframe of interest (short-term or long-term)
	tempfile justncodes
	save `justncodes', replace

	*get_ncode_names, prefix("`prefix'")
insheet using "`code_dir'/como_st_yld_mes.csv", comma names clear
keep n_code
drop if n_code == "N29"
duplicates drop
	
gen age = 999
local numages = wordcount("`ages'")
expand `numages'
local count = 0
foreach age of local ages {
bysort n_code: replace age = `age' if _n == _N - `count'
local count = `count' + 1
}
rename n_code ncode
merge 1:1 ncode age using `justncodes'
forvalues i=0/999 {
quietly replace draw_`i' = 0 if _m == 1
}
	drop _m

	
	** IF you selected longterm, save N to E matrices
	if !missing("`longterm'") {
		preserve

		gen denom = 1
		noisily di "APPENDING ALL_DRAWS"
		append using `all_draws'

		replace denom = 0 if denom == .
			count if draw_69 != . | draw_69 != 0
		noisily di "FASTFRACTION"
		fastfraction draw*, denominator(denom) by(ncode age) prefix(pct_)
		noisily di "FASTFRACTION DONE"
			count if draw_69 != . | draw_69 != 0
		drop if denom == 1
		drop draw* denom
		rename pct_* *
		order ncode ecode age, first
		sort_by_ncode ncode, other_sort(ecode age)
		format draw* %16.0g
			di "`matrix_draw_outfile'"
		merge m:1 age using `age_ids', assert(3) keep(3) nogen
		drop age
		outsheet using "`matrix_draw_outfile'", comma names replace
		
		fastrowmean draw*, mean_var_name("mean")
		fastpctile draw*, pct(2.5 97.5) names(ll ul)
		drop draw*
		outsheet using "`matrix_summ_outfile'", comma names replace
		
		restore
	}
	
// Use all draws by platform
use `all_draws_by_plat', clear
fastcollapse draw*, type(sum) by(age ncode inpatient)
tempfile justncodes_plat
save `justncodes_plat', replace

insheet using "`code_dir'/como_st_yld_mes.csv", comma names clear
keep n_code
drop if n_code == "N29"
duplicates drop

gen age = 999
local numages = wordcount("`ages'")
expand `numages'
local count = 0
foreach age of local ages {
bysort n_code: replace age = `age' if _n == _N - `count'
local count = `count' + 1
}
rename n_code ncode
merge 1:m ncode age using `justncodes_plat'
forvalues i=0/999 {
quietly replace draw_`i' = 0 if _m == 1
}
drop _m
merge m:1 age using `age_ids', keep(3) assert(3) nogen
drop age
tempfile all
save `all', replace

	levelsof ncode, local(fin_ns)

	** Generate tempfile to hold appended summary results
	tempfile summary
	
	** Loop through N-codes and save prevalence draws and append prevalence summary stats
	foreach ncode of local fin_ns {
		di "CREATING PREV: `ncode'"
		use `all', clear
		keep if ncode == "`ncode'"
		levelsof inpatient, l(platforms)
		tempfile n_plat_data
		save `n_plat_data', replace

		foreach plat of local platforms {
			use `n_plat_data', clear
			keep if inpatient == `plat'

		// Check if any results for this platform N-code (for example, there is no long-term prevalence from outpatient amputations). Don't write a missing file for this location/year/sex if it's an N-code we know will be missing
			drop ncode
			sort age

		// Save real file if not going to append N-codes together (which we can do for short-term)
			if !missing("`longterm'") {

				order age_group_id draw*
				local savedir "`n_draw_outdir'/`ncode'"
				cap mkdir "`savedir'"
				cap mkdir "`savedir'/`plat'"
				format draw* %16.0g
				drop inpatient
				outsheet using "`savedir'/`plat'/`output_name'.csv", comma names replace
			}
			else {
				tempfile prev_`n_code'
				save `prev_`n_code''
			}
		
		// summarize
			fastrowmean draw*, mean_var_name("mean")
			fastpctile draw*, pct(2.5 97.5) names(ll ul)
			drop draw*
			gen ncode = "`n_code'"
			
			cap confirm file `summary'
			if !_rc {
				append using `summary'
			}
			save `summary', replace
	}
}

	** Save summary stats file
	use `summary', clear
	order ncode age, first
	sort_by_ncode ncode, other_sort(age)
	cap mkdir "`summ_outdir'/N"
	outsheet using "`summ_outdir'/N/`output_name'.csv", comma names replace

	** If short-term, append to one file
	if !missing("`shortterm'") {
		clear
		tempfile appended_n_draws
		foreach n_code of local fin_ns {
			use `prev_`n_code'', clear
			gen acause = "_none"
			gen grouping = "ncodes"
			gen healthstate = "`n_code'"
			cap confirm file `appended_n_draws'
			if !_rc append using `appended_n_draws'
			save `appended_n_draws', replace
		}
			
		order acause grouping healthstate age, first
		sort_by_ncode healthstate, other_sort(age)
		format draw* %16.0g
		outsheet using "`n_draw_outdir'/`output_name'.csv", comma names replace
	}
		
	
// collapse over e_codes and save
	use `all_draws', clear
	fastcollapse draw*, type(sum) by(age ecode)
	
	** 0's are needed for N-codes that don't have outcomes for the timeframe of interest (short-term or long-term)
	tempfile justecodes
	save `justecodes', replace

	insheet using "`code_dir'/como_st_yld_mes.csv", comma names clear
	keep e_code
	duplicates drop
	
	rename e_code ecode
	gen age = 999
	local numages = wordcount("`ages'")
	expand `numages'
	local count = 0
	foreach age of local ages {
		bysort ecode: replace age = `age' if _n == _N - `count'
		local count = `count' + 1
	}

	merge 1:1 ecode age using `justecodes'
	forvalues i=0/999 {
		quietly replace draw_`i' = 0 if _m == 1
	}
	drop _m


	levelsof ecode, local(fin_es)

	** Generate tempfile to hold appended summary results
	tempfile summary
	
	** Loop through N-codes and save prevalence draws and append prevalence summary stats
	foreach e_code of local fin_es {
		preserve
		keep if ecode=="`e_code'"
		drop ecode
		sort age

			merge m:1 age using `age_ids', keep(3) nogen
			drop age 
		
	// Save real file if not going to append E-codes together (which we can do for short-term)
		if !missing("`longterm'") {
			local savedir "`e_draw_outdir'/`e_code'"
			cap mkdir "`savedir'"
			format draw* %16.0g
			order age_group_id
			outsheet using "`savedir'/`output_name'.csv", comma names replace
		}
		else {
			tempfile prev_`e_code'
			save `prev_`e_code''
		}
	
	// summarize
		fastrowmean draw*, mean_var_name("mean")
		fastpctile draw*, pct(2.5 97.5) names(ll ul)
		
		drop draw*
		gen ecode = "`e_code'"
		cap confirm file `summary'
		if !_rc {
			append using `summary'
		}
		save `summary', replace
		restore
	}
	
	** Save summary stats file
	use `summary', clear
	order ecode age, first
	sort ecode age
	cap mkdir "`summ_outdir'/E"
	outsheet using "`summ_outdir'/E/`output_name'.csv", comma names replace
	
	** If short-term, append to one file
	if !missing("`shortterm'") {
		clear
		tempfile appended_e_draws
		foreach e_code of local fin_es {
			use `prev_`e_code'', clear
			gen acause = "`e_code'"
			cap confirm file `appended_e_draws'
			if !_rc append using `appended_e_draws'
			save `appended_e_draws', replace
		}
			
		order acause age, first
		sort acause age
		format draw* %16.0g
		outsheet using "`e_draw_outdir'/`output_name'.csv", comma names replace
	}
end
