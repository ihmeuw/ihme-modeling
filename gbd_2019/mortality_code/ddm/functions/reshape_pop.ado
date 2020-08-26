

cap program drop reshape_pop
program define reshape_pop

clear
set mem 500m
set more off


syntax, data(string) saveas(string) iso3(string)

g temp = .
tempfile saveastemp
save `saveastemp', replace

use "`data'", clear

if("`iso3'" ~= "all" & _N>0) {

	keep if ihme_loc_id == "`iso3'"
	*
    keep if (regexm(source_type, "CENSUS") & source_type != "CENSUS_NOT_USABLE" & source_type != "CENSUS_OLD") | source_type == "pop_registry" | (source_type == "DSP" & dsp_use_ddm == 1)
	
	* Make two locals: one containing censuses and the other containing pop registries
	preserve
	local has_census 0
	count if regexm(source_type, "CENSUS")
	if (r(N) != 0 & r(N) != 1){
		keep if (regexm(source_type, "CENSUS") & source_type != "CENSUS_NOT_USABLE" & source_type != "CENSUS_OLD")
		egen max_census_year = max(year)
		levelsof max_census_year, local(max_census)
		egen min_census_year = min(year)
		levelsof min_census_year, local(min_census)
		local has_census 1
	}
	restore
	
	* Drop if it's pop registry data and it's between the min and max census
	if (`has_census' == 1){
		drop if year > `min_census' & year < `max_census' & source_type == "pop_registry"
	}

	tempfile pop_data
	save `pop_data', replace
}

qui if(_N>0) { 
	replace ihme_loc_id = ihme_loc_id + "&&" + sex + "@@" + source_type 
	drop sex 
	duplicates drop *, force

	levelsof ihme_loc_id if regexm(ihme_loc_id, "CENSUS") | (regexm(ihme_loc_id, "DSP") & dsp_use_ddm == 1), local(ihme_loc_id_census)
	levelsof ihme_loc_id if regexm(ihme_loc_id, "pop_registry"), local(ihme_loc_id_pop_registry)
	tempfile pop_data
	save `pop_data', replace

	foreach c of local ihme_loc_id_census {
		noisily: di "`c'"
		preserve
		keep if ihme_loc_id == "`c'"
		levelsof precise_year, clean local(date)
		local wcdate = wordcount("`date'")			

		foreach d1 of local date {
			local mindist = 100000000
			local tempyear = 0 
			
			foreach d2 of local date {

				gen yr1 = "`d1'"
				gen yr2 = "`d2'"
				destring yr1, replace
				destring yr2, replace
				replace yr1 = floor(yr1)
				replace yr2 = floor(yr2)
				replace yr1 = yr1 - 1 if yr1 == 1931 & regexm(ihme_loc_id, "SVK") | (inlist(yr1, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
				replace yr2 = yr2 - 1 if yr2 == 1931 & regexm(ihme_loc_id, "SVK") | (inlist(yr2, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
				levelsof yr1, local(year1)
				levelsof yr2, local(year2)
	 			drop yr1 yr2

				levelsof pop_source if year == `year1', clean local(csource1)
				levelsof pop_source if year == `year2', clean local(csource2)

				if((`year2'-`year1') < `mindist' & (`year2'-`year1') > 0) {
					local mindist = `year2'-`year1'
					local tempyear = `year2'
				}
			}
				

			** Find the next closest census to the census taken on d1
			local mindist2 = 100000000
			foreach d2 of local date {
				
				gen yr1 = "`d1'"
				gen yr2 = "`d2'"
				destring yr1, replace
				destring yr2, replace
				replace yr1 = floor(yr1)
				replace yr2 = floor(yr2)
				replace yr1 = yr1 - 1 if (yr1 == 1931 & regexm(ihme_loc_id, "SVK")) | (inlist(yr1, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
				replace yr2 = yr2 - 1 if (yr2 == 1931 & regexm(ihme_loc_id, "SVK")) | (inlist(yr2, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
				levelsof yr1, local(year1)
				levelsof yr2, local(year2)
	 			drop yr1 yr2

				levelsof pop_source if year == `year1', clean local(csource1)
				levelsof pop_source if year == `year2', clean local(csource2)
				
				if((`year2'-`year1') < `mindist2' & (`year2'-`year1') > 0 & `tempyear' ~= `year2') {
					local mindist2 = `year2'-`year1'
				}
			}

			** start reshaping the data
			
			foreach d2 of local date {

				gen yr1 = "`d1'"
				gen yr2 = "`d2'"
				destring yr1, replace
				destring yr2, replace
				replace yr1 = floor(yr1)
				replace yr2 = floor(yr2)
				replace yr1 = yr1 - 1 if yr1 == 1931 & regexm(ihme_loc_id, "SVK") | (inlist(yr1, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
				replace yr2 = yr2 - 1 if yr2 == 1931 & regexm(ihme_loc_id, "SVK") | (inlist(yr2, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
				levelsof yr1, local(year1)
				levelsof yr2, local(year2)
	 			drop yr1 yr2
				levelsof pop_source if year == `year1', clean local(csource1)
				levelsof pop_source if year == `year2', clean local(csource2)	

				if(((`year2'-`year1') == `mindist' | (`year2'-`year1') == `mindist2')) {

					tempfile beforeshape
					save `beforeshape', replace
					keep if precise_year == "`d1'" | precise_year == "`d2'"
					levelsof pop_source if year == `year1', clean local(csource1)
					levelsof pop_footnote if year == `year1', clean local(fn1)
					levelsof pop_nid if year == `year1', clean local(nid1)
					levelsof underlying_pop_nid if year == `year1', clean local(underlying_nid1)
					levelsof pop_source if year == `year2', clean local(csource2)
					levelsof pop_footnote if year == `year2', clean local(fn2)
					levelsof pop_nid if year == `year2', clean local(nid2)
					levelsof underlying_pop_nid if year == `year2', clean local(underlying_nid2)

					
					drop pop_source pop_footnote pop_nid underlying_pop_nid precise_year

					reshape wide pop* agegroup*, i(ihme_loc_id) j(year)

					forvalues j = 0/100 {
						rename pop`j'`year1' pop1_`j'
						rename pop`j'`year2' pop2_`j'
					}
					forvalues j = 0/100 {
						rename agegroup`j'`year1' agegroup1_`j'
						rename agegroup`j'`year2' agegroup2_`j'
					}

					g pop_years = "`d1' `d2'"
					g pop_source = "`csource1' `csource2'"
					g pop_footnote = "`fn1'#`fn2'"
					g pop_nid = "`nid1' `nid2'"
					g underlying_pop_nid = "`underlying_nid1' `underlying_nid2'"
					tempfile aftershape
					save `aftershape', replace
				
					use `saveastemp', clear
					append using `aftershape'
					save `saveastemp', replace
				
					use `beforeshape', clear
				}
			}
		}
		restore
	}

		use `saveastemp', clear
		cap tostring pop_nid, replace
		cap tostring underlying_pop_nid, replace
		tempfile reshaped_census
		save `reshaped_census', replace

		clear
		g temp = .
		save `saveastemp', replace
		
		use `pop_data', clear
		
		if (`has_census' == 1){
			** Match pop registry data together before oldest census
			foreach c of local ihme_loc_id_pop_registry {
				noisily: di "`c'"
				preserve
				keep if ihme_loc_id == "`c'"
				keep if year <= `min_census'

				if (_N > 1){
					tempfile registry_data
					save `registry_data', replace

					select_registry_years, data("`registry_data'")
				} 
				else {
					restore
					continue, break
				}

				levelsof precise_year if year <= `min_census', clean local(date)
				local wcdate = wordcount("`date'")			

				foreach d1 of local date {
					local mindist = 100000000
					local tempyear = 0 
					
					** Determine which census to pair with d1 by finding the two closest censuses

					** Find the closest census to the census taken on d1
					foreach d2 of local date {

						gen yr1 = "`d1'"
						gen yr2 = "`d2'"
						destring yr1, replace
						destring yr2, replace
						replace yr1 = floor(yr1)
						replace yr2 = floor(yr2)
						replace yr1 = yr1 - 1 if yr1 == 1931 & regexm(ihme_loc_id, "SVK") | (inlist(yr1, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
						replace yr2 = yr2 - 1 if yr2 == 1931 & regexm(ihme_loc_id, "SVK") | (inlist(yr2, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
						levelsof yr1, local(year1)
						levelsof yr2, local(year2)
			 			drop yr1 yr2

						levelsof pop_source if year == `year1', clean local(csource1)
						levelsof pop_source if year == `year2', clean local(csource2)

						if((`year2'-`year1') < `mindist' & (`year2'-`year1') > 0) {
							local mindist = `year2'-`year1'
							local tempyear = `year2'
						}
					}
						

					** Find the next closest census to the census taken on d1
					local mindist2 = 100000000
					foreach d2 of local date {

						gen yr1 = "`d1'"
						gen yr2 = "`d2'"
						destring yr1, replace
						destring yr2, replace
						replace yr1 = floor(yr1)
						replace yr2 = floor(yr2)
						replace yr1 = yr1 - 1 if (yr1 == 1931 & regexm(ihme_loc_id, "SVK")) | (inlist(yr1, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
						replace yr2 = yr2 - 1 if (yr2 == 1931 & regexm(ihme_loc_id, "SVK")) | (inlist(yr2, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
						levelsof yr1, local(year1)
						levelsof yr2, local(year2)
			 			drop yr1 yr2

						levelsof pop_source if year == `year1', clean local(csource1)
						levelsof pop_source if year == `year2', clean local(csource2)
						
						if((`year2'-`year1') < `mindist2' & (`year2'-`year1') > 0 & `tempyear' ~= `year2') {
							local mindist2 = `year2'-`year1'
						}
					}

					** Now that we know which censuses to pair, start reshaping the data
					
					foreach d2 of local date {

						gen yr1 = "`d1'"
						gen yr2 = "`d2'"
						destring yr1, replace
						destring yr2, replace
						replace yr1 = floor(yr1)
						replace yr2 = floor(yr2)
						replace yr1 = yr1 - 1 if yr1 == 1931 & regexm(ihme_loc_id, "SVK") | (inlist(yr1, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
						replace yr2 = yr2 - 1 if yr2 == 1931 & regexm(ihme_loc_id, "SVK") | (inlist(yr2, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
						levelsof yr1, local(year1)
						levelsof yr2, local(year2)
			 			drop yr1 yr2
						levelsof pop_source if year == `year1', clean local(csource1)
						levelsof pop_source if year == `year2', clean local(csource2)	

						if(((`year2'-`year1') == `mindist' | (`year2'-`year1') == `mindist2')) {

							tempfile beforeshape
							save `beforeshape', replace
							keep if precise_year == "`d1'" | precise_year == "`d2'"
							levelsof pop_source if year == `year1', clean local(csource1)
							levelsof pop_footnote if year == `year1', clean local(fn1)
							levelsof pop_nid if year == `year1', clean local(nid1)
							levelsof underlying_pop_nid if year == `year1', clean local(underlying_nid1)
							levelsof pop_source if year == `year2', clean local(csource2)
							levelsof pop_footnote if year == `year2', clean local(fn2)
							levelsof pop_nid if year == `year2', clean local(nid2)
							levelsof underlying_pop_nid if year == `year2', clean local(underlying_nid2)

							
							drop pop_source pop_footnote pop_nid underlying_pop_nid precise_year

							reshape wide pop* agegroup*, i(ihme_loc_id) j(year)

							forvalues j = 0/100 {
								rename pop`j'`year1' pop1_`j'
								rename pop`j'`year2' pop2_`j'
							}
							forvalues j = 0/100 {
								rename agegroup`j'`year1' agegroup1_`j'
								rename agegroup`j'`year2' agegroup2_`j'
							}

							g pop_years = "`d1' `d2'"
							g pop_source = "`csource1' `csource2'"
							g pop_footnote = "`fn1'#`fn2'"
							g pop_nid = "`nid1' `nid2'"
							g underlying_pop_nid = "`underlying_nid1' `underlying_nid2'"
							tempfile aftershape
							save `aftershape', replace
						
							use `saveastemp', clear
							append using `aftershape'
							save `saveastemp', replace
						
							use `beforeshape', clear
						}
					}
				}		
			restore
		}

		use `saveastemp', clear
		
		cap tostring pop_nid, replace
		cap tostring underlying_pop_nid, replace

		tempfile reshaped_pop_registry_stage1
		save `reshaped_pop_registry_stage1', replace

		clear
		g temp = .
		save `saveastemp', replace
		
		use `pop_data', clear

			** Match pop registry data together after newest census
			foreach c of local ihme_loc_id_pop_registry {
				noisily: di "`c'"
				preserve
				keep if ihme_loc_id == "`c'"
				keep if year >= `max_census'

				if (_N > 1){
					tempfile registry_data
					save `registry_data', replace

					select_registry_years, data("`registry_data'")
				}
				else {
					restore
					continue, break
				}

				levelsof precise_year if year >= `max_census', clean local(date)
				local wcdate = wordcount("`date'")			

				foreach d1 of local date {
					local mindist = 100000000
					local tempyear = 0 
					
					** Determine which census to pair with d1 by finding the two closest censuses

					** Find the closest census to the census taken on d1
					foreach d2 of local date {

						gen yr1 = "`d1'"
						gen yr2 = "`d2'"
						destring yr1, replace
						destring yr2, replace
						replace yr1 = floor(yr1)
						replace yr2 = floor(yr2)
						replace yr1 = yr1 - 1 if yr1 == 1931 & regexm(ihme_loc_id, "SVK") | (inlist(yr1, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
						replace yr2 = yr2 - 1 if yr2 == 1931 & regexm(ihme_loc_id, "SVK") | (inlist(yr2, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
						levelsof yr1, local(year1)
						levelsof yr2, local(year2)
			 			drop yr1 yr2

						levelsof pop_source if year == `year1', clean local(csource1)
						levelsof pop_source if year == `year2', clean local(csource2)

						if((`year2'-`year1') < `mindist' & (`year2'-`year1') > 0) {
							local mindist = `year2'-`year1'
							local tempyear = `year2'
						}
					}
						

					** Find the next closest census to the census taken on d1
					local mindist2 = 100000000
					foreach d2 of local date {

						gen yr1 = "`d1'"
						gen yr2 = "`d2'"
						destring yr1, replace
						destring yr2, replace
						replace yr1 = floor(yr1)
						replace yr2 = floor(yr2)
						replace yr1 = yr1 - 1 if (yr1 == 1931 & regexm(ihme_loc_id, "SVK")) | (inlist(yr1, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
						replace yr2 = yr2 - 1 if (yr2 == 1931 & regexm(ihme_loc_id, "SVK")) | (inlist(yr2, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
						levelsof yr1, local(year1)
						levelsof yr2, local(year2)
			 			drop yr1 yr2

						levelsof pop_source if year == `year1', clean local(csource1)
						levelsof pop_source if year == `year2', clean local(csource2)
						
						if((`year2'-`year1') < `mindist2' & (`year2'-`year1') > 0 & `tempyear' ~= `year2') {
							local mindist2 = `year2'-`year1'
						}
					}

					** Now that we know which censuses to pair, start reshaping the data
					
					foreach d2 of local date {

						gen yr1 = "`d1'"
						gen yr2 = "`d2'"
						destring yr1, replace
						destring yr2, replace
						replace yr1 = floor(yr1)
						replace yr2 = floor(yr2)
						replace yr1 = yr1 - 1 if yr1 == 1931 & regexm(ihme_loc_id, "SVK") | (inlist(yr1, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
						replace yr2 = yr2 - 1 if yr2 == 1931 & regexm(ihme_loc_id, "SVK") | (inlist(yr2, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
						levelsof yr1, local(year1)
						levelsof yr2, local(year2)
			 			drop yr1 yr2
						levelsof pop_source if year == `year1', clean local(csource1)
						levelsof pop_source if year == `year2', clean local(csource2)	

						if(((`year2'-`year1') == `mindist' | (`year2'-`year1') == `mindist2')) {

							tempfile beforeshape
							save `beforeshape', replace
							keep if precise_year == "`d1'" | precise_year == "`d2'"
							levelsof pop_source if year == `year1', clean local(csource1)
							levelsof pop_footnote if year == `year1', clean local(fn1)
							levelsof pop_nid if year == `year1', clean local(nid1)
							levelsof underlying_pop_nid if year == `year1', clean local(underlying_nid1)
							levelsof pop_source if year == `year2', clean local(csource2)
							levelsof pop_footnote if year == `year2', clean local(fn2)
							levelsof pop_nid if year == `year2', clean local(nid2)
							levelsof underlying_pop_nid if year == `year2', clean local(underlying_nid2)

							
							drop pop_source pop_footnote pop_nid underlying_pop_nid precise_year

							reshape wide pop* agegroup*, i(ihme_loc_id) j(year)

							forvalues j = 0/100 {
								rename pop`j'`year1' pop1_`j'
								rename pop`j'`year2' pop2_`j'
							}
							forvalues j = 0/100 {
								rename agegroup`j'`year1' agegroup1_`j'
								rename agegroup`j'`year2' agegroup2_`j'
							}

							g pop_years = "`d1' `d2'"
							g pop_source = "`csource1' `csource2'"
							g pop_footnote = "`fn1'#`fn2'"
							g pop_nid = "`nid1' `nid2'"
							g underlying_pop_nid = "`underlying_nid1' `underlying_nid2'"
							tempfile aftershape
							save `aftershape', replace
						
							use `saveastemp', clear
							append using `aftershape'
							save `saveastemp', replace
						
							use `beforeshape', clear
						}
					}
				}		
			restore
		}

		use `saveastemp', clear
		cap tostring pop_nid, replace
		cap tostring underlying_pop_nid, replace

		append using `reshaped_pop_registry_stage1'
		append using `reshaped_census'
	} 
	if (`has_census' == 0) {
		noi di "in here"
			foreach c of local ihme_loc_id_pop_registry {
				noisily: di "`c'"
				preserve
				keep if ihme_loc_id == "`c'"
				if (_N > 1){
					tempfile registry_data
					save `registry_data', replace

					select_registry_years, data("`registry_data'")
				}

				levelsof precise_year, clean local(date)
				local wcdate = wordcount("`date'")			

				foreach d1 of local date {
					local mindist = 100000000
					local tempyear = 0 
					
					** Determine which census to pair with d1 by finding the two closest censuses

					** Find the closest census to the census taken on d1
					foreach d2 of local date {

						gen yr1 = "`d1'"
						gen yr2 = "`d2'"
						destring yr1, replace
						destring yr2, replace
						replace yr1 = floor(yr1)
						replace yr2 = floor(yr2)
						replace yr1 = yr1 - 1 if yr1 == 1931 & regexm(ihme_loc_id, "SVK") | (inlist(yr1, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
						replace yr2 = yr2 - 1 if yr2 == 1931 & regexm(ihme_loc_id, "SVK") | (inlist(yr2, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
						levelsof yr1, local(year1)
						levelsof yr2, local(year2)
			 			drop yr1 yr2

						levelsof pop_source if year == `year1', clean local(csource1)
						levelsof pop_source if year == `year2', clean local(csource2)

						if((`year2'-`year1') < `mindist' & (`year2'-`year1') > 0) {
							local mindist = `year2'-`year1'
							local tempyear = `year2'
						}
					}
						

					** Find the next closest census to the census taken on d1
					local mindist2 = 100000000
					foreach d2 of local date {

						gen yr1 = "`d1'"
						gen yr2 = "`d2'"
						destring yr1, replace
						destring yr2, replace
						replace yr1 = floor(yr1)
						replace yr2 = floor(yr2)
						replace yr1 = yr1 - 1 if (yr1 == 1931 & regexm(ihme_loc_id, "SVK")) | (inlist(yr1, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
						replace yr2 = yr2 - 1 if (yr2 == 1931 & regexm(ihme_loc_id, "SVK")) | (inlist(yr2, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
						levelsof yr1, local(year1)
						levelsof yr2, local(year2)
			 			drop yr1 yr2

						levelsof pop_source if year == `year1', clean local(csource1)
						levelsof pop_source if year == `year2', clean local(csource2)
						
						if((`year2'-`year1') < `mindist2' & (`year2'-`year1') > 0 & `tempyear' ~= `year2') {
							local mindist2 = `year2'-`year1'
						}
					}

					** Now that we know which censuses to pair, start reshaping the data
					
					foreach d2 of local date {

						gen yr1 = "`d1'"
						gen yr2 = "`d2'"
						destring yr1, replace
						destring yr2, replace
						replace yr1 = floor(yr1)
						replace yr2 = floor(yr2)
						replace yr1 = yr1 - 1 if yr1 == 1931 & regexm(ihme_loc_id, "SVK") | (inlist(yr1, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
						replace yr2 = yr2 - 1 if yr2 == 1931 & regexm(ihme_loc_id, "SVK") | (inlist(yr2, 1952, 1956, 1961, 1966, 1971) & regexm(ihme_loc_id, "GRL"))
						levelsof yr1, local(year1)
						levelsof yr2, local(year2)
			 			drop yr1 yr2
						levelsof pop_source if year == `year1', clean local(csource1)
						levelsof pop_source if year == `year2', clean local(csource2)	

						if(((`year2'-`year1') == `mindist' | (`year2'-`year1') == `mindist2')) {

							tempfile beforeshape
							save `beforeshape', replace
							keep if precise_year == "`d1'" | precise_year == "`d2'"
							levelsof pop_source if year == `year1', clean local(csource1)
							levelsof pop_footnote if year == `year1', clean local(fn1)
							levelsof pop_nid if year == `year1', clean local(nid1)
							levelsof underlying_pop_nid if year == `year1', clean local(underlying_nid1)
							levelsof pop_source if year == `year2', clean local(csource2)
							levelsof pop_footnote if year == `year2', clean local(fn2)
							levelsof pop_nid if year == `year2', clean local(nid2)
							levelsof underlying_pop_nid if year == `year2', clean local(underlying_nid2)

							
							drop pop_source pop_footnote pop_nid underlying_pop_nid precise_year

							reshape wide pop* agegroup*, i(ihme_loc_id) j(year)

							forvalues j = 0/100 {
								rename pop`j'`year1' pop1_`j'
								rename pop`j'`year2' pop2_`j'
							}
							forvalues j = 0/100 {
								rename agegroup`j'`year1' agegroup1_`j'
								rename agegroup`j'`year2' agegroup2_`j'
							}

							g pop_years = "`d1' `d2'"
							g pop_source = "`csource1' `csource2'"
							g pop_footnote = "`fn1'#`fn2'"
							g pop_nid = "`nid1' `nid2'"
							g underlying_pop_nid = "`underlying_nid1' `underlying_nid2'"
							tempfile aftershape
							save `aftershape', replace
						
							use `saveastemp', clear
							append using `aftershape'

							save `saveastemp', replace
						
							use `beforeshape', clear
						}
					}
				}		
			restore
		}

		use `saveastemp', clear
		
		cap tostring pop_nid, replace
		cap tostring underlying_pop_nid, replace
		append using `reshaped_census' 
	}
	
	if (_N>0) { 
		drop temp

		g id = ihme_loc_id 
		g sex = substr(ihme_loc_id,strpos(ihme_loc_id,"&&")+2,strpos(ihme_loc_id,"@@")-strpos(ihme_loc_id,"&&")-2)
		replace ihme_loc_id = substr(ihme_loc_id,1,strpos(ihme_loc_id,"&&")-1)
        recast str244 pop_footnote pop_source pop_nid underlying_pop_nid, force
	}
    
}
save "`saveas'", replace
di "DONE"
end
