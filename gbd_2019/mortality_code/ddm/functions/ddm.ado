cap program drop ddm
program define ddm

clear
set mem 500m
set more off
pause on

syntax, data(string) sex(integer) trim_specific(integer)


tempfile ggb
tempfile seg
tempfile ggbseg


set obs 1
g filename = "`data'"
levelsof filename, local(filename)

local count = 0

foreach f of local filename {

	di in red "GGB"
	tempfile ggb_`count'
	ggb, data("`f'")

	if("`data'" == "sims") {
		keep ihme_loc_id pop_years deaths_years VR_SOURCE SUBDIV C_* VR_* CENSUS_SOURCE time c1* c2* vr* diagnosticggb* orthog* completeness* vrmiss cmiss1 cmiss2 migrants_per_1000 cage1_2 cage1_3 cage2_2 cage2_3 vrage_2 vrage_3 vrc1miss vrc2miss avemiss
	}
	else {
		keep ihme_loc_id country pop_years deaths_years deaths_source source_type pop_footnote pop_source deaths_footnote time c1* c2* vr* diagnosticggb* orthog* completeness* agegroup* *nid
	}
	sort ihme_loc_id pop_years
	save `ggb_`count'', replace

	di in red "SEG"
	tempfile seg_`count'
	seg, data("`f'") sex(`sex')

	keep ihme_loc_id country pop_years deaths_years deaths_source source_type pop_footnote pop_source deaths_footnote time seg_avgcompleteness* diagnostic* agegroup* *nid
	sort ihme_loc_id pop_years
	save `seg_`count'', replace

	if(`trim_specific' == 0) {
		preserve
		use `ggb_`count'', clear
		g openend = .
		
		forvalues j = 0/100 {
			lookfor agegroup`j'
			return list
			if("`r(varlist)'" ~= "") {
				local maxnum = `j'
			}
		}

		local mplus = `maxnum'+1

		local maxnumminus = `maxnum'-1


		keep ihme_loc_id country pop_years deaths_years deaths_source source_type pop_footnote pop_source deaths_footnote c1_* c2_* vr_* completenessc1toc2_* time agegroup* *nid
		
		
		tempfile ggb_`count'_adj
		save `ggb_`count'_adj', replace

		restore
		tempfile ggbseg_`count'
		ggbseg, data(`ggb_`count'_adj') sex(`sex')
	
		forvalues j = 1/`maxnumminus' {
			local trim_upper = `maxnum'-`j'
			local upper = `trim_upper'-4
			forvalues trim_lower = 1/`upper' {	
				rename seg_avgcompleteness_`trim_lower'to`trim_upper' ggbseg_avgcompleteness_`trim_lower'to`trim_upper'
				rename diagnosticseg_`trim_lower'to`trim_upper' diagnosticggbseg_`trim_lower'to`trim_upper'
			}
		}
			
		keep ihme_loc_id country pop_years deaths_years deaths_source source_type pop_footnote pop_source deaths_footnote time ggbseg_avgcompleteness* diagnosticggbseg* agegroup* *nid
		sort ihme_loc_id pop_years
		save `ggbseg_`count'', replace

	}
	else {

		forvalues j = 0/100 {
			lookfor agegroup`j'
			return list
			if("`r(varlist)'" ~= "") {
				local maxnum = `j'
			}
		}

		local mplus = `maxnum'+1

		local maxnumminus = `maxnum'-1

		forvalues w = 1/`maxnumminus' {
			local trim_upper = `maxnum'-`w'
			local upper = `trim_upper'-4
			forvalues trim_lower = 1/`upper' {
				preserve
				use `ggb_`count'', clear

				forvalues j = 0/100 {
					lookfor agegroup`j'
					return list
					if("`r(varlist)'" ~= "") {
						local maxnum = `j'
					}
				}

				local mplus = `maxnum'+1

				local maxnumminus = `maxnum'-1

				keep ihme_loc_id country pop_years deaths_years deaths_source source_type pop_footnote pop_source deaths_footnote c1_* c2_* vr_* completenessc1toc2_* time agegroup* *nid
				
				forvalues j = 0/`maxnumminus' {
					replace c1_`j' = (c1_`j')/completenessc1toc2_`trim_lower'to`trim_upper' if c1_`j' ~= .
				}

				tempfile ggb_`count'_adj
				save `ggb_`count'_adj', replace
				
				restore

				ggbseg, data(`ggb_`count'_adj') sex(`sex')

				forvalues j = 0/100 {
					lookfor agegroup`j'
					return list
					if("`r(varlist)'" ~= "") {
						local maxnum = `j'
					}
				}

				local mplus = `maxnum'+1
		
				local maxnumminus = `maxnum'-1

				rename seg_avgcompleteness_`trim_lower'to`trim_upper' ggbseg_avgcompleteness_`trim_lower'to`trim_upper'
				rename diagnosticseg_`trim_lower'to`trim_upper' diagnosticggbseg_`trim_lower'to`trim_upper'
				keep ihme_loc_id pop_years ggbseg_avgcompleteness_`trim_lower'to`trim_upper' diagnosticggbseg_`trim_lower'to`trim_upper' agegroup* *nid
			
				sort ihme_loc_id pop_years
				tempfile ggbseg_`count'_`trim_lower'to`trim_upper'
				save `ggbseg_`count'_`trim_lower'to`trim_upper'', replace
			
				if(`trim_lower' == 1 & `trim_upper' == `maxnumminus') {
					tempfile ggbseg_`count'
					sort ihme_loc_id pop_years
					save `ggbseg_`count'', replace

				}	
				else {
					use `ggbseg_`count'', clear
					sort ihme_loc_id pop_years
					merge ihme_loc_id pop_years using `ggbseg_`count'_`trim_lower'to`trim_upper''
					tab _merge
					drop _merge
					save `ggbseg_`count'', replace
				}
			}
		}
	}

	local count = `count'+1
}

local count = `count'-1	

use `ggb_0', clear

forvalues t = 1(1)`count' {
	append using `ggb_`t''
}
sort ihme_loc_id pop_years

save `ggb', replace

use `seg_0', clear
forvalues t = 1(1)`count' {
	append using `seg_`t''
}
sort ihme_loc_id pop_years
save `seg', replace

use `ggbseg_0', clear
forvalues t = 1(1)`count' {
	append using `ggbseg_`t''
}
sort ihme_loc_id pop_years
save `ggbseg', replace

sort ihme_loc_id pop_years
merge ihme_loc_id pop_years using `ggb'
tab _merge
drop _merge

sort ihme_loc_id pop_years
merge ihme_loc_id pop_years using `seg'
tab _merge
drop _merge

end
