
// NOTES: all of this is occuring at the 'draw level' (1000 draws)
	clear
	clear mata
	clear matrix
	set maxvar 15000
	
	if (c(os)=="Unix") {
		global root "ADDRESS"
	}

	if (c(os)=="Windows") {
		global root "ADDRESS"
	}

// set locals

	local bradmod_dir "FILEPATH"
	local hr_dir "FILEPATH" 
	local data_dir "FILEPATH"
	
// GOAL OUTPUT: 1000 marticies of death rates (per person year), at the REGION-DURATION-CD4-SEX-AGE-SPECIFIC level

// INPUT: region-duration-cd4-specific mortality estimates

	use "`bradmod_dir'/mortality_rate_output.dta",clear
	gen n=1
	
	gen merge_super=super
	replace merge_super="ssa" if super=="ssabest"
	

// STEP 1: generate region-duration-cd4-AGE-specific estimates
	// multiply the region-duration-cd4-specific mortality rates by the region-specific AGE hazard ratios by the region-duration-cd4-specific estimates (total number of 'cells' is multiplied by 4)

		expand=5
		bysort super dur cd4_lower: gen rownum=_n
		gen age=""
		replace age="15_25" if rownum==1
		replace age="25_35" if rownum==2
		replace age="35_45" if rownum==3
		replace age="45_55" if rownum==4
		replace age="55_100" if rownum==5
		order age
		
		merge m:1 merge_super age using "`hr_dir'/age_hazard_ratios.dta"
		
		sort super dur cd4_lower age
		order super dur cd4_lower age age_hr_1 age_hr_2
		
		forvalues i=1/1000 {
			qui	replace rate`i'=rate`i'*age_hr_`i'
		}
		
		drop age_hr* _m
		

		
// STEP 2: generate region-duration-cd4-age-SEX-specific estimates
	// use region-specific proportion male estimates to determine the region-specific male and female hazard ratios (total number of `cells' is multipled by 2)
	// algebraic logic: (overall death rate) = [(propmale)*(REGIONAL SEX HR)*(female death rate)]+[(1-propmale)*(female death rate)]
	// known are: (overall death rate); (prop male); (REGIONAL SEX HR)
	
	// solve for female death rate=(total death rate)/((REGIONAL SEX HR)(propmale)-(propmale)+(1))
	// and then the male death rate=female death rate*REGIONAL SEX HR
		
		** save sex ratio as csv then pull in and run in Stata 12 
		preserve
			insheet using "`hr_dir'/sex_hazard_ratios.csv", clear 
			gen merge_super=super
			tempfile tmp_sex_HRs
			save `tmp_sex_HRs', replace 
		restore
		
		merge m:1 merge_super using "`tmp_sex_HRs'"
		
		drop rownum
		expand=2 
		bysort super cd4_lower dur age: gen rownum=_n
		gen sex=""
		order super dur cd4_lower age sex 
		replace sex="female" if rownum==1
		replace sex="male" if rownum==2
		
		rename pct_male_weighted prop_male
		
		
		// Calculate female death rate for each draw
			forvalues drawnum=1/1000 {		

				qui replace rate`drawnum'=rate`drawnum'/(1+(prop_male*(sex_hr`drawnum'-1))) if sex=="female"
				qui order super cd4_lower dur age sex
				qui sort super cd4_lower dur age sex
				
				// calculate male rate based on female rate
				by super cd4_lower dur age: replace rate`drawnum'=sex_hr`drawnum'*rate`drawnum'[_n-1] if sex=="male"
				
			}
			
			
		
		drop rownum _m sex_hr* n prop_male raw*
		sort super dur cd4_lower cd4_upper sex age 
		order super dur cd4_lower cd4_upper sex age 
		drop cd4_point
		
// SAVE
	outsheet using "`data_dir'/adj_death_rates.csv", comma names replace

// FIGURE FOR MANUSCRIPT
	insheet using "`data_dir'/adj_death_rates.csv", comma names clear
	
	egen mean_rate=rowmean(rate*)
	egen low_rate=rowpctile(rate*), p(2.5)
	egen high_rate=rowpctile(rate*), p(97.5) 
	
	drop rate* merge_super
	
	// reshape wide mean_rate, i(cd4_lower cd4_upper sex age dur) j(super) string
	
	reshape wide mean_rate low_rate high_rate, i(super cd4_lower cd4_upper sex age) j(dur) string
	
	sort super sex age cd4_lower
	order super sex age cd4_lower cd4_upper mean_rate0_6 low_rate0_6 high_rate0_6 mean_rate7_12 low_rate7_12 high_rate7_12 mean_rate12_24 low_rate12_24 high_rate12_24
	
	// br if super=="ssa"
	
	