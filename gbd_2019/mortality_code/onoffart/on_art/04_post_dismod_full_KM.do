// After running dismod, need to convert conditional probs into rates - and make graphs to make sure everything looks ok


clear all
set more off
if (c(os)=="Unix") {
	global root "ADDRESS"
}

if (c(os)=="Windows") {
	global root "ADDRESS"
}


// do you want to graph?

local compile_data=1

local graph_conditional=1
local graph_conditional_data=1
local graph_mortality_rates=1
local graph_cumulative=1
local graph_survival = 1

local color_none "102 194 165"
local color_high "252 141 98"
local color_ssa "141 160 203"
local color_other "231 138 195"
local color_ssabest "229 196 148"

local color1 "215 48 39"
local color2 "244 109 67"
local color3 "253 174 97"
local color4 "254 224 144"
local color5 "171 217 233"
local color6 "116 173 209"
local color7 "69 117 180"

local dismod_dir "FILEPATH"
local bradmod_dir "FILEPATH"
local graph_dir "FILEPATH"


// Initialize pdfmaker
if 0==0 {
	if c(os) == "Windows" {
		global prefix "$root"
		do "FILEPATH"
	}
	if c(os) == "Unix" {
		global prefix "FILEPATH"
		do "FILEPATH"
		set odbcmgr unixodbc
	}
}


if `compile_data' {

********* SAVE RAW DATA INPUTS AND FORMAT FOR GRAPHING ************

 	// JUST BEST SITES
	foreach per in 0_6 7_12 12_24 {
		
		insheet using "`dismod_dir'/HIV_KM_ssabest_`per'//data_in.csv", comma names clear
		keep if super=="ssabest"
		keep if pubmed_id!=.

		// this allows it to merge with other data
		gen dur="`per'"
		gen type="raw"
		rename meas_value mean_cond_prob
		destring mean_cond_prob, replace
		
					
		// graphing x-axis
		gen cd4_lower=age_lower*10
		gen cd4_upper=age_upper*10
		egen cd4_point=rowmean(cd4_lower cd4_upper)
		
		tempfile raw_ssabest_`per'
		save `raw_ssabest_`per'', replace	
	}

	// ALL SITES
	foreach sup in ssa high other {
		foreach per in 0_6 7_12 12_24 {
			
			insheet using "`dismod_dir'/HIV_KM_`sup'_`per'//data_in.csv", comma names clear
			keep if super=="none" | super=="`sup'"
			keep if pubmed_id!=.
			replace super="`sup'"
			
			// graphing x-axis
			gen cd4_lower=age_lower*10
			gen cd4_upper=age_upper*10
			egen cd4_point=rowmean(cd4_lower cd4_upper)

			// this allows it to merge with other data	
			gen dur="`per'"
			gen type="raw"
			rename meas_value mean_cond_prob
			destring mean_cond_prob, replace
			
			tempfile raw_`sup'_`per'
			save `raw_`sup'_`per'', replace	
		}
	}
	
	use `raw_ssa_0_6'
	append using `raw_ssa_7_12' `raw_ssa_12_24' `raw_other_0_6' `raw_other_7_12' `raw_other_12_24' `raw_high_0_6' `raw_high_7_12' `raw_high_12_24' `raw_ssabest_0_6' `raw_ssabest_7_12' `raw_ssabest_12_24'
	sort super
	order type super dur cd4_lower cd4_upper cd4_point pubmed_id
	sort type super dur 
	
	tempfile raw_data
	save `raw_data', replace
	
********* COMPILE PROBABILITY RESULTS *****

 
	******** BRING IN EACH OF THE OUTPUTS
	// keep last 1000
	// Merge together results (on draw# and period (duration))
	
	foreach sup in ssa ssabest other high {
	
		foreach per in 0_6 7_12 12_24 {
			
			insheet using "`dismod_dir'/HIV_KM_`sup'_`per'/model_draw2.csv", comma names clear 
			
/* 			insheet using "`dismod_dir'/HIV_KM_`sup'_`per'/sample_out.csv", comma names clear */
			
			drop if _n<=4000
			
/* 			rename iota_0 `sup'_0
			rename iota_5 `sup'_50
			rename iota_10 `sup'_100
			rename iota_20 `sup'_200
			rename iota_25 `sup'_250
			rename iota_35 `sup'_350
			rename iota_50 `sup'_500
			
			local count=0
			foreach var of varlist u_incidence* {
				sum `var'
				local mean=`r(mean)'
				if `mean'==0 {
					drop `var'
				}
				if `mean'!=0 {
					local count=`count'+1
					rename `var' u_incidence`count'
				}
			}
			
			// generate random number for the effect and row
			set seed 33
			gen random_randomeffect=floor(runiform()*(`count'))+1
			
			set seed 22
			gen random_row=floor(runiform()*1000)+1
			
			gen row_num=_n
			gen random_value=.
			


			forvalues row=1/1000 {
				forvalues randomeffect=1/`count' {
					sum u_incidence`randomeffect' if row_num==`row'
					local effect=`r(mean)'
					replace random_value=`r(mean)' if random_row==`row' & random_randomeffect==`randomeffect'
				}
			}
			
			
			// log transform estimate, and add randomly selected random effect
			foreach var of varlist `sup'* {
				replace `var'=log(`var')
				replace `var'=`var'+random_value
				replace `var'=exp(`var')
			} */
			
			
			keep `sup'*
			gen draw=_n
			gen dur="`per'"
			
			tempfile t`sup'_`per'
			save `t`sup'_`per'', replace
		
		}
		
		use `t`sup'_0_6', clear
		append using `t`sup'_7_12' `t`sup'_12_24'
		tempfile alldur_`sup'
		save `alldur_`sup'', replace
	
	}
	
	use `alldur_ssa', clear
	merge 1:1 dur draw using `alldur_ssabest', nogen
	merge 1:1 dur draw using `alldur_other', nogen
	merge 1:1 dur draw using `alldur_high', nogen
	

	foreach var of varlist _all {
			rename `var' prob`var'
		}
		rename probdur dur
		rename probdraw draw

		
	reshape long prob, i(draw dur) j(super_cd4) string
	reshape wide prob, i(super_cd4 dur) j(draw)
	
	// cd4 and super region
		split super_cd4, parse(_)
		rename super_cd41 super
		rename super_cd42 cd4_lower
		destring cd4_lower, replace
		drop super_cd4

		gen cd4_upper=50 if cd4_lower==0
		replace cd4_upper=100 if cd4_lower==50
		replace cd4_upper=200 if cd4_lower==100
		replace cd4_upper=250 if cd4_lower==200
		replace cd4_upper=350 if cd4_lower==250
		replace cd4_upper=500 if cd4_lower==350
		replace cd4_upper=1000 if cd4_lower==500
		
	
		
	// graphing x-axis
		egen cd4_point=rowmean(cd4_upper cd4_lower)
		
	// generate mean, high and low probability estimates
		egen mean_cond_prob=rowmean(prob*)
		egen low_cond_prob=rowpctile(prob*), p(2.5)
		egen high_cond_prob=rowpctile(prob*), p(97.5)
	
	
		order super dur cd4_lower cd4_upper cd4_point mean_cond_prob low_cond_prob high_cond_prob
		sort super dur cd4_lower cd4_upper cd4_point mean_cond_prob low_cond_prob high_cond_prob
		
		save "`bradmod_dir'/mortality_conditional_prob_output.dta", replace
		
		tempfile full_conditional_output
		save `full_conditional_output', replace


		
	******** CONVERT TO MORTALITY RATES ********

	use `full_conditional_output', clear
	
	
	forvalues i=1/1000 {
		gen rate`i'=.
		replace rate`i'=-(ln(1-prob`i'))/.5 if dur=="0_6"
		replace rate`i'=-(ln(1-prob`i'))/.5 if dur=="7_12"
		replace rate`i'=-(ln(1-prob`i'))/1 if dur=="12_24"
	}

	egen mean_rate=rowmean(prob*)
	egen low_rate=rowpctile(prob*), p(5)
	egen high_rate=rowpctile(prob*), p(95)
	
	order mean_rate
	
	tempfile mortality_rate_output
	save `mortality_rate_output', replace
	
	drop prob* *prob* mean* low* high*
	
	save "`bradmod_dir'/mortality_rate_output.dta", replace
	
	
}


********** GRAPH

if `graph_conditional' {

	********  CONDITIONAL PROBABILITIES ***************

	pdfstart using "`graph_dir'//Conditional_All_Regions.pdf"
		use `full_conditional_output', clear
		
		foreach duration in 0_6 7_12 12_24 {
		
			preserve
			sort cd4_point
			keep if dur=="`duration'"
			
			line mean_cond_prob cd4_point if super=="high",  lcolor(red) || ///
				line mean_cond_prob cd4_point if super=="other",  lcolor(blue) ||  ///
				line mean_cond_prob cd4_point if super=="ssa",  lcolor(green) ||  ///
				line mean_cond_prob cd4_point if super=="ssabest",  lcolor(orange)  ///
				title("HIV mort on ART: `duration' months") subtitle("DisMod output", size(medsmall)) ///
				ytitle("prob of death", size(small)) ///
				leg(lab(1 "High Income") lab(2 "Other") lab(3 "SSA") lab(4 "Best SSA") ///
				row(2) size(vsmall)) ///
				xtitle("CD4 count") ///
				ylabel(, labsize(vsmall) grid format(%12.0gc))
				
			pdfappend
			restore

		}
pdffinish	
} 


if `graph_conditional_data' {
	

	************ GRAPH CONDITIONAL PROBABILITIES AGAINST DATA
		// X-AXIS IS CD4 CATEGORY. Y-AXIS IS PROBABILITY OF DEATH. SEPARATE GRAPH FOR EACH TIME PERIOD AND REGION. RAW DATA SCATTERED.
		
	pdfstart using "`graph_dir'/Manuscript/Figure_3_4_5_Conditional_Data_IEDEA.pdf"
	foreach sup in ssa ssabest high other  {
		use `full_conditional_output', clear
		append using `raw_data'

		gen tag="IEDEA" if pubmed_id==20683318 | pubmed_id==18670668 | pubmed_id==21681057 | pubmed_id==22067665 | pubmed_id==22205933 | pubmed_id==19430306
		replace tag = "" if pubmed_id == 22205933 & iso3 == "CHE" // The Swiss data here is from SHCS -- the Zambia data from that study is IEDEA

		
		replace time_point=6 if dur=="0_6"
		replace time_point=12 if dur=="7_12"
		replace time_point=24 if dur=="12_24"
		keep if super=="`sup'"

		
 		if "`sup'"=="ssa" {
			local title_super "Sub-Saharan Africa" 
		}
		
		if "`sup'"=="ssabest" {
			local title_super "Best Sub-Saharan Africa" 
		} 
		
		if "`sup'"=="other" {
			local title_super "Other"
		}
		
		if "`sup'"=="high" {
			local title_super "High Income"
		} 
		

		sort cd4_point

		foreach point in  6 12 24 {
			preserve
			keep if time_point==`point'

		
			if "`point'"=="6" {
				local title_per "0-6 months"
			}
			
			if "`point'"=="12" {
				local title_per "7-12 months"
			}
			
			if "`point'"=="24" {
				local title_per "13-24 months"
			}
	
			
			twoway rarea low_cond_prob high_cond_prob cd4_point if type!="raw", color(blue*.1) || ///
				scatter mean_cond_prob cd4_point if type=="raw"  & cd4_lower==cd4_upper & tag=="", mcol(red)|| ///
				scatter mean_cond_prob cd4_point if type=="raw"  & cd4_lower==cd4_upper & tag=="IEDEA", mcol(green)|| ///
				rcap cd4_lower cd4_upper mean_cond_prob if type=="raw" & cd4_lower!=cd4_upper & tag=="", horizontal color(red) lwidth(vthin) || ///	
				rcap cd4_lower cd4_upper mean_cond_prob if type=="raw" & cd4_lower!=cd4_upper & tag=="IEDEA", horizontal color(green) lwidth(vthin) || ///	
				line mean_cond_prob cd4_point if type!="raw", lcolor(blue) ///
				xtitle("Initial CD4") ///
				ytitle("Conditional probability") ///
				ylabel( ,angle(0)) ///
				legend(off) ///
 				legend(lab (1 "95% CI") lab(2 "Raw - median") lab(3 "Raw - data from IeDEA") lab(4 "Raw - cd4-specific data")  lab(5 "Est conditional probability")  order(5 4 1 2 3)) /// 
				title("`title_per', `title_super'")
				// graph export "`graph_dir'/data_`point'_`sup'.eps", replace
		
/* 				ysc(r(0(.1).4)) ylab(0(.1).4) /// */
		
			pdfappend
			
			restore	
		
		}
	}
	
	pdffinish
	
	
	/// make legend GN 6/22: Is this even used??
	pdfstart using "`graph_dir'/Manuscript/Figure_3_4_5_legend.pdf"
	twoway 	line mean_cond_prob cd4_point if type!="raw", lcolor(blue) || ///
		rarea low_cond_prob high_cond_prob cd4_point, color(blue*.1) || ///
		scatter mean_cond_prob cd4_point if type=="raw"  & cd4_lower==cd4_upper & tag=="", mcol(red)|| ///
		scatter mean_cond_prob cd4_point if type=="raw"  & cd4_lower==cd4_upper & tag=="IEDEA", mcol(green)|| ///
		rcap cd4_lower cd4_upper mean_cond_prob if type=="raw" & cd4_lower!=cd4_upper & tag=="", horizontal color(red) lwidth(vthin) || ///	
		rcap cd4_lower cd4_upper mean_cond_prob if type=="raw" & cd4_lower!=cd4_upper & tag=="IEDEA", horizontal color(green) lwidth(vthin) ///
		legend( lab(1 "Estimated conditional probability") lab(2 "95% CI") lab(3 "Non-IeDEA median data") lab(4 "IeDEA median data") lab(5 "Non-IeDEA CD4 stratum data") lab(6 "IeDEA CD4 stratum data") order(1 2 4 3 6 5) symxsize(5))
	pdfappend
	pdffinish
	// graph export "`graph_dir'/Manuscript/Figure3_4_5_data_legend.eps", replace
	
		
}


if `graph_mortality_rates' {
	******* GRAPH MORTALITY RATES	
	pdfstart using "`graph_dir'/Mortality_Rate.pdf"
		
		use `mortality_rate_output', clear
		foreach duration in 0_6 7_12 12_24 {
			preserve
			sort cd4_point
			keep if dur=="`duration'"
			
			line mean_rate cd4_point if super=="high",  lcolor(red) lwidth(thick)  || ///
				line mean_rate cd4_point if super=="other",  lcolor(lime) lwidth(thick)  || ///
				line mean_rate cd4_point if super=="ssa",  lcolor(blue) lwidth(thick)   ///
				title("HIV mort on ART: `duration' months") subtitle("DisMod output", size(medsmall)) ///
				ytitle("prob of death", size(small)) ///
				leg( lab(1 "High Income") lab(2 "Latin America/Other") lab(3 "SSA")  ///
				row(1) size(vsmall)) ///
				xtitle("CD4 count") ///
				ylabel(, labsize(vsmall) grid format(%12.0gc))
			pdfappend
			
			restore
		}
	pdffinish


	
}
	

if `graph_cumulative' {
	
********** CUMULATIVE PROBABILITIES ************	
	
	use `full_conditional_output', clear
	
	order super
	
	split dur, parse(_)
	destring dur2, replace
	gen timepoint=dur2
	
	gen cumul_prob=mean_cond_prob if timepoint==6
	
	order super cd4_point timepoint cumul_prob mean_cond_prob
	
	sort super cd4_point timepoint
	bysort super cd4_point: replace cumul_prob=cumul_prob[_n-1]+((1-cumul_prob[_n-1])*mean_cond_prob) if timepoint==12
	bysort super cd4_point: replace cumul_prob=cumul_prob[_n-1]+((1-cumul_prob[_n-1])*mean_cond_prob) if timepoint==24
	
	levelsof cd4_point, local(cd4s)
	
	tempfile pre
	save `pre', replace

	pdfstart using "`graph_dir'/Cumulative_Mortality.pdf"
	foreach super in ssabest ssa high other  {
		use `pre', clear
		keep if super=="`super'"
		
		if "`super'"=="high" {
			local title "High Income"
		}
		
		if "`super'"=="other" {
			local title "Other"
		}
		
		if "`super'"=="ssa" {
			local title "Sub-Saharan Africa"
		}
		
		if "`super'"=="ssabest" {
			local title "Best Sub-Saharan Africa"
		}

		
		twoway connected cumul_prob timepoint if cd4_point==25, color("`color1'") || ///
			connected cumul_prob timepoint if cd4_point==75, color("`color2'") || ///
			connected cumul_prob timepoint if cd4_point==150 , color("`color3'") || ///
			connected cumul_prob timepoint if cd4_point==225 , color("`color4'")  || ///
			connected cumul_prob timepoint if cd4_point==300 , color("`color5'") || ///
			connected cumul_prob timepoint if cd4_point==425 , color("`color6'")  || ///
			connected cumul_prob timepoint if cd4_point==750, color("`color7'")  ///
			legend(off) ///
			title("`title'", size(huge)) ///
			ysc(r(0(.1).35)) ylab(0(.1).35, labsize(large) angle(0)) xlab(6 12 24, labsize(large)) xtitle("Months after initiation", size(large)) ytitle("Cumulative prob") xtitle("Months")
		/* 			legend(lab(1 "0-50") lab(2 "50-100") lab(3 "100-200")  lab(4 "200-250") lab(5 "250-350") lab(6 "350-500") lab(7 "500+") r(2) title("Initial CD4") symxsize(1)) /// */
			// graph export "`graph_dir'/cumulative_`super'.eps", replace
		
		pdfappend
	}
pdffinish

	pdfstart using "`graph_dir'/Cumulative_Mortality_byCD4.pdf"

	foreach cd4 in "0-50" "50-100" "100-200" "200-250" "250-350" "350-500" "500+"  {
		use `pre', clear
		tostring cd4_lower cd4_upper, replace
		gen cd4_category=cd4_lower+"-"+cd4_upper
		replace cd4_category="500+" if cd4_category=="500-1000"
		keep if cd4_category=="`cd4'"
		local title "CD4: `cd4'"

		
		twoway connected cumul_prob timepoint if super=="ssabest", color(orange) || ///
			connected cumul_prob timepoint if super=="ssa", color(blue) || ///
			connected cumul_prob timepoint if super=="high" , color(red) || ///
			connected cumul_prob timepoint if super=="other" , color(lime) ///
			legend(lab(1 "Best SSA") lab(2 "SSA") lab(3 "High Income")  lab(4 "Other") title("Region") symxsize(1)) ///
			title("`title'", size(huge)) ytitle("Cumulative prob of mortality") ///
			ysc(r(0(.1).35)) ylab(0(.1).35, labsize(large) angle(0)) xlab(6 12 24, labsize(large)) xtitle("Months after initiation", size(large)) xtitle("Months")
		
		pdfappend
	}
	pdffinish


}

if `graph_survival' {

	use `full_conditional_output', clear
	
	order super
	
	split dur, parse(_)
	destring dur2, replace
	gen timepoint=dur2
	
	gen cumul_prob=mean_cond_prob if timepoint==6
	
	order super cd4_point timepoint cumul_prob mean_cond_prob
	
	sort super cd4_point timepoint
	bysort super cd4_point: replace cumul_prob=cumul_prob[_n-1]+((1-cumul_prob[_n-1])*mean_cond_prob) if timepoint==12
	bysort super cd4_point: replace cumul_prob=cumul_prob[_n-1]+((1-cumul_prob[_n-1])*mean_cond_prob) if timepoint==24
	
	levelsof cd4_point, local(cd4s)
	
	keep super cd4_point timepoint cumul_prob 
	
	gen cumulative_survival=1-cumul_prob

	drop cumul_prob
	
	
	// exponential decay function: y(t) = a * (e^kt)
	
	// 6 months
	gen survival_0=1
	
	gen k=(ln(cumulative_survival))/6 if timepoint==6

	forvalues t=1/6 {
		gen survival_`t'=survival_0*(exp(k*`t')) if timepoint==6
	}
	
	
 	// 12 months
	replace k=(ln(((cumulative_survival)/(cumulative_survival[_n-1]))))/(6) if timepoint!=6
	
	forvalues realpoint=7/12 {
		local t=`realpoint'-6
		gen survival_`realpoint'=(cumulative_survival[_n-1])*(exp(k*`t')) if timepoint==12
	}
	
	forvalues realpoint=13/24 {
		local t=`realpoint'-12
		gen survival_`realpoint'=(cumulative_survival[_n-1])*(exp(k*`t')) if timepoint==24
	} 
	 
	
	// TEMPORARY - need to have dismod output these actually range estimates
	gen cd4_temp_point=25 if cd4_point==25 
	replace cd4_temp_point=125 if cd4_point==75 | cd4_point==150
	replace cd4_temp_point=275 if cd4_point==225 | cd4_point==300
	replace cd4_temp_point=425 if cd4_point==425 | cd4_point==750
	replace cd4_point=cd4_temp_point
	
	collapse survival*, by(super cd4_point)
	
	reshape long survival_, i(super cd4_point) j(months)
	
	// HERE YOU CAN GET STATS FOR PAPER
	preserve
	gen mort=1-survival
	replace mort=mort*100
	keep if months==6 | months==12 | months==24
	reshape wide mort survival_, i(cd4_point months) j(super) string
	gen dif_high_ssa=mortssa-morthigh
	gen gain_topssa=mortssa-mortssabest
	gen pct_gain_topssa=gain_topssa/dif_high_ssa
	restore
	
	
	pdfstart using "`graph_dir'/Manuscript/Figure6_Survival_CD4.pdf"
	
	foreach sup in ssa ssabest high other {
	
		if "`sup'"=="ssa" {
			local title "Sub-Saharan Africa"
		}
		
		if "`sup'"=="ssabest" {
			local title "Top-Performing Sub-Saharan Africa"
		}
	
		if "`sup'"=="high" {
			local title "High Income"
		}
		
		if "`sup'"=="other" {
			local title "Other (Latin America, Asia)"
		}

		preserve
		
		keep if super=="`sup'"
		
		twoway line survival months if cd4_point==25,  lpattern(dot) lwidth(medthick) lcolor(black) || ///
			line survival months if cd4_point==125, lpattern(dash) lwidth(medthick) lcolor(black*.5) || ///
			line survival months if cd4_point==275, lpattern(solid) lwidth(medthick) lcolor(black) || ///
			line survival months if cd4_point==425, lpattern(longdash_dot) lwidth(medthick) lcolor(black*.5) ///
			title("`title'") ///
			ytitle("Net proportion surviving") xtitle("Months since ART initiation") ///
			xlabel(0[6]24) ///
			ysc(r(0(.1)1)) ylab(0(.1)1, angle(0)) ///
			legend(lab(1 "0-50") lab(2 "50-200") lab(3 "200-350") lab(4 "350+") ///
			r(1))
		
		pdfappend
		
		restore
		
	}
	
		pdffinish
		
	// MORTALTIY NOT SURVIVAL
	pdfstart using "`graph_dir'/Manuscript/Figure6_Mortality_CD4.pdf"
	
	foreach sup in ssa ssabest high other {
	
		if "`sup'"=="ssa" {
			local title "Sub-Saharan Africa"
		}
		
		if "`sup'"=="ssabest" {
			local title "Top-Performing Sub-Saharan Africa"
		}
	
		if "`sup'"=="high" {
			local title "High Income"
		}
		
		if "`sup'"=="other" {
			local title "Other (Latin America, Asia)"
		}

		preserve
		
		gen mortality=1-survival
		keep if super=="`sup'"
		
		twoway line mortality months if cd4_point==25,  lpattern(dot) lwidth(medthick) lcolor(black) || ///
			line mortality months if cd4_point==125, lpattern(dash) lwidth(medthick) lcolor(black*.5) || ///
			line mortality months if cd4_point==275, lpattern(solid) lwidth(medthick) lcolor(black) || ///
			line mortality months if cd4_point==425, lpattern(longdash_dot) lwidth(medthick) lcolor(black*.5) ///
			title("`title'") ///
			ytitle("Net mortality") xtitle("Months since ART initiation") ///
			xlabel(0[6]24) ///
			ysc(r(0(.1).4)) ylab(0(.1).4, angle(0)) ///
			legend(lab(1 "0-50") lab(2 "50-200") lab(3 "200-350") lab(4 "350+") ///
			r(1))
		
		pdfappend
		
		restore
		
	}
	
		pdffinish
		

		
	// For chris' presentation at gates	
	pdfstart using "`graph_dir'/survival_props.pdf"
	foreach sup in ssa ssabest high other {
		if "`sup'"=="ssa" {
			local title "Sub-Saharan Africa"
		}
		
		if "`sup'"=="ssabest" {
			local title "Top-Performing Sub-Saharan Africa"
		}
	
		if "`sup'"=="high" {
			local title "High Income"
		}
		
		if "`sup'"=="other" {
			local title "Other (Latin America, Asia)"
		}

		preserve
		
		keep if super=="`sup'"
		
		
		twoway line survival months if cd4_point==25, lwidth(medthick) lcolor("161 39 33") || ///
			line survival months if cd4_point==125,lwidth(medthick) lcolor("243 110 66") || ///
			line survival months if cd4_point==275, lwidth(medthick) lcolor("254 190 16") || ///
			line survival months if cd4_point==425, lwidth(medthick) lcolor("3 106 55") ///
			title("`title'") ///
			ytitle("Net proportion surviving") xtitle("Months since ART initiation") ///
			xlabel(0[6]24) ///
			ysc(r(0(.2)1)) ylab(0(.2)1, angle(0)) ///
			legend(off)
		// graph export "`graph_dir'/survival_`sup'.eps", replace
		pdfappend
		restore
	}
	pdffinish
	
	
	pdfstart using "`graph_dir'/mortality_props.pdf"
	gen mortality=1-survival

	foreach sup in ssa ssabest high other {
		if "`sup'"=="ssa" {
			local title "Sub-Saharan Africa"
		}
		
		if "`sup'"=="ssabest" {
			local title "Top-Performing Sub-Saharan Africa"
		}
	
		if "`sup'"=="high" {
			local title "High Income"
		}
		
		if "`sup'"=="other" {
			local title "Other (Latin America, Asia)"
		}

		preserve
		keep if super=="`sup'"
		
		twoway line mortality months if cd4_point==25, lwidth(medthick) lcolor("161 39 33") || ///
			line mortality months if cd4_point==125,lwidth(medthick) lcolor("243 110 66") || ///
			line mortality months if cd4_point==275, lwidth(medthick) lcolor("254 190 16") || ///
			line mortality months if cd4_point==425, lwidth(medthick) lcolor("3 106 55") ///
			title("`title'") ///
			ytitle("Net proportion dead") xtitle("Months since ART initiation") ///
			xlabel(0[6]24) ///
			ysc(r(0(.1).4)) ylab(0(.1).4, angle(0)) ///
			legend(off)	
		// graph export "`graph_dir'/mortality_`sup'.eps", replace
		pdfappend
		restore
	}
	pdffinish

	pdfstart using "`graph_dir'/survival_legend.pdf"
	twoway line survival months if cd4_point==25, lwidth(medthick) lcolor("161 39 33") || ///
		line survival months if cd4_point==125,lwidth(medthick) lcolor("243 110 66") || ///
		line survival months if cd4_point==275, lwidth(medthick) lcolor("254 190 16") || ///
		line survival months if cd4_point==425, lwidth(medthick) lcolor("3 106 55") ///
		legend(lab(1 "0-50") lab(2 "50-200") lab(3 "200-350") lab(4 "350+") ///
		c(1) symxsize(4) title("Initial CD4"))
	// graph export  "`graph_dir'/survival_legend.eps", replace
	pdfappend
	pdffinish
	
}

