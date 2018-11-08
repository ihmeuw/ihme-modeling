
clear all
set more off
if (c(os)=="Unix") {
	global root "ADDRESS"
}

if (c(os)=="Windows") {
	global root "ADDRESS"
}


********* SETUP UP DATA IN FILES FOR BRADMOD
local run_ssa_high_other = 0 
local run_ssa_high_separate = 1
local run_other = 1
local run_best = 0

local all_best = 1
local 24_month_best = 1
local drop_outliers = 1

local gen_year_fe = 1 // Do you want to add a year fixed effect for pre-/post- certain years?

local dismod_templates "FILEPATH"
local dismod_dir "FILEPATH"
local bradmod_dir "FILEPATH"


************** PART 1 : Run all regions together for each sex age duration combo
	// Save data for full run with all sites
if `run_ssa_high_other' == 1 {
	foreach a in 15_25 25_35 35_45 45_55 55_100 { 
		foreach s in 1 2{
	
			cap mkdir "`dismod_dir'/HIV_KM_`a'_`s'_0_6"
			cap mkdir "`dismod_dir'/HIV_KM_`a'_`s'_7_12"
			cap mkdir "`dismod_dir'/HIV_KM_`a'_`s'_12_24"

			use "`bradmod_dir'/tmp_conditional.dta", clear
			keep if (age == "`a'" & sex == `s') | integrand=="mtall"
		
			if `drop_outliers' {
				drop if time_lower<1996 & time_upper<2003  & super=="high"
				drop if time_lower==1994  & super=="high"
				drop if meas_value>.05 & age_lower!=0 & super=="high" & nid != "1997"
				drop if pubmed_id==18981772 & time_point==24   & super=="high"
				drop if pubmed_id==22205933 & time_point==24 & age_lower==35 & super=="high"
				drop if nid == "1997" & !regexm(cohort, "Africa") & !regexm(cohort, "North America Crude") & !regexm(cohort,"Latin America")
				drop if nid == "214573"
			} 
			
			replace x_sex = 0

		    
			if `gen_year_fe' == 1  {
				// Generate a fixed-year effect on pre-2003 and pre-2000 studies
				egen x_year_linear=rowmean(time_lower time_upper)
				// sum x_year_linear
				// local mean=`r(mean)'
				// replace x_year_linear=x_year_linear-`mean'
				gen x_year_pre2002=(x_year_linear < 2002)
				replace x_year_pre2002 = 0 if integrand == "mtall"
				// gen x_year_pre2003=(x_year_linear < 2003)
				drop x_year_linear
			}
			
		
			// 0-6
				preserve
				keep if time_point==6 | integrand=="mtall"
				sum age_upper if integrand!="mtall"
				local max_0_6=`r(max)'
				replace age_upper=`max_0_6' if integrand=="mtall" & age_lower!=0
				outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_0_6/data_in.csv", delim(",") replace 
				restore
			
			// 7-12
				preserve
				keep if time_point==12 | integrand=="mtall"
				sum age_upper if integrand!="mtall"
				local max_7_12=`r(max)'
				replace age_upper=`max_7_12' if integrand=="mtall" & age_lower!=0
				outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_7_12/data_in.csv", delim(",") replace 
				restore
			
			// 13-24
				preserve
				keep if time_point==24 | integrand=="mtall"
				sum age_upper if integrand!="mtall"
				local max_12_24=`r(max)'
				replace age_upper=`max_12_24' if integrand=="mtall" & age_lower!=0	
				outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_12_24/data_in.csv", delim(",") replace 
				restore
		
			// Save plain in, effect in, value in, draw in, rate in csv
				insheet using "`dismod_templates'//plain_in.csv", clear comma names
				foreach dur in 0_6 7_12 12_24 {
					outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_`dur'//plain_in.csv", comma names replace
				}
			
				insheet using "`dismod_templates'//value_in.csv", clear comma names
				replace value = "1993" if name == "random_seed"
				foreach dur in 0_6 7_12 12_24 {
					outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_`dur'//value_in.csv", comma names replace
				}
			
					
				foreach dur in 0_6 7_12 12_24 {
					insheet using "`dismod_templates'//effect_in.csv", clear comma names
					

					// Effect in: Specify the covariates etc. that should go in (should match with covariates present in data_in and draw_in
					/*
						// linear
						local setobs=_N+1
						set obs `setobs'
						replace integrand="incidence" if _n==_N
						replace effect="xcov" if _n==_N
						replace name="x_year_linear" if _n==_N
						replace lower=-2 if _n==_N
						replace upper=2 if _n==_N
						replace mean=0 if _n==_N
						replace std="inf" if _n==_N 
					*/
					
					
					if `gen_year_fe' == 1  {
						// binary
						// foreach year in 2000 2003 {
						foreach year in 2002 {
							local setobs=_N+1
							set obs `setobs'
							replace integrand="incidence" if _n==_N
							replace effect="xcov" if _n==_N
							replace name="x_year_pre`year'" if _n==_N
							replace lower=-2 if _n==_N
							replace upper=2 if _n==_N
							replace mean=0 if _n==_N
							replace std="inf" if _n==_N   
						}
					}
									
					outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_`dur'//effect_in.csv", comma names replace
				}
			
				foreach dur in 0_6 7_12 12_24 {
					insheet using "`dismod_dir'/HIV_KM_high_0_6/draw_in.csv", clear 
					tempfile high 
					save `high', replace
					insheet using "`dismod_dir'/HIV_KM_other_0_6/draw_in.csv", clear 
					append using `high' 
					replace super = "high" if super == "none"
					
					outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_`dur'//draw_in.csv", comma names replace
				}
			
				foreach dur in 0_6 7_12 12_24 {
					insheet using "`dismod_templates'//rate_in.csv", clear comma names
					drop if age==`max_`dur'' & (type=="diota" | type=="iota") & age!=100
					replace age=`max_`dur'' if age==100
				
					outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_`dur'//rate_in.csv", comma names replace
				}
		}	
	}
		
}


********** PART 2A: Run high and ssa in separate models (alot of models) 
if `run_ssa_high_separate' == 1 {
	foreach a in 15_25 25_35 35_45 45_55 55_100 { 
		foreach s in 1 2{
			foreach sup in ssa high{
	
				cap mkdir "`dismod_dir'/HIV_KM_`a'_`s'_0_6_`sup'"
				cap mkdir "`dismod_dir'/HIV_KM_`a'_`s'_7_12_`sup'"
				cap mkdir "`dismod_dir'/HIV_KM_`a'_`s'_12_24_`sup'"

				use "`bradmod_dir'/tmp_conditional.dta", clear
				keep if (age == "`a'" & sex == `s' & super == "`sup'") | integrand=="mtall"
			
				if `drop_outliers' {
					drop if time_lower<1996 & time_upper<2003  & super=="high"
					drop if time_lower==1994  & super=="high"
					drop if meas_value>.05 & age_lower!=0 & super=="high" & nid != "1997"
					drop if pubmed_id==18981772 & time_point==24   & super=="high"
					drop if pubmed_id==22205933 & time_point==24 & age_lower==35 & super=="high"
					drop if nid == "1997" & !regexm(cohort, "Africa") & !regexm(cohort, "North America Crude") & !regexm(cohort,"Latin America")
					drop if nid == "214573"
				} 
				
				replace x_sex = 0
				replace super = "none"
				
				if `gen_year_fe' == 1 & "`sup'" == "high" {
					// Generate a fixed-year effect on pre-2003 and pre-2000 studies
					egen x_year_linear=rowmean(time_lower time_upper)
					// sum x_year_linear
					// local mean=`r(mean)'
					// replace x_year_linear=x_year_linear-`mean'
					gen x_year_pre2002=(x_year_linear < 2002)
					replace x_year_pre2002 = 0 if integrand == "mtall"
					// gen x_year_pre2003=(x_year_linear < 2003)
					drop x_year_linear
				}
				
			
				// 0-6
					preserve
					keep if time_point==6 | integrand=="mtall"
					sum age_upper if integrand!="mtall"
					local max_0_6=`r(max)'
					replace age_upper=`max_0_6' if integrand=="mtall" & age_lower!=0
					outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_0_6_`sup'/data_in.csv", delim(",") replace 
					restore
				
				// 7-12
					preserve
					keep if time_point==12 | integrand=="mtall"
					sum age_upper if integrand!="mtall"
					local max_7_12=`r(max)'
					replace age_upper=`max_7_12' if integrand=="mtall" & age_lower!=0
					outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_7_12_`sup'/data_in.csv", delim(",") replace 
					restore
				
				// 13-24
					preserve
					keep if time_point==24 | integrand=="mtall"
					sum age_upper if integrand!="mtall"
					local max_12_24=`r(max)'
					replace age_upper=`max_12_24' if integrand=="mtall" & age_lower!=0	
					outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_12_24_`sup'/data_in.csv", delim(",") replace 
					restore
			
				// Save plain in, effect in, value in, draw in, rate in csv
					insheet using "`dismod_templates'//plain_in.csv", clear comma names
					foreach dur in 0_6 7_12 12_24 {
						outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_`dur'_`sup'//plain_in.csv", comma names replace
					}
				
					insheet using "`dismod_templates'//value_in.csv", clear comma names
					replace value = "1993" if name == "random_seed"
					foreach dur in 0_6 7_12 12_24 {
						outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_`dur'_`sup'//value_in.csv", comma names replace
					}
				
						
					foreach dur in 0_6 7_12 12_24 {
						insheet using "`dismod_templates'//effect_in.csv", clear comma names
						drop if inlist(name,"high","ssa","other")

						// Effect in: Specify the covariates etc. that should go in (should match with covariates present in data_in and draw_in
						/*
							// linear
							local setobs=_N+1
							set obs `setobs'
							replace integrand="incidence" if _n==_N
							replace effect="xcov" if _n==_N
							replace name="x_year_linear" if _n==_N
							replace lower=-2 if _n==_N
							replace upper=2 if _n==_N
							replace mean=0 if _n==_N
							replace std="inf" if _n==_N 
						*/
						
						
						if `gen_year_fe' == 1 & "`sup'" == "high"  {
							// binary
							// foreach year in 2000 2003 {
							foreach year in 2002 {
								local setobs=_N+1
								set obs `setobs'
								replace integrand="incidence" if _n==_N
								replace effect="xcov" if _n==_N
								replace name="x_year_pre`year'" if _n==_N
								replace lower=-2 if _n==_N
								replace upper=2 if _n==_N
								replace mean=0 if _n==_N
								replace std="inf" if _n==_N   
							}
						}
										
						outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_`dur'_`sup'//effect_in.csv", comma names replace
					}
				
					foreach dur in 0_6 7_12 12_24 {
						insheet using "`dismod_dir'/FILEPATH//draw_in.csv", comma clear names 
						if `gen_year_fe' == 1 { 
							gen x_year_pre2002 = 0
						}
						
						outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_`dur'_`sup'//draw_in.csv", comma names replace
					}
				
					foreach dur in 0_6 7_12 12_24 {
						insheet using "`dismod_templates'//rate_in.csv", clear comma names
						drop if age==`max_`dur'' & (type=="diota" | type=="iota") & age!=100
						replace age=`max_`dur'' if age==100
					
						outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_`dur'_`sup'//rate_in.csv", comma names replace
					} 
			}
		}	
	}
		
}

************** PART 2B : Run OTHER region seperately, but include SSA data to help inform it
if `run_other' == 1 {
	foreach a in 15_25 25_35 35_45 45_55 55_100 { 
			foreach s in 1 2{
				// Save data for full run with all sites
				
				cap mkdir "`dismod_dir'/HIV_KM_`a'_`s'_0_6_other"
				cap mkdir "`dismod_dir'/HIV_KM_`a'_`s'_7_12_other"
				cap mkdir "`dismod_dir'/HIV_KM_`a'_`s'_12_24_other"

				use "`bradmod_dir'/tmp_conditional.dta", clear
				keep if (super=="ssa" & age == "`a'" & sex == `s') | (super=="other" & age == "`a'" & sex == `s') | integrand=="mtall"
				
				/*
				if `gen_year_fe' == 1 {
					// Generate a fixed-year effect on pre-2003 and pre-2000 studies
					egen x_year_linear=rowmean(time_lower time_upper)
					// sum x_year_linear
					// local mean=`r(mean)'
					// replace x_year_linear=x_year_linear-`mean'
					gen x_year_pre2002=(x_year_linear < 2002)
					replace x_year_pre2002 = 0 if integrand == "mtall"
					// gen x_year_pre2003=(x_year_linear < 2003)
					drop x_year_linear
				}
				*/
				replace x_sex = 0 
				
				if `drop_outliers' {
					drop if time_lower<1996 & time_upper<2003  & super=="high"
					drop if time_lower==1994  & super=="high"
					drop if meas_value>.05 & age_lower!=0 & super=="high" & nid != "1997"
					drop if pubmed_id==18981772 & time_point==24   & super=="high"
					drop if pubmed_id==22205933 & time_point==24 & age_lower==35 & super=="high"
					drop if nid == "1997" & !regexm(cohort, "Africa") & !regexm(cohort, "North America Crude") & !regexm(cohort,"Latin America")
					drop if nid == "214573"
				} 

					// 0-6
					preserve
					keep if time_point==6 | integrand=="mtall"
					sum age_upper if integrand!="mtall"
					local max_0_6=`r(max)'
					replace age_upper=`max_0_6' if integrand=="mtall" & age_lower!=0
					outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_0_6_other/data_in.csv", delim(",") replace 
					restore
					
					// 7-12
					preserve
					keep if time_point==12 | integrand=="mtall"
					sum age_upper if integrand!="mtall"
					local max_7_12=`r(max)'
					replace age_upper=`max_7_12' if integrand=="mtall" & age_lower!=0
					outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_7_12_other/data_in.csv", delim(",") replace 
					restore
					
					// 13-24
					preserve
					keep if time_point==24 | integrand=="mtall"
					sum age_upper if integrand!="mtall"
					local max_12_24=`r(max)'
					replace age_upper=`max_12_24' if integrand=="mtall" & age_lower!=0	
					outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_12_24_other/data_in.csv", delim(",") replace 
					restore
				
				// Save plain in, effect in, value in csv
					insheet using "`dismod_templates'//plain_in.csv", clear comma names
					foreach dur in 0_6 7_12 12_24 {
						outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_`dur'_other//plain_in.csv", comma names replace
					}
					
					insheet using "`dismod_templates'//value_in.csv", clear comma names
					replace value = "1993" if name == "random_seed"
					foreach dur in 0_6 7_12 12_24 {
						outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_`dur'_other//value_in.csv", comma names replace
					}
					
							
					foreach dur in 0_6 7_12 12_24 {
						insheet using "`dismod_templates'//effect_in.csv", clear comma names
						drop if name=="high"
						
						// Effect in: Specify the covariates etc. that should go in (should match with covariates present in data_in and draw_in
						/*
							// linear
							local setobs=_N+1
							set obs `setobs'
							replace integrand="incidence" if _n==_N
							replace effect="xcov" if _n==_N
							replace name="x_year_linear" if _n==_N
							replace lower=-2 if _n==_N
							replace upper=2 if _n==_N
							replace mean=0 if _n==_N
							replace std="inf" if _n==_N 
						*/
						
						/*		
						if `gen_year_fe' == 1 {					
							// binary
							// foreach year in 2000 2003 {
							foreach year in 2002 {
								local setobs=_N+1
								set obs `setobs'
								replace integrand="incidence" if _n==_N
								replace effect="xcov" if _n==_N
								replace name="x_year_pre`year'" if _n==_N
								replace lower=-2 if _n==_N
								replace upper=2 if _n==_N
								replace mean=0 if _n==_N
								replace std="inf" if _n==_N   
							}
						}
						*/						
						outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_`dur'_other//effect_in.csv", comma names replace
					}
					
					foreach dur in 0_6 7_12 12_24 {
						insheet using "`dismod_dir'/FILEPATH//draw_in.csv", comma clear names
						if `gen_year_fe' == 1 {
							gen x_year_pre2002 = 0
							// gen x_year_pre2003 = 0
						}
						outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_`dur'_other//draw_in.csv", comma names replace
					}
					
					foreach dur in 0_6 7_12 12_24 {
						insheet using "`dismod_templates'//rate_in.csv", clear comma names
						drop if age==`max_`dur'' & (type=="diota" | type=="iota") & age!=100
						replace age=`max_`dur'' if age==100
						outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_`dur'_other//rate_in.csv", comma names replace
					} 
			}
		}

}
	
	
************ PART 3: Run the 'best performers' in a model with the rest of africa, but just add a separate random effect

if `run_best' == 1 {
	// Try running a completely separate bradmod model where the only data inputs are the best performers
		
		cap mkdir "`dismod_dir'/HIV_KM_ssabest_0_6"
		cap mkdir "`dismod_dir'/HIV_KM_ssabest_7_12"
		cap mkdir "`dismod_dir'/HIV_KM_ssabest_12_24"
		
		
		// keep only the best performers
		use "`bradmod_dir'/tmp_conditional.dta", clear
		
		// Based on residuals and qualitative judgements about site quality, make a new 'super region' just for the best african sites

		if `24_month_best' {
		replace super="ssabest" if ///
			site=="Buhera Hospitals and Health Centres" | ///
			site=="rural clinic, Mbarara" | ///
			site=="HIV/AIDS care clinic in Johannesburg" | ///
			site=="Buhera Health Centres" | ///
			site=="41 healthcare facilities in Kigali City and the western province of Rwanda"
		}
			
			
		if `all_best' {	
		replace super="ssabest" if ///
			site=="Buhera Hospitals and Health Centres" | ///
			site=="rural clinic, Mbarara" | ///
			site=="HIV/AIDS care clinic in Johannesburg" | ///
			site=="Buhera Health Centres" | ///
			site=="41 healthcare facilities in Kigali City and the western province of Rwanda" | ///
			cohort=="OPERA" | ///
			cohort=="HIMS" | ///
			cohort=="CTAC" | ///
			cohort=="Gaborone Independent" 
		}
		
		drop if super=="other" | super=="high"
		
		// Gauteng, KwaZulu-Natal, and Western Cape" also top, but EXCLUDED B/C GENDER SPECIFIC		
		// save data in files
			preserve
			keep if time_point==6 | integrand=="mtall"
			sum age_upper if integrand!="mtall"
			local max_0_6=`r(max)'
			replace age_upper=`max_0_6' if integrand=="mtall" & age_lower!=0
			outsheet using "`dismod_dir'/HIV_KM_ssabest_0_6/data_in.csv", comma names replace
			restore
				
			preserve
			keep if time_point==12 | integrand=="mtall"
			sum age_upper if integrand!="mtall"
			local max_7_12=`r(max)'
			replace age_upper=`max_7_12' if integrand=="mtall" & age_lower!=0	
			outsheet using "`dismod_dir'/HIV_KM_ssabest_7_12/data_in.csv", comma names replace
			restore
			
			preserve
			keep if time_point==24 | integrand=="mtall"
			sum age_upper if integrand!="mtall"
			local max_12_24=`r(max)'
			replace age_upper=`max_12_24' if integrand=="mtall" & age_lower!=0
			outsheet using "`dismod_dir'/HIV_KM_ssabest_12_24/data_in.csv", comma names replace
			restore
		
		// Save plain in, and value in,  effect_in and rate_in csv
			insheet using "`dismod_templates'//plain_in.csv", clear comma names
			foreach dur in 0_6 7_12 12_24 {
				outsheet using "`dismod_dir'/HIV_KM_ssabest_`dur'//plain_in.csv", comma names replace
			}
			
			insheet using "`dismod_templates'//value_in.csv", clear comma names
			replace value = "1993" if name == "random_seed"
			foreach dur in 0_6 7_12 12_24 {
				outsheet using "`dismod_dir'/HIV_KM_ssabest_`dur'//value_in.csv", comma names replace
			}
				 
			foreach dur in 0_6 7_12 12_24 {
				insheet using "`dismod_templates'//effect_in.csv", clear comma names
				expand=2 if name=="ssa"
				replace name="ssabest" if name=="ssa" & _n==_N
				drop if name=="other" | name=="high"
				outsheet using "`dismod_dir'/HIV_KM_ssabest_`dur'//effect_in.csv", comma names replace
			}
			 
			foreach dur in 0_6 7_12 12_24 {
				insheet using "`dismod_templates'//rate_in.csv", clear comma names
				replace age=`max_`dur'' if age==100
				duplicates drop type age lower upper mean std, force // Right now, iota is getting duplicated
				qui sum age if type == "iota"
				if `r(max)' == 50 drop if type == "diota" & age == 50 // Diota needs to have one less observation than iota, so if iota gets truncated at age 50 and an observation gets dropped then drop diota for that corresponding one
				outsheet using "`dismod_dir'/HIV_KM_ssabest_`dur'//rate_in.csv", comma names replace
			}
			
}


	
