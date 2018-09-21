///Purpose is to create all the csvs need for bradmod
clear all
set more off
if (c(os)=="Unix") {
	global root FILEPATH
}

if (c(os)=="Windows") {
	global root FILEPATH
}


********* SETUP UP DATA IN FILES FOR BRADMOD
local run_ssa_high_other = 1


local drop_outliers = 1

local gen_year_fe = 1 // Do you want to add a year fixed effect for pre-/post-2002

local dismod_templates FILEPATH
local dismod_dir FILEPATH
local bradmod_dir FILEPATH


//Run all regions together for each sex age duration combo
	
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
				drop if meas_value>.05 & age_lower!=0 & super=="high"
				drop if pubmed_id==18981772 & time_point==24   & super=="high"
				drop if pubmed_id==22205933 & time_point==24 & age_lower==35 & super=="high"
			} 
			
			//since we have sex specific models we will treat it as both sex
			replace x_sex = 0

		    
			if `gen_year_fe' == 1  {
				// Generate a fixed-year effect on pre-2002 cohorts
				egen x_year_linear=rowmean(time_lower time_upper)
				gen x_year_pre2002=(x_year_linear < 2002)
				replace x_year_pre2002 = 0 if integrand == "mtall"
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
				foreach dur in 0_6 7_12 12_24 {
					outsheet using "`dismod_dir'/HIV_KM_`a'_`s'_`dur'//value_in.csv", comma names replace
				}
			
					
				foreach dur in 0_6 7_12 12_24 {
					insheet using "`dismod_templates'//effect_in.csv", clear comma names				
					if `gen_year_fe' == 1  {
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





	