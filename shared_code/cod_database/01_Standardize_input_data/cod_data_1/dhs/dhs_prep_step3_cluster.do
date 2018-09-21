**********************************************************************************
**Author: Mollie Hogan
**Purpose: Create weights to adjust for bias and get deaths
*************************************************************************

clear all
set more off
pause off
capture restore, not

if c(os) == "Windows" {
	global prefix "J:"
}
if c(os) == "Unix" {
	global prefix "/home/j"
}

** Working directories
global log_dir "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/logs"
global data_dir "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/data"
global used_file "$prefix/WORK/03_cod/01_database/02_programs/dhs_rhs_sibling_survival/data/DHS_used_file.csv"

** set parameters
set obs 1
gen arg = "$arg"
split arg, parse(-) generate(a) destring
global newflag              = a1
local country				= a2	

** start log
cap log close
cd "$log_dir"
log using "dp3_`country'.smcl"

di in red "newflag: $newflag"
di in red "country: `country'"

** erase old data
capture erase "$data_dir/bycountry/`country'.dta"

** load data that was correctly formatted
insheet using "$used_file", clear
keep if country == "`country'"																		

** for new datasets, format so ready to be appended to the summarized country-specific files
if ($newflag == 1) keep if  new==1 

gen year1 = substr(year,1,4)
levelsof year1, local(years)
foreach year of local years {
    cap use "$data_dir/bycountry/`country' `year'.dta", clear
    if (_rc != 0) continue
    
    gen newid = id+"-"+filename 
    egen id_sm = group(newid)

    ******************************************************************************
    *****		Drop sibs with missing data						******
    ******************************************************************************

    qui count if alive > 1
    local n = r(N)
    di "Dropping `n' siblings from `country' with unknown alive/dead status"
    drop if alive>1

    qui count if yob==.
    local n = r(N)
    di "Dropping `n' siblings from `country' with no year of birth information"
    drop if yob==.

    qui count if yod==. & alive==0
    local n = r(N)
    di "Dropping `n' dead siblings from `country' with no year of death information"
    drop if yod==. & alive==0

    drop alive

    **********************************************************************************************************************************************************************
    * 1. Creates GK (Gakidou-King) weights
    **********************************************************************************************************************************************************************

    *GK WEIGHT:                              		  	
    ** generating GK weights = births/survivors in each family 
    bysort id_sm: gen bi = _N
    gen alive = 1 if yod == .
    replace alive = 0 if yod != .
    bysort id_sm : egen si_old = sum(alive)
    ** this is the new gk weight: 1/si where si is siblings who are eligible to respond to the survey (for DHS this is females between the ages of 15 and 49)
    bysort id_sm : egen si = total(alive) if sex==0 & (yr_interview+1900 - yob)>=15 & (yr_interview+1900 - yob)<=49
    gen gkwt_old = bi/si_old
    sort id_sm si
    by id_sm: replace si=si[1]
    gen gkwt = 1/si
    replace gkwt = 1 if gkwt == .

    *SAMPLE WEIGHT:
    ** we want to multiply the gk weights by the sampling weights and then divide by the sum to normalize our weights
    gen samplewt = v005
    gen totalwt = samplewt*gkwt
    egen totalwt_total = total(totalwt)
    ** Use sampling weights as well as GK weights. 
    replace totalwt = totalwt/totalwt_total               			
    label var totalwt "(gkwt * samplewt) / totalwt"
    drop v005 bi alive si si_old 

    *drop certain years from the most recent survey		
    ** Because we're using the last FULL year before the survey, there will be some additional births and deaths. 
    ** (For example: Survey actually conducted in March of 2006, but last full year was 2005, so the 2006 info is taken out)
    drop if yob > surveyyear
    drop if yod > surveyyear & yod !=. 

    *Storing YEAR INTERVALS for CY's in locals so that we can use them later - it is done this way to automate the process. 
    egen lastsurvey = max(surveyyear)
    ** CY1 = most recent period chronogically; Each of these is the beginning of a period, (e.g. 91 = 91-92) */
    gen cy1 = lastsurvey - 1					    
    ** Austin Schumacher, 1/25/2014: No need to do special stuff with RWA since it's in 1-year age groups now 
    ** replace cy1 = cy1+1 if country == "RWA"		/* Shift Rwanda so that 1991-1995 (genocide year(s)) are all part of the same CY */
    local cy1 = cy1[1]

    ** Updated 1/25/2014: Creates single-year country-year periods
    ** the total number of intervals created will need to be increased with more survey data */
    forvalues x = 2/55 {						
        local one = `x'-1
        ** CY2 = 1 yr before CY1, etc... */
        gen cy`x' = cy1 - `one'	
        ** Store in locals to be used later when generating CY variables */        
        local cy`x' = cy`x'[1]					
        disp `cy`x''
        drop cy`x'
    }

    drop lastsurvey


    **********************************************************************************************************************************************************************
    * 2. Expands each sibling by 15 years so that each observation is a sibship-year. For each sibship year, generates yearly information to be used in omission bias
    **********************************************************************************************************************************************************************

    *YEARS
    ** Expanding from each individual to each individual YEAR - only going back 14 years in time from the time of the survey 
    expand 15						    
    bysort id_sm sibid: gen order = _n
    gen calcyear = surveyyear
    gen yrcatstart = calcyear - order + 1
    *gen yrcatend = yrcatstart + 1
    *gen midyrcat = yrcatstart + 1


    **********************************************************************************************************************************************************************
    * 4. Generates TPS variable, CY, Age blocks (to be used in regression analysis)
    **********************************************************************************************************************************************************************


    ********AGE:
    gen age = . 								
    ** single year age calculation 
    replace age = yrcatstart - yob
    ** because they aren't then yet born 
    replace age = . if age < 0								
    ** drop the young and the old outside of our range 
    drop if age < 15								
    drop if age > 59

    *****AGE BLOCKS
    gen ageblock = . 
    forvalues age = 0(5)60   {
        replace ageblock =`age' if age - `age'  < 5 & age - `age' >= 0
    }
    replace ageblock = 60 if ageblock > 60 & ageblock != . 
    tab ageblock, gen(dumage) label


    ** ********CY:  Generating Country-years based on individual country's YEAR INTERVALS (see above) 
    di "Generating country-year blocks"
    gen yearblock = . 
    replace yearblock = yrcatstart   
    gen twodigyrblock = yearblock - 1900
    drop if twodigyrblock==.
    tostring twodigyrblock , gen(stryrblock)
    ** Making years 3 digit string so that they will be in chronological order once they are converted to dummies 
    gen stryrblock3dig = stryrblock if twodigyrblock > 99			
    replace stryrblock3dig = "0"+stryrblock if twodigyrblock < 100
    drop stryrblock
    rename stryrblock3dig stryrblock
    gen cy = country+stryrblock
    destring stryrblock, gen(year)
    replace year = 1900 + year

        
    **********************************************************************************************************************************************************************
    * 7. Creates outcome variable (reported dead, yes or no)
    **********************************************************************************************************************************************************************
    di "Generating Dead variable"
    ** Generating Dead variable 		 
	** Outcome variable dead values: missing if not born yet, 0 once born until year before dead, 1 on the year they die, missing after they die 
    ** Dead (dependent variable)
    gen dead = .
    *For people who are dead: 
    replace dead = 1 if yod !=. & yod == yrcatstart 
    replace dead = . if yod !=. & yod < yrcatstart
    replace dead = 0 if yod !=. & yod > yrcatstart & yob <= yrcatstart
    replace dead = . if yod != . & yod > yrcatstart & yob > yrcatstart

    *For alive people: 
    replace dead = 0 if yod ==. & yob <= yrcatstart

    drop if dead == . 
     ** For maternal deaths:

    generate dead_mat = 1 if matdeath == 1 & dead == 1
    
    capture append using "$data_dir/bycountry/`country'.dta"
    saveold "$data_dir/bycountry/`country'.dta", replace
}		
