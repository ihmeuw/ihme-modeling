capture program drop run_regression
program define run_regression
 
    ** ***************************************************************************************
    **  SET UP STATA                            
    ** ***************************************************************************************

    clear
    capture clear matrix
    set mem 7g
    set more off
    set matsize 7000
    set maxvar 32000
    pause on

    syntax , [iso3(string) year(string) clear] 

    local user "`c(username)'"

    if c(os)=="Windows" {
         local prefix="FILEPATH"
         local homeprefix="FILEPATH"

        }
    else if c(os)=="Unix" & "`1'" != "" {
        local prefix="FILEPATH"
         local homeprefix="FILEPATH"

        }
    else {
        local prefix = "FILEPATH"
         local homeprefix="FILEPATH"
    }

    use "FILEPATH", clear

    replace yr_interview=yr_interview+100 if yr_interview<=1950

    ** ***************************************************************************************
    **  Drop sibs with missing data                 
    ** ***************************************************************************************
    qui count if alive > 1
    local n = r(N)
    n di "Dropping `n' siblings from `country' with unknown alive/dead status"
    drop if alive>1

    qui count if yob==.
    local n = r(N)
    n di "Dropping `n' siblings from `country' with no year of birth information"
    drop if yob==.

    qui count if yod==. & alive==0
    local n = r(N)
    n di "Dropping `n' dead siblings from `country' with no year of death information"
    drop if yod==. & alive==0

    ** ***************************************************************************************
    **  Create GK weights
    ** ***************************************************************************************

    ** Generate Gakidou-King (GK) weights. GK weight = births/survivors in each family 
    ** GK WEIGHT:                                       
    bysort id_sm : gen bi = _N
    bysort id_sm : egen si_old = total(alive)
    gen gkwt_old = bi/si_old
    bysort id_sm : egen si = total(alive) if sex==0 & (yr_interview-yob)>=15 & (yr_interview-yob)<=49
    bysort id_sm : egen tot_si = total(si)  
    replace si = 1 if tot_si==. | tot_si==0
    sort id_sm si
    by id_sm: replace si=si[1]
    gen gkwt = 1 / si
    replace gkwt = 1 if missing_sib == 1 | gkwt == .

    ** SAMPLE WEIGHT:
    gen samplewt = v005
    replace samplewt = 1 if missing_sib == 1
    gen totalwt = samplewt*gkwt
    egen totalwt_total = total(totalwt)
    ** Use sampling weights as well as Gakidou-King (GK) weights. 
    replace totalwt = totalwt/totalwt_total                         
    label var totalwt "(gkwt * samplewt) / totalwt"
    drop v005 alive si 

    if "`iso3'_`year'" == "NPL_2016_2017" {
        replace surveyyear = 2016 if surveyyear == 2072
        replace yob = yob - 57
    }
    drop if yob > surveyyear
    drop if yod > surveyyear & yod !=. 

    ** ***************************************************************************************
    **  3. Expand each sibling by 15 years so that each observation is a sibship-year
    ** ***************************************************************************************

    ** YEARS
    ** Expanding from each individual to each individual YEAR - we're expanding by 15 because we're only going back 15 years in time from the time of the survey 
    expand 15               
    bysort id_sm sibid: gen order = _n
    gen calcyear = surveyyear
    gen yrcatstart = calcyear - order + 1
    ** renaming for merge with HIV data - converts back to yrcatstart after 
    rename yrcatstart year              

    n tab missing_sib

    ** ****************************
    // DECISION POINT: IF YEARS NEED TO BE GROUPED DIFFERENTLY THAN 3 FIVE YEAR PERIODS, THIS IS THE PLACE TO DO IT
    ** ****************************
    gen year2 = .
    replace year2 = year
    gen svy_yr = iso3
    tostring svy_yr, replace
    replace svy_yr = svy_yr + "_" + string(year2)

    rename year yrcatstart

    ** ***************************************************************************************
    **  5. Generate TPS variable, CY, Age blocks (to be used in regression analysis)
    ** ***************************************************************************************

    ** AGE:
    ** single year age calculation 
    gen age = .                                 
    replace age = yrcatstart - yob
    ** because they aren't then yet born 
    replace age = . if age < 0
    ** drop young and old:
    ** drop the young and the old outside of our range 
    drop if age < 15                            
    drop if age > 59

    ** AGE BLOCKS: 
    gen ageblock = . 
    forvalues age = 0(5)60   {
                replace ageblock =`age' if age - `age'  < 5 & age - `age' >= 0
                }
    replace ageblock = 60 if ageblock > 60 & ageblock != . 

    rename year2 year

    ** ***************************************************************************************
    **  6. Create outcome variable (reported dead, yes or no) 
    ** ***************************************************************************************

    ** Outcome variable dead values: missing if not born yet, 0 once born until year before dead, 1 on the year they die, missing after they die 
    n di "Generating Dead variable"

    gen dead = .

    ** For dead people: 
    replace dead = 1 if yod !=. & yod == yrcatstart 
    replace dead = . if yod !=. & yod < yrcatstart
    replace dead = 0 if yod !=. & yod > yrcatstart & yob <= yrcatstart
    replace dead = . if yod != . & yod > yrcatstart & yob > yrcatstart

    ** For alive people: 
    replace dead = 0 if yod ==. & yob <= yrcatstart
    drop if dead == . 

    preserve
    gen alive = 1 if dead == 0
    replace alive = 0 if dead == 1
    collapse (sum) dead alive, by(year)
    save "FILEPATH", replace

    levelsof dead, local(deaths)

    foreach death of local deaths {
        if "`death'" == "0"{
            dis "no reported deaths in one of the reported years"
        }
    }
    restore

    ** drop variables that are no longer needed
    drop country v008 yod yob year yrcatstart surveyyear calcyear yr_interview  
    order  sex psu samplewt totalwt     
    compress
    save "FILEPATH", replace 

    foreach sex in 0 1 {

        matrix drop _all
        use svy using "FILEPATH", clear

        levelsof svy, local(svys)

        foreach svy of local svys {

            use if sex == `sex' & svy == "`svy'" using "FILEPATH", clear     

            ** ***************************************************************************************
            **  1. Put existing variable names into command lines that can be automatically put into 
            **       code to run regression so that each variable doesn't have to be typed in manually
            ** ***************************************************************************************

            tab ageblock, gen(dumage)
            foreach var of varlist dumage* {
                sort `var'
                local name = ageblock[_N]
                rename `var' `var'_`name'
                }

            local svy4 = ""
            local svy5 = ""

            tab svy_yr, gen(dumsvy)
            foreach var of varlist dumsvy* {
                sort `var'
                local name = svy_yr[_N]
                rename `var' `var'_`name'
                }
            local i = 1
            local svy_list = ""
            foreach var of varlist dumsvy* {
                if `i' > 1 {
                    local svy_list = "`svy_list' `var'"
                }
                local svy`i' = "`var'" // Not used, but in case we need to revert, let's keep it here
                local i = `i' + 1
                }
            di "`svy2'" 
            di "`svy3'"
            di "`svy4'"
            di "`svy5'"

            ** ***************************************************************************************
            **  2. Runs regression, saves coefficient and variance-covariance in matrix
            ** ***************************************************************************************

            disp "`svy' `sex' Logit"
            if "`svy'" == "NPL_2016_2017" {
                logit dead dumage2 - dumage7 `svy_list'  [pweight=totalwt] if sex == `sex', cluster(psu)
            }
            else {
                logit dead dumage2 - dumage9 `svy_list'  [pweight=totalwt] if sex == `sex', cluster(psu)
            }

            ** Storing betas into matrix 
            matrix B = e(b) 
            ** Storing var/covar matrix into matrix         
            matrix V = e(V)                     
            ** Storing the number of observations in the regression
            local obs = e(N)    
            ** Saves pseudo-R2 for comparing models
            local pseudoR2 = e(r2_p)             

            ** ***************************************************************************************
            **  3. Saves regression output for graphing age coefficients
            ** ***************************************************************************************

            preserve
            clear

            matrix var = vecdiag(V)
            matrix rownames var = variance
            matrix rownames B = coefficient

            matrix O = (B \ var)
            matrix O = O'
            local rows = rowsof(O)
            local names : rowfullnames O

            set obs `rows'

            gen varname = ""

            forvalues x = 1/`rows' {
                local varnm : word `x' of `names'
                replace varname = "`varnm'" in `x'
                }

            svmat O, names(col)

            if `sex'==1 {
                local sx = "male"
                }

            if `sex'==0 {
                local sx = "female"
                }

            gen pseudoR2 = `pseudoR2'

            save "FILEPATH", replace  
            restore

            ** ***************************************************************************************
            **  4. Generate Uncertainty: create X matrix with each unique 0/1 combination representing each of the CY age categories 
            ** ***************************************************************************************

            gen tpsz = 0                                
            gen cons = 1                                

            ** Contract the dataset so there is one unique observation for every unique combination of X variables. 
            if "`svy'" == "NPL_2016_2017" {
                contract svy_yr ageblock dumage2 - dumage7 `svy_list'  cons
                sort svy_yr ageblock cons
                ** Create the X matrix of the independent variables. 
                mkmat dumage2 - dumage7`svy_list'  cons, matrix(X)
            }
            else {
                contract svy_yr ageblock dumage2 - dumage9 `svy_list'  cons
                sort svy_yr ageblock cons
                ** Create the X matrix of the independent variables. 
                mkmat dumage2 - dumage9 `svy_list'  cons, matrix(X)             
            }

            ** ***************************************************************************************
            **  5. Match the regression variance-covariance matrix and the possible CY age categories 
            **   matrix to draw 1000 predictions for each category 
            ** ***************************************************************************************

            ** Preserves dataset with one observation for each unique combination of X variables. 
            preserve                                
            ** Store the number of betas in the model. 
            local betas = colsof(V)                 
            ** Designates how many values to draw. 
            local s = 1000                          
            clear
            ** Draw s (defined above) values of the betas using the variance covariance matrix. 
            set seed 123456789

            drawnorm b1-b`betas', means(B) cov(V) n(`s')    
            ** Store draws of betas into a matrix. 
            mkmat b* in 1/`s', matrix(betaxpose)            
            ** Reshape matrix. 
            matrix beta = betaxpose'                        
            ** Calculate the probabilities for each scenario 1000 times. 
            matrix P = X*beta                               

            clear
            ** Restores dataset with one observation for each unique combination of X variables. 
            restore                                         
            ** Keeps all variables used to create the dummies - still one obs for each combination of X variables. 
            keep svy_yr ageblock cons       
            sort svy_yr ageblock cons       

            preserve                            
            contract svy_yr 
            drop _freq
            ** x is a unique ID for every CY and will be useful for looping later 
            gen x = _n                                      
            tempfile addx
            sort svy_yr 
            save `addx', replace
            restore

            sort svy_yr 
            merge m:1 svy_yr using `addx'
            tab _merge
            drop _merge

            sort svy_yr ageblock cons   
            ** Adds `s' predicted probabilities for every unique combination of Xs 
            svmat P                                         

            ** **************************************************************************************
            **  6. Convert coefficients to yearly q's, then converts the yearly q's to 5 year q's for each age
            ** ***************************************************************************************

            ** Convert the predicted values to probabilities of death (reverse logit transformation). 
            forvalues x = 1/`s' {               
                ** e`x' is the one year age probability of death. 
                gen e`x' = exp(P`x')/(1+exp(P`x'))      
                ** q`x' is the five year age probability of death. 
                gen q`x' = (1-(1-e`x')^5)               
                ** minusq`x' is the five year probability of survival. 
                gen minusq`x' = 1 - q`x'                
                drop P`x'

            }

            bysort x: gen agegrp = _n           
                                                ** Within every CY, create a unique ID for each age group. 
                                                ** Now, CY and ageblock are the variables defining CY and age group with useful data - i.e. values are informative 
                                                ** and agegrp are variables defining CY and age group but are just sequential values 
                                                ** which are not informative but useful for looping. 

            ** ***************************************************************************************
            **  7. Calculates 45q15 from the 5 year q's 
            ** ***************************************************************************************

            ** Calculate 45q15 - bysort x refers to one 45q15 for each country year. 
            forvalues x = 1/`s' {                   
            bysort x: gen value`x' = 1-minusq`x'[1]*minusq`x'[2]*minusq`x'[3]*minusq`x'[4]*minusq`x'[5]*minusq`x'[6]*minusq`x'[7]*minusq`x'[8]*minusq`x'[9]
            bysort x: gen lgt`x' = ln(value`x'/(1 - value`x')) 
            }

            tempfile data
            save `data', replace

            preserve
            ** Create datafile of CY and age groups for merging results later and to distinguish which CY and age they are for. 
            keep svy_yr ageblock                    
            contract svy_yr ageblock

            drop _freq
            sort svy_yr ageblock
            tempfile labels
            save `labels', replace
            restore

            use `data', clear

            keep value* lgt* q* x svy_yr agegrp ageblock        
            order svy_yr x ageblock agegrp q* x*

            sort svy_yr ageblock
            ** Create matrix for agegroup-specific q's. 
            mkmat q*, matrix(agegrp_reshape)                    
            ** Reshape the matrix to prepare for percentile calculation; CY-ageblocks are columns, simulations are rows. 
            matrix agegrp = agegrp_reshape'                     

            ** Collapse across age groups to create one observation of 45q15 for each CY. 
            collapse(mean) value* lgt*, by(svy_yr)                  

            sort svy_yr
            ** Create matrix for 45q15. 
            mkmat value*, matrix(svy_yr_reshape)                    
            ** Reshape the matrix to prepare for percentile calculation: CY's are columns, simulations are rows.
            matrix svy_yr = svy_yr_reshape'                             

            sort svy_yr
            mkmat lgt*, matrix(lgt_reshape)
            matrix lgt = lgt_reshape'

            ** ***************************************************************************************
            **  8. Use the 2.5-97.5 percentile to compute lower bound and upper bound for uncertainty
            ** ***************************************************************************************

            ** Age group uncertainty bound generation 
            clear
            ** Recall agegrp matrix 
            svmat agegrp                                
            foreach var of varlist agegrp* {
                    _pctile `var', p(2.5, 97.5)
                    gen lb_`var' = r(r1)
                    gen ub_`var' = r(r2)
                egen mean_`var' = mean(`var')
                egen sd_`var' = sd(`var')
            }

            ** Create matrix of one lower bound (lb) estimate of q for each CY-ageblock. 
            mkmat lb_* in 1, matrix(lb_reshape)         
            matrix lb = lb_reshape'
            matrix colnames lb = lb_agegrp

            ** Creater matrix of one upper bound (ub) estimate of q for each CY-ageblock. 
            mkmat ub_* in 1, matrix(ub_reshape)         
            matrix ub = ub_reshape'
            matrix colnames ub = ub_agegrp

            ** Create matrix of one mean estimate of q for each CY-ageblock. 
            mkmat mean_* in 1, matrix(mean_reshape)     
            matrix mean = mean_reshape'
            matrix colnames mean = mean_agegrp          

            ** Create matrix of one standard deviation estimate of q for each CY-ageblock. 
            mkmat sd_* in 1, matrix(sd_reshape)         
            matrix sd = sd_reshape'
            matrix colnames sd = sd_agegrp  

            use `labels', clear
            svmat lb, names(col)
            svmat ub, names(col)
            svmat mean, names(col)
            svmat sd, names(col)

            ** ***************************************************************************************
            **  9. Reshape results so that each observation yields 45q15, 5 year q's, and corresponding 
            **   uncertainty bounds for each CY
            ** ***************************************************************************************

            reshape wide  lb_agegrp ub_agegrp mean_agegrp sd_agegrp, i(svy_yr) j(ageblock)
            tempfile mergeresult
            sort svy_yr
            save `mergeresult', replace

            clear
            svmat svy_yr
            foreach var of varlist svy_yr* {
                    _pctile `var', p(2.5, 97.5)
                    gen lb_45q15_`var' = r(r1)
                    gen ub_45q15_`var' = r(r2)
                egen mean_45q15_`var' = mean(`var')
                egen sd_45q15_`var' = sd(`var')
            }

            ** Create matrix of one lower bound (lb) estimate of 45q15 for each CY. 
            mkmat lb_* in 1, matrix(lb_reshape)         
            matrix lb = lb_reshape'
            matrix colnames lb = lb_45q15_svy_yr

            ** Create matrix of one upper-bound (ub) estimate of 45q15 for each CY. 
            mkmat ub_* in 1, matrix(ub_reshape)         
            matrix ub = ub_reshape'
            matrix colnames ub = ub_45q15_svy_yr

            ** Create matrix of one mean estimate of 45q15 for each CY. 
            mkmat mean_* in 1, matrix(mean_reshape)     
            matrix mean = mean_reshape'
            matrix colnames mean = mean_45q15_svy_yr            

            ** Create matrix of one standard deviation estimate of 45q15 for each CY. 
            mkmat sd_* in 1, matrix(sd_reshape)         
            matrix sd = sd_reshape'
            matrix colnames sd = sd_45q15_svy_yr            

            use `mergeresult', clear
            svmat lb, names(col)
            svmat ub, names(col)
            svmat mean, names(col)
            svmat sd, names(col)
            sort svy_yr
            save `mergeresult', replace

            clear
            svmat lgt
            foreach var of varlist lgt* {
                    _pctile `var', p(2.5, 97.5)
                    gen lb_lgt45q15_`var' = r(r1)
                    gen ub_lgt45q15_`var' = r(r2)
                egen mean_lgt45q15_`var' = mean(`var')
                egen sd_lgt45q15_`var' = sd(`var')
            }

            ** Create matrix of one lower bound (lb) estimate of 45q15 for each CY. 
            mkmat lb_* in 1, matrix(lb_reshape)         
            matrix lb = lb_reshape'
            matrix colnames lb = lb_lgt45q15_svy_yr

            ** Create matrix of one upper bound (ub) estimate of 45q15 for each CY. 
            mkmat ub_* in 1, matrix(ub_reshape)         
            matrix ub = ub_reshape'
            matrix colnames ub = ub_lgt45q15_svy_yr

            ** Create matrix of one mean estimate of 45q15 for each CY. 
            mkmat mean_* in 1, matrix(mean_reshape)     
            matrix mean = mean_reshape'
            matrix colnames mean = mean_lgt45q15_svy_yr         

            ** Create matrix of one standard deviation estimate of 45q15 for each CY. 
            mkmat sd_* in 1, matrix(sd_reshape)         
            matrix sd = sd_reshape'
            matrix colnames sd = sd_lgt45q15_svy_yr         

            use `mergeresult', clear
            svmat lb, names(col)
            svmat ub, names(col)
            svmat mean, names(col)
            svmat sd, names(col)

            gen sex = `sex'
            rename mean_45q15_svy_yr mean_45q15_`sex'
            rename ub_45q15_svy_yr ub_45q15_`sex'
            rename lb_45q15_svy_yr lb_45q15_`sex'
            rename sd_45q15_svy_yr sd_45q15_`sex'

            rename mean_lgt45q15_svy_yr mean_lgt45q15_`sex'
            rename ub_lgt45q15_svy_yr ub_lgt45q15_`sex'
            rename lb_lgt45q15_svy_yr lb_lgt45q15_`sex'
            rename sd_lgt45q15_svy_yr sd_lgt45q15_`sex'

            order svy_yr sex mean* sd* lb* ub*

            ** Rename 45q15 variables so male and female can be distinguished from each other
            capture rename mean_45q15_1 male_45q15
            capture rename mean_45q15_0 female_45q15
            capture rename mean_lgt45q15_1 male_lgt45q15
            capture rename mean_lgt45q15_0 female_lgt45q15

            foreach stem in ub lb sd {
                capture rename `stem'_45q15_1 male_`stem'_45q15
                capture rename `stem'_45q15_0 female_`stem'_45q15
                capture rename `stem'_lgt45q15_1 male_`stem'_45q15
                capture rename `stem'_lgt45q15_0 female_`stem'_45q15
                }

            ** Rename 5-year q's so male and female can be distinguished from each other
            forvalues x = 15(5)55 {
            capture rename mean_agegrp`x' mean_agegrp`x'_`sex'
            capture rename lb_agegrp`x' lb_agegrp`x'_`sex'
            capture rename ub_agegrp`x' ub_agegrp`x'_`sex'
            capture rename sd_agegrp`x' sd_agegrp`x'_`sex'
            }

            foreach cat in mean ub lb {
            forvalues x = 15(5)55 {
            capture rename `cat'_agegrp`x'_1 male_`cat'_agegrp`x'
            capture rename `cat'_agegrp`x'_0 female_`cat'_agegrp`x' 
            }
            }

            forvalues x = 15(5)55 {
            capture rename sd_agegrp`x'_1 sd_agegrp`x'_male
            capture rename sd_agegrp`x'_0 sd_agegrp`x'_female
            }

            save "FILEPATH", replace 
            }
        }

end
