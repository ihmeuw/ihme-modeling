######################################################################################################################################################################
## 	Adult Mortality through Sibling Histories: #4. REGRESSION & GENERATING UNCERTAINTY TO CALCULATE 5 YEAR Q's & 45q15
##	Description: This do-file runs a regression model on sibling-period data for each survey, by sex.
##					It saves regression output and calculates age-specific q's, 45q15's (not fully adjusted yet), and confidence intervals.
##
##  NOTE: IHME OWNS THE COPYRIGHT
##


rm(list=ls())
library(readr)
library(data.table)
library(plyr)
library(fastDummies, lib=FILEPATH)
library(survey)
library(MASS)

args <- commandArgs(trailingOnly = T)
input_dir <- args[1]
input_svy <- args[2]
source <- args[3]
nid <-  args[4]

input <- fread(FILEPATH)
acs <- fread(FILEPATH)
acs[yr_interview<=1950,yr_interview:=yr_interview+100]

# Drop duplicates
acs <- unique(acs)

#########################################################################################
# Drop sibs with missing data
#########################################################################################

missing_drop <- acs[alive %in% c(0,1)]
missing_drop <- missing_drop[!(is.na(yod) & alive==0)]
missing_drop <- missing_drop[!is.na(yob)]


#########################################################################################
# Create GK weights
#########################################################################################

missing_drop[sex==2 & between(yr_interview-yob, 15, 49), si := sum(alive, na.rm=T), by=id_sm]
missing_drop[,tot_si := sum(si, na.rm=T), by=id_sm]
missing_drop[is.na(tot_si) | tot_si==0, si:=1]
missing_drop <- missing_drop[order(id_sm,si)]
missing_drop[, si := si[1], by=id_sm]

missing_drop[,gkwt := 1/si]
missing_drop[(is.na(gkwt)) | (missing_sib==1), gkwt := 1]

# Sample weight:
missing_drop[,samplewt := v005]
missing_drop[missing_sib==1, samplewt := 1]
missing_drop[,totalwt := samplewt*gkwt]

totalwt_total <- sum(missing_drop$totalwt, na.rm = T)
missing_drop[,totalwt := totalwt / totalwt_total]
missing_drop[,c('v005','alive','si') := NULL]

# Remove certain years
final_years <- missing_drop[yob <= surveyyear]
final_years <- final_years[!(!is.na(yod) & (yod > surveyyear))]

#########################################################################################
# 1. Expand each sibling by 15 years so that each observation is a sibship-year
#########################################################################################

expanded <- final_years
for (i in 1:14) {
  expanded <- rbind(expanded,final_years)
}

expanded <- expanded[order(id_sm,sibid)]
expanded[, order := 1:.N, by=c('id_sm','sibid')]
expanded[, calcyear := surveyyear]
expanded[, year := calcyear-order + 1]

expanded[, year2 := year]

expanded[, svy_yr := paste0(iso3,'_', as.character(year2))]



#########################################################################################
# 2. Generate TPS variable, CY, Age blocks (to be used in regression analysis)
#########################################################################################

tpsvar <- copy(expanded)
setnames(tpsvar,"year","yrcatstart")
tpsvar[,age := yrcatstart-yob]
tpsvar[age<0, age:= NA]

tpsvar <- tpsvar[between(age, 15,59) & !is.na(age),]
tpsvar[,ageblock := (age%/% 5)*5]

tpsvar[ageblock>60 & is.na(ageblock), ageblock := 60]

setnames(tpsvar,"year2","year")


#########################################################################################
# 3. Create outcome variable (reported dead, yes or no)
#########################################################################################

# Outcome variable dead values: missing if not born yet, 0 once born until year before dead, 1 on the year they die, missing after they die
print("Generating Dead variable")
# For dead people:
tpsvar[,dead:=NA]
tpsvar[!is.na(yod)&yod==yrcatstart, dead := 1]
tpsvar[!is.na(yod) & yod>yrcatstart & yob<=yrcatstart, dead := 0]

# For alive people:
tpsvar[is.na(yod) & yob<=yrcatstart, dead:=0]

step6 <- tpsvar[!is.na(dead),]

# drop varaibles that are no longer needed
step6[,c('v008','yod','yob','year','yrcatstart','surveyyear','calcyear','yr_interview') := NULL]

step6 <- step6[order(sex,psu,samplewt,totalwt)]
step6[, dead := as.integer(dead)]
fwrite(step6,  FILEPATH)


for (sx in unique(step6$sex)) {

  for (survey in unique(step6$svy)) {

    sex_spec <- step6[sex==sx & svy==survey,]

    design <- svydesign(id=~psu, weights=~totalwt, data=sex_spec)
    mylogit <- svyglm(formula = dead ~ factor(ageblock) + factor(svy_yr),
                      design=design, family="binomial")
    summary(mylogit)

    B <- mylogit$coefficients

    V <- vcov(mylogit)


    var <- diag(V)
    O <- data.table(varname = names(B),coefficients=B, variance=var)

    # save coefficients
    fwrite(O, FILEPATH)



    #########################################################################################
    # Generate Uncertainty: create X matrix with each unique 0/1 combination representing each of the CY age categories
    #########################################################################################

    sex_spec[, `:=`(tpsz=0, cons=1)]


    matrix_x <- sex_spec[, .N, by=c('svy_yr','ageblock', 'cons')]
    matrix_x <- matrix_x[order(svy_yr, ageblock, cons)]
    matrix_x[, N := NULL]
    matrix_x <- matrix_x[between(ageblock, 15,55)]
    matrix_x <- fastDummies::dummy_cols(matrix_x, select_columns='ageblock', remove_first_dummy = T)
    blocks <- grep('ageblock_', names(matrix_x), value=T)
    matrix_x <- matrix_x[,.SD, .SDcols=c('cons', blocks, 'svy_yr')]

    matrix_x <- fastDummies::dummy_cols(matrix_x, select_columns = 'svy_yr', remove_first_dummy = T)
    matrix_x[, svy_yr := NULL]
    matrix_x <- as.matrix(matrix_x) # unique combinations of independent dummy variables

    #########################################################################################
    #  Match the regression variance-covariance matrix and the possible CY age categories
    #	 matrix to draw 1000 predictions for each category
    #########################################################################################

    # Set seed for reproducibility
    set.seed(5)
    P <- MASS::mvrnorm(n=1000, mu=B, Sigma=V) # random draws
    # set column order
    P <- as.matrix(P)
    P <- matrix_x %*% t(P)  # Calculate the probabilities for each scenario 1000 times.

    data <- sex_spec[, .N, by=c('svy_yr','ageblock','cons')]
    data <- data[order(svy_yr,ageblock,cons)]
    data[, N := NULL]

    data[, x := .GRP, by='svy_yr']

    # cbind P matrix to data
    data <- cbind(data, P)

    ########################################################################################
    # Convert coefficients to yearly q's, then converts the yearly q's to 5 year q's for each age
    #########################################################################################

    # e = 1 year probability of death
    # q = 5 year age probability of death
    # minusq=5year prob of survival

    # 1st, make data long - don't need column-wise operations
    data <- melt(data, id.vars=c('svy_yr','ageblock','cons','x'), variable.name='draw_no', value.name='probability')
    data[, draw_no := as.numeric(substring(draw_no, 2))]

    # assign e, q, minusq
    data[, e := exp(probability)/(1+exp(probability))]
    data[, q := 1-(1-e)^5]

    data[, minusq := 1-q]

    #########################################################################################
    # Calculate 45q15 from the 5 year q's
    #########################################################################################

    if (survey=="NPL_2016_2017") {
      data[, value := 1-prod(minusq[1:7]), by=c('draw_no','x')]
    } else {
      data[, value := 1-prod(minusq[1:9]), by=c('draw_no','x')]
    }
    data[, lgt := log(value/(1-value)), by=c('draw_no','x')]

    labels <- data[, c('svy_yr','ageblock')]
    labels <- labels[, .N, by=names(labels)]
    labels <- labels[, N := NULL]

    # matrix for agegroup-specific q's
    agegrp <- data[,.(svy_yr,x,ageblock,draw_no,q,value,lgt)]

    cy_specific <- data[, lapply(.SD, mean), by=c('svy_yr','draw_no'), .SDcols=c('value','lgt')] # 1 45q15 estimate for each country year


    #########################################################################################
    # Use the 2.5-97.5 percentile to compute lower bound and upper bound for uncertainty
    #########################################################################################

    agegrp[, `:=`(pct_lower=quantile(q, .025), pct_upper=quantile(q, .975),
                  grp_sd = sd(q)), by=c('ageblock','svy_yr')]

    agegrp <- agegrp[,lapply(.SD, mean), by=c('ageblock','svy_yr'), .SDcols=c('q','pct_lower','pct_upper','grp_sd')]


    #########################################################################################
    # Reshape results so that each observation yields 45q15, 5 year q's, and corresponding
    #	 uncertainty bounds for each CY
    #########################################################################################

    output <- copy(cy_specific)

    output <- output[, `:=`(lb_45q15=quantile(value,.025, na.rm=T),
                            ub_45q15=quantile(value,.975, na.rm=T),
                            sd_45q15=sd(value),
                            mean_45q15=mean(value),
                            lb_lgt45q15=quantile(lgt,.025, na.rm=T),
                            ub_lgt45q15=quantile(lgt,.975, na.rm=T),
                            sd_lgt45q15=sd(lgt),
                            mean_lgt45q15=mean(lgt)), by=c('svy_yr')]
    output <- output[, lapply(.SD, mean), by='svy_yr', .SDcols=c('mean_45q15','lb_45q15','ub_45q15','sd_45q15','mean_lgt45q15','lb_lgt45q15','ub_lgt45q15','sd_45q15')]


    output <- merge(agegrp, output, by='svy_yr', all=T)
    output[, sex := sx]
    fwrite(output, FILEPATH)
  }

}

# ** *****************************************************************************************
#   ** 1. Append male and female results together
# ** *****************************************************************************************
file_list <- "FILEPATH"
outputs <- lapply(file_list, fread)
both_sexes <- do.call("rbind", outputs)
both_sexes[, deaths_source := source]

fwrite(both_sexes, "FILEPATH")

# ** *****************************************************************************************
#   ** 2. Destring cy into iso code and year
# ** *****************************************************************************************
fullmodel <- both_sexes
fullmodel[, yr := as.integer(substr(svy_yr,nchar(svy_yr)-3,nchar(svy_yr)))]#  Beginning of the 5 year period (ex. 1990: 1990-1994)

fullmodel <- fullmodel[order(sex, -yr)]
fullmodel[,period := .GRP, by = c('svy_yr')]
fullmodel[,female:=ifelse(sex==1,0,1)]
fullmodel[,svy:=input_svy]
fullmodel[,NID:=nid]
setnames(fullmodel, "mean_45q15","q45q15")
fullmodel <- unique(fullmodel[,.SD,.SDcols = c("svy","female","period","q45q15","NID","deaths_source")])
setorder(fullmodel,-female,period)
fwrite(fullmodel, "FILEPATH")
