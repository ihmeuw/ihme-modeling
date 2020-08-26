############################################################################################################
## Project: RF: Lead Exposure
## Purpose: Compile Exposure Estimates from all extractions and prepare files for Bradmod
############################################################################################################

# Libraries
rm(list=objects())
library(data.table)
library(magrittr)

source("FILEPATH")

#Load locations and restrict to relevant vars
locations <- get_location_metadata(gbd_round_id = 6, location_set_id = 22) 
locs <- copy(locations[, c("location_id", "ihme_loc_id"), with=F])

#***************************************************************************************************************

# Read in compiled data
df13 <- fread("FILEPATH")
df13 <- df13[order(nid)]

# remove studies from previous extraction for which we have no known source
df13 <- df13[citation != ""]

# Drop NHANES studies (which were more recently re-extracted from microdata)
df13 <- df13[!grepl("Centers for Disease Control and Prevention (CDC)*",citation),]
df13 <- df13[!grepl("Brody DJ, Pirkle JL, Kramer RA, et al. Blood lead levels",citation),]
df13 <- df13[!grepl("Blood lead levels--United States, 1988-1991",citation),]

# Drop other studies with any mis-extracted or duplicated data
df13 <- df13[!grepl("Lead, mercury and cadmium levels in Canadians",citation),]
df13 <- df13[!grepl("Blood lead levels in children after phase-out of leaded gasoline in Bombay",citation),]
df13 <- df13[!grepl("Assessing windows of susceptibility to lead-induced",citation),]
df13 <- df13[!grepl("Windows of lead exposure sensitivity, attained",citation),]
df13 <- df13[!grepl("Nriagu J, Afeiche M, Linder A, et al. Lead poisoning associated",citation),]
df13 <- df13[!grepl("Association between prenatal lead exposure and blood pressure in children",citation),]
df13 <- df13[!grepl("The impact of drinking water, indoor dust",citation),]
df13 <- df13[!grepl("National estimates of blood lead, cadmium, and mercury",citation),]
df13 <- df13[!grepl("Associations of blood lead, cadmium, and mercury with estimated",citation),]
df13 <- df13[!grepl("Impact of low blood lead concentrations on IQ and school",citation),]
df13 <- df13[!grepl("Blood lead levels and associated sociodemographic factors among preschool",citation),]
df13 <- df13[!grepl("Environmental Factors Predicting Blood Lead Levels in Pregnant",citation),]
df13 <- df13[!grepl("Implications of different residential lead standards",citation),]
df13 <- df13[!grepl("Mobile phone use, blood lead levels, and attention deficit",citation),]
df13 <- df13[!grepl("Low blood levels of lead and mercury and symptoms of attention deficit hyperactivity ",citation),]
df13 <- df13[!grepl("The intelligence quotient and malnutrition. Iron deficiency and the lead concentration as confusing variables",citation),]
df13 <- df13[!grepl("Niveles de plomo en sangre en ",citation),]
df13 <- df13[!grepl("Prevalence and determinants of lead intoxication in Mexican children of low socioeconomic status",citation),]
df13 <- df13[!grepl("relationship of blood and bone lead to menopause and bone mineral density among middle",tolower(citation)),]
df13 <- df13[!grepl("maternal influences on cord blood lead levels",tolower(citation)),]
df13 <- df13[!(grepl("Thomas VM, Socolow RH, Fanelli JJ",citation) & (site=="Trelleborg" | site == "Landskrona")),]
df13 <- df13[!(grepl("Use of isotope ratios to identify sources contributing to pediatric lead poisoning in Peru",citation) & mean == 25.6),]
df13 <- df13[nid != 122601 | old_nid != 103215]
df13 <- df13[nid != 122634 | old_nid != 103215]
df13 <- df13[nid != 131499 | old_nid != 103215]
df13 <- df13[nid != 131586]
df13 <- df13[nid != 131727]
df13 <- df13[nid != 131643]
df13[grepl("background exposure of urban populations to lead and cadmium: comparison between china and japan",tolower(citation)) & site == "Sendai",
     mean := 2.53]

# remove this NID that was previously used to signify that no NID existed yet
df13[nid==103215, nid:= ""]

# rename and create new variables to better overlap with the format of the extraction sheet
df13[, c("modelable_entity_id", "modelable_entity_name") := .(8953, "Lead exposure in blood exposure")]
df13[site!=".",smaller_site_unit := 1]
setnames(df13,c("citation","iso3","site","sex","notes"),c("field_citation_value","ihme_loc_id","site_memo","sex_id","note_SR"))
df13[sex_id==1,sex:="Male"]
df13[sex_id==2,sex:="Female"]
df13[sex_id==3,sex:="Both"]

# add1 asks whether it is measured in geometric mean (the gold standard) which corresponds to a cv_mean_type of 0
df13[,cv_mean_type := ifelse(add1,0, 1)]
df13[is.na(cv_mean_type), cv_mean_type := 1]

df13 <- df13[!grepl("USA",ihme_loc_id)]

#***************************************************************************************************************

# read in the extraction data
df16 <- fread("FILEPATH")

#subset to just actual data (remove empty rows)
df16 <- df16[modelable_entity_id==8953,]

# remove studies found to be overexposed or otherwise better to exclude for methodlogical reasons
df16 <- df16[-1*grep("environmental exposure to lead and renal adverse effects",tolower(field_citation_value))]
df16 <- df16[nid != ""]

# sample_size and cases were reversed when extracting 
df16[,sample_size:=NULL]
setnames(df16,"cases","sample_size")

#standardize sex vars
df16[sex=="Male",sex_id:=1]
df16[sex=="Female",sex_id:=2]
df16[sex=="Both",sex_id:=3]

# remove all US subnats for same reason listed above for GBD 13 (shifting intercept in model in odd ways)
df16 <- df16[!grepl("USA",ihme_loc_id) | nid == 303323]
df16 <- df16[nid != 278389]

#***************************************************************************************************************

# read in 2017 extraction data
df17 <- fread("FILEPATH")

# subset to just actual data (remove empty rows)
df17 <- df17[bundle_id_==1535]

# standardize sex vars
df17[sex=="Male",sex_id:=1]
df17[sex=="Female",sex_id:=2]
df17[sex=="Both",sex_id:=3]

#***************************************************************************************************************

#load newly extracted and tabulated microdata
NHANES1 <- fread("FILEPATH")
setnames(NHANES1, "bll_mean", "mean")
NHANES2 <- fread("FILEPATH")
setnames(NHANES2, "bll_mean", "mean")
NHANES3 <- fread("FILEPATH")
setnames(NHANES3, "bll_mean", "mean")
NHANES4 <- fread("FILEPATH")
setnames(NHANES4, "bll_mean", "mean")
NHANES5 <- fread("FILEPATH")
setnames(NHANES5, "bll_mean", "mean")
NHANES6 <- fread("FILEPATH")
setnames(NHANES6, "bll_mean", "mean")
NHANES7 <- fread("FILEPATH")
setnames(NHANES7, "bll_mean", "mean")
NHANES8 <- fread("FILEPATH")
setnames(NHANES8, "bll_mean", "mean")
NHANES9 <- fread("FILEPATH")
setnames(NHANES9, "bll_mean", "mean")
KNHANES1 <- fread("FILEPATH")
setnames(KNHANES1, "bll_mean", "mean")
KNHANES2 <- fread("FILEPATH")
setnames(KNHANES2, "bll_mean", "mean")
KNHANES3 <- fread("FILEPATH")
setnames(KNHANES3, "bll_mean", "mean")
KNHANES4 <- fread("FILEPATH")
setnames(KNHANES4, "bll_mean", "mean")
KNHANES5 <- fread("FILEPATH")
setnames(KNHANES5, "bll_mean", "mean")
KNHANES6 <- fread("FILEPATH")
setnames(KNHANES6, "bll_mean", "mean")
DEUENV <- fread("FILEPATH")
setnames(DEUENV, "bll_mean", "mean")
DELHIDHS <- fread("FILEPATH")
setnames(DELHIDHS, "bll_mean", "mean")
MUMBAIDHS <- fread("FILEPATH")
setnames(MUMBAIDHS, "bll_mean", "mean")
UZBDHS <- fread("FILEPATH")
setnames(UZBDHS, "bll_mean", "mean")

DELHIDHS[,location_id := 43880]
DELHIDHS[,ihme_loc_id := "IND_43880"]
DELHIDHS[,location_name := "Delhi, Urban|IND_43880"]

#***************************************************************************************************************

#append all the microdata together
dfdata <- rbindlist(list(NHANES1, NHANES2, NHANES3, NHANES4, NHANES5, NHANES6, NHANES7, NHANES8, NHANES9, 
                         KNHANES1, KNHANES2, KNHANES3, KNHANES4, KNHANES5, KNHANES6, DEUENV, DELHIDHS, MUMBAIDHS, UZBDHS), fill=T)
# format additional variables for microdata
dfdata[,group := 1]
dfdata[cv_mean_type == 1,specificity := "AM"]
dfdata[cv_mean_type == 1,group_review := 1]
dfdata[cv_mean_type == 0,specificity := "GM"]
dfdata[cv_mean_type == 0,group_review := 0]

# add in literature extractions
dfdata <- rbindlist(list(df16,df13,dfdata),fill=T)

#remove data that is orders of magnitude off from plausibility (or else it will skew subsequent crosswalking substantially)
dfdata <- dfdata[as.numeric(mean)<10000,]
dfdata <- dfdata[year_end > 1969,]

# additional formatting
dfdata <- merge(dfdata[,-c("location_id"),with=F],locs,by="ihme_loc_id",all.x=T)
dfdata[,measure := "continuous"]
dfdata[,year_id := floor((as.numeric(year_start) + as.numeric(year_end))/2)]

#write compiled sources to csv
write.csv(dfdata, "FILEPATH")

#***************************************************************************************************************

###########################################################
##
## Preparing inputs to Dismod ODE for generating age-sex trend
##
###########################################################

# Dismod ODE requires standard deviation for all observations, so this must be imputed where missing
# 1. When confidence interval is present (for arithmetic mean), can be used to calculates standard deviation. 
# 2. When only have mean and sample size, generate the global mean of the "coefficient of variation" (or 
#    at least, an approximation of it) in order to estimate the variance of those points
# Note: While geometric mean and median may have CI as well, standard deviation cannot be calculated directly from them.
# Already tested on a subset of GMs with both CI and SD which method got closer on average, and it was the coefficient 
# of variation method, so that's what I use here

dfdata[, standard_deviation := as.numeric(standard_deviation)]
dfdata[, lower := as.numeric(lower)]
dfdata[, upper := as.numeric(upper)]
dfdata[, sample_size := as.numeric(sample_size)]

# impute variance from standard errors and confidence intervals 
dfdata[,se2 := as.numeric(standard_deviation)]
dfdata[is.na(se2) & !is.na(lower) & cv_mean_type == 1,se2:= (upper-lower)/(2*1.96)]
dfdata[,se2:=se2**2]

# generate global mean of coefficient of variation
df <- data.table(cbind(data=as.numeric(dfdata$mean), sample_size=as.numeric(dfdata$sample_size), variance=as.numeric(dfdata$se2)))
## Compute standard deviation
df[!is.na(variance), standard_deviation := sqrt(sample_size * variance)]
## Take a global mean of the coefficient of variation
df[, cv_mean := mean(standard_deviation/data, na.rm=T)]
## Estimate missing standard_deviation using global average cv
df[is.na(standard_deviation), standard_deviation := data * cv_mean]
## Impute sample size using 5th percentile sample_size
sample_size_lower <- quantile(df[sample_size > 0, sample_size], 0.05, na.rm=T)
df <- df[is.na(sample_size) | sample_size==0, sample_size := sample_size_lower]
## Estimate variance as variance2 where missing
df <- df[is.na(variance), variance2 := standard_deviation^2/sample_size]

# fill in missing standard errors in data set
dfdata[is.na(se2), se2:= df[!is.na(data) & is.na(variance), variance2]]
dfdata[,standard_deviation:=sqrt(se2)]
dfdata[,se2:=NULL]

#save new data set with imputed standard errors
write.csv(dfdata, "FILEPATH")

#subset to only data with geometric means and save as an alternate version of data with imputed standard error
write.csv(dfdata[cv_mean_type == 0,], "FILEPATH")
