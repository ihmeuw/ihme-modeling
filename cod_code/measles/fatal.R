#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose: Model measles fatal and nonfatal outcomes for GBD
#          PART ONE -    Incidence natural history model
#          PART TWO -    Rescale case estimates
#          PART THREE -  Age-sex split cases
#          PART FOUR -   Calculate prevalence and incidence from split case estimates
#          PART FIVE -   Format for COMO and save results to the database
#          PART SIX -    Model fatal outcomes with negative binomial model
#          PART SEVEN -  Calculate deaths from CFR
#          PART EIGHT -  Rescale death estimates
#          PART NINE -   Age-sex split deaths
#          PART TEN -    Replace modeled death estimates in select locations with CODEm data-rich feeder model
#          PART ELEVEN - Format for codcorrect and save results to the database
#***********************************************************************************************************************


########################################################################################################################
##### START-UP #########################################################################################################
########################################################################################################################


#----CONFIG-------------------------------------------------------------------------------------------------------------
### load packages
pacman::p_load(magrittr, foreign, stats, MASS, data.table, dplyr, plyr, lme4, reshape2, parallel)
if (Sys.info()["sysname"] == "Linux") {
  require(mvtnorm, lib="FILEPATH")
  library(rhdf5, lib="FILEPATH")
} else { 
  pacman::p_load(mvtnorm, rhdf5)
}
#***********************************************************************************************************************


#----DIRECTORIES--------------------------------------------------------------------------------------------------------
### set objects
acause <- "measles"
age_start <- 4  #2           ## age "early neonatal"
age_end <- 16                ## age 55-59 years
a <- 3                       ## birth cohort years before 1980
cause_id <- 341
me_id <- 1436

### make folders on cluster
cl.death.dir <- file.path("FILEPATH")                                                  
dir.create(cl.death.dir, recursive = T)

cl.version.dir <- file.path("FILEPATH")                                            
dir.create(file.path(cl.version.dir), recursive = T)

### directories
home <- file.path(j_root, "FILEPATH") 
j.version.dir <- file.path(home, "models", custom_version)
j.version.dir.inputs <- file.path(j.version.dir, "model_inputs")
j.version.dir.logs <- file.path(j.version.dir, "model_logs")
dir.create(j.version.dir.inputs, recursive = T)
dir.create(j.version.dir.logs, recursive = T)

### save description of model run
write.table(description, file.path(j.version.dir, "MODEL_DESCRIPTION.txt"))
#***********************************************************************************************************************


#----FUNCTIONS----------------------------------------------------------------------------------------------------------  
### reading .h5 files
read_block_names <- function(str){
  # Use string splitting to get the column names of a value block
  split_vec <- strsplit(str, "'")[[1]]
  N <- length(split_vec)
  split_vec[seq(from=2, to=N, by=2)]
}
value_blocks <- function(attrib){
  # get all the value block names from teh attributes of an hdf5 table
  grep(glob2rx("values_block_*_kind"), names(attrib), value=TRUE)
}
h5_value_col_names <- function(h5File, key){
  # return a list where each element are the column names of the differnt
  # value block types i.e. float, int, string
  attrib <- h5readAttributes(h5File, paste0(key, "/table/"))
  value_block_names <- value_blocks(attrib)
  nam <- lapply(value_block_names, function(x) read_block_names(attrib[[x]]))
  names(nam) <- gsub("_kind", "", value_block_names)
  nam
}
mat_to_df <- function(mat, mat_names){
  # pytables hdf5 saves values as matrices when more than one column exists 
  # We need to transpose it then apply the names
  if (length(dim(mat)) == 1){
    mat <- matrix(data=mat, nrow=length(mat), ncol=length(mat_names))
  }
  else{
    mat <- t(mat)
  }
  df <- as.data.frame(mat)
  names(df) <- mat_names
  df
}
read_hdf5_table <- function(h5File, key){
  # read in the indices  values as well as the value blocks
  # only indices values may be strings
  data_list <- h5read(h5File, paste0(key, "/table/"), compoundAsDataFrame=F)
  data_value_names <- h5_value_col_names(h5File, key)
  indices <- setdiff(names(data_list), c("index", names(data_value_names)))
  df_index <- data.frame(data_list[indices])
  df_values <- lapply(names(data_value_names), function(x) 
    mat_to_df(data_list[[x]], data_value_names[[x]]))
  do.call(cbind, c(list(df_index), df_values))
}

### load shared functions
file.path(j_root, "FILEPATH/get_population.R") %>% source
file.path(j_root, "FILEPATH/get_location_metadata.R") %>% source
file.path(j_root, "FILEPATH/get_covariate_estimates.R") %>% source

### load personal functions
file.path(home, "FILEPATH/age_sex_split.R") %>% source
file.path(home, "FILEPATH/collapse_point_and_CI.R") %>% source
file.path(home, "FILEPATH/rescaling_function_GBD2016_with_UK.R") %>% source
#*********************************************************************************************************************** 


########################################################################################################################
##### PART ONE: INCIDENCE NAT HIST MODEL ###############################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
### get location data
locations <- get_location_metadata(gbd_round_id=4, location_set_id=22)
locations <- subset(locations, select=c("location_id", "ihme_loc_id", "location_name", "region_id", "super_region_id", "level", "location_type", "parent_id"))
pop_locs <- unique(locations$location_id)

### read population file
# get population for birth cohort at average age of notification by year
population_young <- get_population(location_id=pop_locs, year_id=(1980-a):2016, age_group_id=2:4, status="best") #, process_version_map_id=11)
population_young$year_id <- population_young$year_id + a
population_young <- subset(population_young, year_id <= 2016)
# collapse by location-year
sum_pop_young <- ddply(population_young, .(location_id, year_id), summarise, pop=sum(population) )

# get all population
population_all <- get_population(location_id=pop_locs, year_id=1980:2016, age_group_id=age_start:age_end, sex_id=2, status="best") #, process_version_map_id=11)
population_all[population_all$age_group_id %in% c(2,3,4), "age_group_id"] <- 0
# collapse by location-year-age
sum_pop_all <- ddply(population_all, c("location_id", "year_id", "age_group_id"), summarise, pop=sum(population) )

### get updated covariate
# covariate: measles_vacc_cov_prop, covariate_id=75
covs1 <- get_covariate_estimates(covariate_name_short="measles_vacc_cov_prop")
covs1 <- subset(covs1, select=c("location_id", "year_id", "mean_value"))
covs1$ln_unvacc <- log(1-covs1$mean_value)

# covariate: MCV2 coverage, covariate_id=1108
mcv2 <- get_covariate_estimates(covariate_name_short="measles_vacc_cov_prop_2")
mcv2$ln_unvacc_mcv2 <- log(1 - mcv2$mean_value)
mcv2 <- subset(mcv2, select=c("location_id", "year_id", "ln_unvacc_mcv2"))

# merge together MCV1 and MCV2
covs_both <- merge(covs1, mcv2, by=c("location_id", "year_id"), all.x=TRUE) 

if (MCV_lags == "yes") {
    
  # calculate proportion of people receiving ONLY mcv1 versus mcv2
  covs_both$ln_unvacc_mcv1_only <- log( 1 - (covs_both$mean_value - covs_both$gpr_mean) )
  covs_both$ln_unvacc_mcv2_only <- log(1 - mcv2$gpr_mean)

  # lags on MCV coverage
  mcv_lag_1 <- subset(covs_both, select=c("location_id", "year_id", "ln_unvacc", "ln_unvacc_mcv2"))
  colnames(mcv_lag_1)[colnames(mcv_lag_1)=="ln_unvacc"] <- "mcv1_lag_1"
  colnames(mcv_lag_1)[colnames(mcv_lag_1)=="ln_unvacc_mcv2"] <- "mcv2_lag_1"
  mcv_lag_1$year_id_lag <- mcv_lag_1$year_id + 1
  mcv_lag_1 <- subset(mcv_lag_1, select=c("location_id", "year_id_lag", "mcv1_lag_1", "mcv2_lag_1"))
  colnames(mcv_lag_1)[colnames(mcv_lag_1)=="year_id_lag"] <- "year_id"

  mcv_lag_2 <- subset(covs_both, select=c("location_id", "year_id", "ln_unvacc", "ln_unvacc_mcv2"))
  colnames(mcv_lag_2)[colnames(mcv_lag_2)=="ln_unvacc"] <- "mcv1_lag_2"
  colnames(mcv_lag_2)[colnames(mcv_lag_2)=="ln_unvacc_mcv2"] <- "mcv2_lag_2"
  mcv_lag_2$year_id_lag <- mcv_lag_2$year_id + 2
  mcv_lag_2 <- subset(mcv_lag_2, select=c("location_id", "year_id_lag", "mcv1_lag_2", "mcv2_lag_2"))
  colnames(mcv_lag_2)[colnames(mcv_lag_2)=="year_id_lag"] <- "year_id"

  mcv_lag_3 <- subset(covs_both, select=c("location_id", "year_id", "ln_unvacc", "ln_unvacc_mcv2"))
  colnames(mcv_lag_3)[colnames(mcv_lag_3)=="ln_unvacc"] <- "mcv1_lag_3"
  colnames(mcv_lag_3)[colnames(mcv_lag_3)=="ln_unvacc_mcv2"] <- "mcv2_lag_3"
  mcv_lag_3$year_id_lag <- mcv_lag_3$year_id + 3
  mcv_lag_3 <- subset(mcv_lag_3, select=c("location_id", "year_id_lag", "mcv1_lag_3", "mcv2_lag_3"))
  colnames(mcv_lag_3)[colnames(mcv_lag_3)=="year_id_lag"] <- "year_id"

  mcv_lag_4 <- subset(covs_both, select=c("location_id", "year_id", "ln_unvacc", "ln_unvacc_mcv2"))
  colnames(mcv_lag_4)[colnames(mcv_lag_4)=="ln_unvacc"] <- "mcv1_lag_4"
  colnames(mcv_lag_4)[colnames(mcv_lag_4)=="ln_unvacc_mcv2"] <- "mcv2_lag_4"
  mcv_lag_4$year_id_lag <- mcv_lag_4$year_id + 4
  mcv_lag_4 <- subset(mcv_lag_4, select=c("location_id", "year_id_lag", "mcv1_lag_4", "mcv2_lag_4"))
  colnames(mcv_lag_4)[colnames(mcv_lag_4)=="year_id_lag"] <- "year_id"

  mcv_lag_5 <- subset(covs_both, select=c("location_id", "year_id", "ln_unvacc", "ln_unvacc_mcv2"))
  colnames(mcv_lag_5)[colnames(mcv_lag_5)=="ln_unvacc"] <- "mcv1_lag_5"
  colnames(mcv_lag_5)[colnames(mcv_lag_5)=="ln_unvacc_mcv2"] <- "mcv2_lag_5"
  mcv_lag_5$year_id_lag <- mcv_lag_5$year_id + 5
  mcv_lag_5 <- subset(mcv_lag_5, select=c("location_id", "year_id_lag", "mcv1_lag_5", "mcv2_lag_5"))
  colnames(mcv_lag_5)[colnames(mcv_lag_5)=="year_id_lag"] <- "year_id"

  mcv_lag_1$mcv1_lag_1[is.na(mcv_lag_1$mcv1_lag_1)] <- log(1)
  mcv_lag_2$mcv1_lag_2[is.na(mcv_lag_2$mcv1_lag_2)] <- log(1)
  mcv_lag_3$mcv1_lag_3[is.na(mcv_lag_3$mcv1_lag_3)] <- log(1)
  mcv_lag_4$mcv1_lag_4[is.na(mcv_lag_4$mcv1_lag_4)] <- log(1)
  mcv_lag_5$mcv1_lag_5[is.na(mcv_lag_5$mcv1_lag_5)] <- log(1)

  mcv_lag_1$mcv2_lag_1[is.na(mcv_lag_1$mcv2_lag_1)] <- log(1)
  mcv_lag_2$mcv2_lag_2[is.na(mcv_lag_2$mcv2_lag_2)] <- log(1)
  mcv_lag_3$mcv2_lag_3[is.na(mcv_lag_3$mcv2_lag_3)] <- log(1)
  mcv_lag_4$mcv2_lag_4[is.na(mcv_lag_4$mcv2_lag_4)] <- log(1)
  mcv_lag_5$mcv2_lag_5[is.na(mcv_lag_5$mcv2_lag_5)] <- log(1)

  covs_both <- merge(covs_both, mcv_lag_1, by=c("location_id", "year_id"), all.x=TRUE)
  covs_both <- merge(covs_both, mcv_lag_2, by=c("location_id", "year_id"), all.x=TRUE)
  covs_both <- merge(covs_both, mcv_lag_3, by=c("location_id", "year_id"), all.x=TRUE)
  covs_both <- merge(covs_both, mcv_lag_4, by=c("location_id", "year_id"), all.x=TRUE)
  covs <- merge(covs_both, mcv_lag_5, by=c("location_id", "year_id"), all.x=TRUE)
} else { 
    covs <- covs_both 
}

### read in corrected SIA coverage
SIA <- read.csv(file.path(home, "data", "corrected_SIA_input_data.csv"))
colnames(SIA)[colnames(SIA)=="corrected_target_coverage"] <- "supp"

# don't want coverage >100%; if campaign targeted entire population rather than just <15, this would be possible in our measure
SIA$supp[SIA$supp > 1] <- 0.99

# apply SIAs in Sudan pre-2011 to South Sudan
SIA_s_sudan <- subset(SIA, location_id==522 & year_id < 2011)
SIA_s_sudan$location_id <- 435
SIA <- rbind(SIA, SIA_s_sudan)

# apply SIAs to subnational units
SIA_sub <- SIA
SIA_sub$parent_id <- SIA_sub$location_id
# make df of unique subnational locations 
loc_subs <- locations[!locations$location_type %in% c("admin0", "global", "superregion", "region"), c("location_id", "parent_id")]
# subset SIAs to only admin0 locations with subnational units
SIA_sub <- SIA_sub[SIA_sub$location_id %in% unique(loc_subs$parent_id), ]
SIA_sub <- subset(SIA_sub, select=c("year_id", "parent_id", "supp"))

# level 1
subs_level1 <- merge(SIA_sub, loc_subs, by="parent_id", all.x=TRUE)

# level 2
subs_level2 <- subs_level1
subs_level2$parent_id <- subs_level2$location_id
subs_level2 <- subset(subs_level2, select=c("year_id", "parent_id", "supp"))
subs_level2 <- merge(subs_level2, loc_subs, by="parent_id", all.x=TRUE)
subs_level2 <- subs_level2[!is.na(subs_level2$location_id), ]

# level 3
subs_level3 <- subs_level2
subs_level3$parent_id <- subs_level3$location_id
subs_level3 <- subset(subs_level3, select=c("year_id", "parent_id", "supp"))
subs_level3 <- merge(subs_level3, loc_subs, by="parent_id", all.x=TRUE)
subs_level3 <- subs_level3[!is.na(subs_level3$location_id), ]

# remove parent_id column
subs_level1 <- subs_level1[, colnames(subs_level1) != "parent_id"]
subs_level2 <- subs_level2[, colnames(subs_level2) != "parent_id"]
subs_level3 <- subs_level3[, colnames(subs_level3) != "parent_id"]

# bring together SIAs for all subnational levels
SIA_subs <- bind_rows(subs_level1, subs_level2, subs_level3)

# add subnational SIAs to national SIA df
SIA <- bind_rows(SIA, SIA_subs)
SIA <- SIA[!duplicated(SIA[, c("location_id", "year_id")]), ]

# add lag to SIA, 1 to 5 years
for (i in 1:5) {
  assign(paste("lag", i, sep="_"), SIA)
}
lag_1$year_id <- lag_1$year_id + 1
lag_1$supp_1 <- lag_1$supp
lag_2$year_id <- lag_2$year_id + 2
lag_2$supp_2 <- lag_2$supp
lag_3$year_id <- lag_3$year_id + 3
lag_3$supp_3 <- lag_3$supp
lag_4$year_id <- lag_4$year_id + 4
lag_4$supp_4 <- lag_4$supp
lag_5$year_id <- lag_5$year_id + 5
lag_5$supp_5 <- lag_5$supp

# bring lags together
SIAs <- join_all(list(SIA, lag_1[, c("location_id", "year_id", "supp_1")], lag_2[, c("location_id", "year_id", "supp_2")],
                     lag_3[, c("location_id", "year_id", "supp_3")], lag_4[, c("location_id", "year_id", "supp_4")], 
                     lag_5[, c("location_id", "year_id", "supp_5")]), by=c("location_id", "year_id"), type="full")
# make missing SIAs 0
SIAs$supp_1[is.na(SIAs$supp_1)] <- 0
SIAs$supp_2[is.na(SIAs$supp_2)] <- 0
SIAs$supp_3[is.na(SIAs$supp_3)] <- 0
SIAs$supp_4[is.na(SIAs$supp_4)] <- 0
SIAs$supp_5[is.na(SIAs$supp_5)] <- 0

### prep incidence data
# get WHO case notification data, downloaded from http://www.who.int/immunization/monitoring_surveillance/data/en/
case_notif <- read.csv(file.path(home, "data", "WHO_incidence_series_measles_compatible.csv"))
names(case_notif)[names(case_notif)=="ISO_code"] <- "ihme_loc_id"
case_notif <- case_notif[, c("ihme_loc_id", colnames(case_notif)[grepl("X", colnames(case_notif))])]
case_notif <- melt(case_notif, id.var="ihme_loc_id")
names(case_notif)[names(case_notif)=="variable"] <- "year_id"
names(case_notif)[names(case_notif)=="value"] <- "cases"
case_notif$year_id <- gsub("X", "",  case_notif$year_id)
case_notif$year_id <- as.numeric(case_notif$year_id)

# merge on location_id, region, and super region
regress <- merge(case_notif, subset(locations, select=c("location_id", "ihme_loc_id", "region_id", "super_region_id")), 
                 by="ihme_loc_id", all.x=TRUE)
# merge on population (NOTE: this is JUST the population of the birth cohort at the average age of notification--our fix for underreporting)
regress <- join(regress, sum_pop_young, by=c("location_id", "year_id"), type="inner")
# merge on ihme covariates and WHO SIA data
regress <- join(regress, covs, by=c("location_id", "year_id"), type="inner")
regress <- join(regress, SIAs[, c("location_id", "year_id", colnames(SIAs)[grepl("supp", colnames(SIAs))])], by=c("location_id", "year_id"), type="left")

# fix missing lags
regress[grepl("supp", colnames(regress))][is.na(regress[grepl("supp", colnames(regress))])] <- 0

### set up regression
# incidence rate generated from the population just for the 1 year birth cohort at average age of notification
regress$inc_rate <- (regress$cases / regress$pop) * 100000
regress <- regress[!is.na(regress$inc_rate), ]
# because of the 1-year population, some unrealisticly high incidence rates generated for places with good notification and outbreaks 
# drop these high outliers that suggest >95% of the population of the entire country got measles
regress <- regress[regress$inc_rate <= 95000, ]
# outlier PNG
regress <- regress[!(regress$ihme_loc_id=="PNG" & regress$year_id==2013), ]
regress <- regress[!(regress$ihme_loc_id=="PNG" & regress$year_id==2014), ]

### generate transformed variables
# proportion of population unvaccinate
regress$ln_inc <- log(regress$inc_rate)
regress <- do.call(data.frame, lapply(regress, function(x) replace(x, is.infinite(x), NA))) 

# set up factors for regression
regress$super_region <- as.factor(regress$super_region)
regress$region <- as.factor(regress$region)
regress$location_id <- as.factor(regress$location_id)

### add herd immunity covariate
regress$herd_imm_95_mcv1 <- 0
regress$herd_imm_95[regress$mean_value >= 0.95] <- 1

regress$herd_imm_95_mcv2 <- 0
regress$herd_imm_95_mcv2[regress$ln_unvacc_mcv2 < log(0.05)] <- 1

# we found older data to be less reliable - we tested cutting off data at 1980, 1985, 1990, and 1995 and found 1995 gave most expected 
# relationship between vaccination and incidence
regress <- subset(regress, year_id >= 1995)

#regress <- na.omit(regress)
regress[grepl("supp_", colnames(regress))][regress[grepl("supp_", colnames(regress))]==0] <- 0.000000001

# save model input
write.csv(regress, file.path(j.version.dir.inputs, "case_regression_input.csv"), row.names=FALSE)
#***********************************************************************************************************************


#----MIXED EFFECTS MODEL------------------------------------------------------------------------------------------------
### run mixed effects regression model
me_model <- lmer(ln_inc ~ ln_unvacc + ln_unvacc_mcv2 + supp_1 + supp_2 + supp_3 + supp_4 + supp_5 + 
                (1 | super_region_id) + (1 | region_id) + (1 | location_id), data=regress)
coef(summary(me_model))

# save log
capture.output(summary(me_model), file = file.path(j.version.dir.logs, "log_incidence_mereg.txt"), type="output")
#***********************************************************************************************************************  


#----DRAWS--------------------------------------------------------------------------------------------------------------
### prep prediction data
# merge ihme_loc_id, population (NOTE: this is JUST the population of the birth cohort at the average age of notification--our 
# fix for underreporting), ihme covariates, and WHO SIA data
draws <- merge(covs, subset(locations, select=c("location_id", "ihme_loc_id", 
                                     "region_id", "super_region_id")), by="location_id", all.x=TRUE)
draws <- merge(draws, sum_pop_young, by=c("location_id", "year_id"), all.x=TRUE)
draws <- merge(draws, SIAs[, c("location_id", "year_id", colnames(SIAs)[grepl("supp", colnames(SIAs))])], 
              by=c("location_id", "year_id"), all.x=TRUE)

# fix missing lags
draws$supp_1[is.na(draws$supp_1)] <- 0
draws$supp_2[is.na(draws$supp_2)] <- 0
draws$supp_3[is.na(draws$supp_3)] <- 0
draws$supp_4[is.na(draws$supp_4)] <- 0
draws$supp_5[is.na(draws$supp_5)] <- 0

if (MCV1_or_MCV2 == "use_MCV2") {

  ### 1000 draws for uncertainty - with MCV2
  #coefficient matrix
  beta0 <- fixef(me_model)[[1]] %>% data.frame
  beta1 <- coef(me_model)[[1]] %>% data.frame
  beta1 <- beta1[, !colnames(beta1)=="X.Intercept."]
  beta1 <- beta1[1, ]
  coeff <- cbind(beta0, beta1)
  colnames(coeff) <- c("constant", "b_ln_unvacc", "b_ln_unvacc_mcv2", "b_lag_1", "b_lag_2", "b_lag_3", "b_lag_4", "b_lag_5")
  coefmat <- matrix(unlist(coeff), ncol=8, byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coeff)))))

  # generate a standard random effect
  # set such that 95% of unvaccinated individuals will get measles (95% attack rate)
  # given 0% vaccinated, the combination of the RE and the constant will lead to an incidence of 95,000/100,000
  standard_RE <- log(95000) - coefmat["coef", "constant"]

  # covariance matrix
  vcovmat <- vcov(me_model)
  vcovlist <- NULL
  for (ii in 1:8) {
      vcovlist_a <- c(vcovmat[ii,1], vcovmat[ii,2], vcovmat[ii,3], vcovmat[ii,4], vcovmat[ii,5], vcovmat[ii,6], vcovmat[ii,7], vcovmat[ii,8])
      vcovlist <- c(vcovlist, vcovlist_a)
  }
  vcovmat2 <- matrix(vcovlist, ncol=8, byrow=TRUE)

  # create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
  betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat2)

  # transpose coefficient matrix
  betas <- t(betadraws)

  for (i in 0:999) { 
    draws[, paste0("case_draw_", i)] <- exp(  betas[1, (i+1)] + 
                                            ( betas[2, (i+1)] * draws$ln_unvacc ) +
                                            ( betas[3, (i+1)] * draws$ln_unvacc_mcv2 ) +
                                            ( betas[4, (i+1)] * draws$supp_1 ) +
                                            ( betas[5, (i+1)] * draws$supp_2 ) +
                                            ( betas[6, (i+1)] * draws$supp_3 ) +
                                            ( betas[7, (i+1)] * draws$supp_4 ) +
                                            ( betas[8, (i+1)] * draws$supp_5 ) + standard_RE ) *  
                                            ( draws$pop / 100000 )
  }
    
} else if (MCV1_or_MCV2 == "MCV1_only") {

  ### 1000 draws for uncertainty 
  #coefficient matrix
  beta0 <- fixef(me_model)[[1]] %>% data.frame
  beta1 <- coef(me_model)[[1]] %>% data.frame
  beta1 <- beta1[, !colnames(beta1)=="X.Intercept."]
  beta1 <- beta1[1, ]
  coeff <- cbind(beta0, beta1)
  colnames(coeff) <- c("constant", "b_ln_unvacc", "b_lag_1", "b_lag_2", "b_lag_3", "b_lag_4", "b_lag_5")
  coefmat <- matrix(unlist(coeff), ncol=7, byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coeff)))))

  # generate a standard random effect
  # set such that 95% of unvaccinated individuals will get measles (95% attack rate)
  # given 0% vaccinated, the combination of the RE and the constant will lead to an incidence of 95,000/100,000
  standard_RE <- log(95000) - coefmat["coef", "constant"]

  # covariance matrix
  vcovmat <- vcov(me_model)
  vcovlist <- NULL
  for (ii in 1:7) {
      vcovlist_a <- c(vcovmat[ii,1], vcovmat[ii,2], vcovmat[ii,3], vcovmat[ii,4], vcovmat[ii,5], vcovmat[ii,6], vcovmat[ii,7])
      vcovlist <- c(vcovlist, vcovlist_a)
  }
  vcovmat2 <- matrix(vcovlist, ncol=7, byrow=TRUE)

  # create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
  betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat2)

  # transpose coefficient matrix
  betas <- t(betadraws)

  # combine betas and predictions into one data frame
  for (beta in 1:7) {
      for (i in 0:999) {
          draws[, paste0("beta", beta, "_", i)] <- betas[beta, (i+1)]
      }
  }

  # generate draws of the prediction using coefficient draws
  draws <- draws %>% data.frame
  for (i in 0:999) { 
    draws[, paste0("case_draw_", i)] <- exp( draws[, paste0("beta1_", i)] + 
                                            ( draws[, paste0("beta2_", i)] * draws$ln_unvacc ) +
                                            ( draws[, paste0("beta3_", i)] * draws$supp_1 ) +
                                            ( draws[, paste0("beta4_", i)] * draws$supp_2 ) +
                                            ( draws[, paste0("beta5_", i)] * draws$supp_3 ) +
                                            ( draws[, paste0("beta6_", i)] * draws$supp_4 ) +
                                            ( draws[, paste0("beta7_", i)] * draws$supp_5 ) + standard_RE ) *  
                                            ( draws$pop / 100000 )
  }
    
}   
#***********************************************************************************************************************


#----SAVE---------------------------------------------------------------------------------------------------------------
# save results
draws <- subset(draws, select=c("location_id", "ihme_loc_id", "year_id", "region_id", "super_region_id", 
                                colnames(draws)[grepl("case_draw_", colnames(draws))]))

if (WRITE_FILES == "yes") {
  write.csv(draws[, c("location_id", "year_id", colnames(draws)[grepl("case_draw_", colnames(draws))])], 
            file.path(j.version.dir, "01_case_predictions_from_model_no_fix.csv"), row.names=FALSE)
}
#***********************************************************************************************************************


#----FIX UNDER-REPORTING------------------------------------------------------------------------------------------------
### assume cases reported well in WHO case notifications in some super regions -- use model for the others
# save estimated cases draws for "BMU", "GRL", "PRI" & "VIR" because there is no WHO notification data for these countries
draws_fix <- draws
draws_no_notif <- draws_fix[draws_fix$ihme_loc_id %in% c("BMU", "GRL", "PRI", "VIR"), ]
draws_no_notif <- draws_no_notif[, c("location_id", "year_id", colnames(draws_no_notif)[grep("case_draw_", colnames(draws_no_notif))])]

### fixes for missingness
# recreate case notification dataframe for fixing
pop_u5 <- get_population(location_id=pop_locs, year_id=1980:2016, age_group_id=2:5) #, process_version_map_id=11)
pop_u5 <- subset(pop_u5, select=c("location_id", "year_id", "population"))
pop_u5 <- ddply(pop_u5, c("location_id", "year_id"), summarise, pop=sum(population) )

# keep only countries from 3 super regions: high income, eastern europe/central asia, latin america/caribbean
sr_64_31_103 <- locations[locations$super_region_id %in% c(64, 31, 103), ]
notif_fix <- join(case_notif, sr_64_31_103, by="ihme_loc_id", type="inner")
# make sure 2016 is there
notif_fix_2016 <- data.frame(ihme_loc_id=unique(notif_fix$ihme_loc_id), year_id=2016)
notif_fix_2016 <- join(notif_fix_2016, sr_64_31_103, by="ihme_loc_id", type="inner")
notif_fix <- bind_rows(notif_fix, notif_fix_2016)

# calculate incidence rate from case notification and population data
notif_fix <- merge(notif_fix, pop_u5, by=c("location_id", "year_id"), all.x=TRUE)
notif_fix$inc_rate <- notif_fix$cases / notif_fix$pop

# get previous and next year estimates for stand-alone missing years
# previous year
notif_fix$year_id <- as.integer(notif_fix$year_id)
notif_fix_previous <- notif_fix
notif_fix_previous$year_id <- notif_fix_previous$year_id + 1
colnames(notif_fix_previous)[colnames(notif_fix_previous)=="inc_rate"] <- "prev_year_inc"
notif_fix_previous <- subset(notif_fix_previous, select=c("ihme_loc_id", "year_id", "prev_year_inc"))
# next year                                   
notif_fix_next <- notif_fix
notif_fix_next$year_id <- notif_fix_next$year_id - 1
colnames(notif_fix_next)[colnames(notif_fix_next)=="inc_rate"] <- "next_year_inc"
notif_fix_next <- subset(notif_fix_next, select=c("ihme_loc_id", "year_id", "next_year_inc"))
# 2016 incidence
notif_fix_2015 <- notif_fix[notif_fix$year_id==2015, ]
notif_fix_2015 <- notif_fix_2015[, c("ihme_loc_id", "inc_rate")]
colnames(notif_fix_2015)[colnames(notif_fix_2015)=="inc_rate"] <- "inc_2015"

# merge together
notif_fix_missings <- merge(notif_fix, notif_fix_previous, by=c("ihme_loc_id", "year_id"), all.x=TRUE)
notif_fix_missings <- merge(notif_fix_missings, notif_fix_next, by=c("ihme_loc_id", "year_id"), all.x=TRUE)
notif_fix_missings <- merge(notif_fix_missings, notif_fix_2015, by="ihme_loc_id", all.x=TRUE)

notif_fix_missings$inc_rate[is.na(notif_fix_missings$inc_rate)] <- 
                                            notif_fix_missings$prev_year_inc[is.na(notif_fix_missings$inc_rate)]
notif_fix_missings$inc_rate[is.na(notif_fix_missings$inc_rate)] <- 
                                            notif_fix_missings$next_year_inc[is.na(notif_fix_missings$inc_rate)]
notif_fix_missings$inc_rate[is.na(notif_fix_missings$inc_rate) & notif_fix_missings$year_id==2016] <- 
                notif_fix_missings$inc_2015[is.na(notif_fix_missings$inc_rate) & notif_fix_missings$year_id==2016]
notif_fix_missings$inc_rate[notif_fix_missings$location_id==102 & notif_fix_missings$year_id==2016] <-
                notif_fix_missings$inc_rate[notif_fix_missings$location_id==102 & notif_fix_missings$year_id==2015]

# use parent and super region incidences if still missing
collapsed_notif_reg <- ddply(notif_fix_missings, c("region_id", "year_id", "super_region_id"), summarise, reg_rate=mean(inc_rate, na.rm=TRUE))
collapsed_notif_sr <- ddply(notif_fix_missings, c("super_region_id", "year_id"), summarise, sr_rate=mean(inc_rate, na.rm=TRUE))
#collapsed_notif_sr <- ddply(collapsed_notif_reg, c("super_region_id", "year_id"), summarise, sr_rate=mean(reg_rate, na.rm=TRUE))
notif_fix_missings <- merge(notif_fix_missings, collapsed_notif_reg[, c("region_id", "year_id", "reg_rate")], 
                                            by=c("region_id", "year_id"), all.x=TRUE)
notif_fix_missings <- merge(notif_fix_missings, collapsed_notif_sr[, c("super_region_id", "year_id", "sr_rate")], 
                                            by=c("super_region_id", "year_id"), all.x=TRUE)
notif_fix_missings[is.na(notif_fix_missings$inc_rate), "inc_rate"] <- 
                                            notif_fix_missings[is.na(notif_fix_missings$inc_rate), "reg_rate"]
notif_fix_missings[is.na(notif_fix_missings$inc_rate), "inc_rate"] <- 
                                            notif_fix_missings[is.na(notif_fix_missings$inc_rate), "sr_rate"]

### generate error
notif_fix_missings$SE <- sqrt( (notif_fix_missings$inc_rate * (1 - notif_fix_missings$inc_rate)) / notif_fix_missings$pop )

# calculate region and super-region average error for country-years with 0 cases reported
notif_error_reg <- ddply(notif_fix_missings, c("region_id", "year_id"), summarise, reg_SE=mean(SE, na.rm=TRUE))
notif_error_sr <- ddply(notif_fix_missings, c("super_region_id", "year_id"), summarise, sr_SE=mean(SE, na.rm=TRUE))

# add on region and super-region mean error, replace with these errors if country-level error is zero
notif_fix_missings <- merge(notif_fix_missings, notif_error_reg, by=c("region_id", "year_id"), all.x=TRUE)
notif_fix_missings <- merge(notif_fix_missings, notif_error_sr, by=c("super_region_id", "year_id"), all.x=TRUE)
notif_fix_missings$SE[notif_fix_missings$SE==0] <- notif_fix_missings$reg_SE[notif_fix_missings$SE==0]
notif_fix_missings$SE[notif_fix_missings$SE==0] <- notif_fix_missings$sr_SE[notif_fix_missings$SE==0]

# if region and super region error is zero, calculate non-zero SE 
notif_fix_missings$SE[notif_fix_missings$SE==0] <- sqrt( ((1 / notif_fix_missings$pop[notif_fix_missings$SE==0]) * 
                                                           notif_fix_missings$inc_rate[notif_fix_missings$SE==0] *
                                                          (1 - notif_fix_missings$inc_rate[notif_fix_missings$SE==0])) + 
                                                         ((1 / (4 * (notif_fix_missings$pop[notif_fix_missings$SE==0])^2)) * 
                                                          (qnorm(0.975))^2) )

# generate 1000 draws of incidence from error term
incdraws <- rnorm(n=1000*length(notif_fix_missings$inc_rate), mean=notif_fix_missings$inc_rate, sd=notif_fix_missings$SE)
inc_draws <- matrix(incdraws, ncol=1000, byrow=FALSE) %>% data.frame
colnames(inc_draws) <- paste0("inc_draw_", 0:999)
notif_fix_missings <- cbind(notif_fix_missings, inc_draws)

# calculate cases from incidence and population
for (i in 0:999) {
    notif_fix_missings[notif_fix_missings[, paste0("inc_draw_", i)] < 0, paste0("inc_draw_", i)] <- 0
    notif_fix_missings[, paste0("case_draw_", i)] <- notif_fix_missings[, paste0("inc_draw_", i)] * notif_fix_missings$pop
}

### bring together modeled case draws and notification case draws
notif_with_draws <- notif_fix_missings[, c("location_id", "year_id", colnames(notif_fix_missings)[grep("case_draw_", colnames(notif_fix_missings))])]
modeled_draws <- subset(draws, select=c("location_id", "year_id", colnames(draws)[grep("case_draw_", colnames(draws))]))

# remove modeled draws from 3 super regions
not_modeled <- unique(notif_with_draws$location_id)
modeled_draws <- modeled_draws[!modeled_draws$location_id %in% not_modeled, ]
combined_case_draws <- rbind(modeled_draws, notif_with_draws)
#***********************************************************************************************************************


#----SAVE---------------------------------------------------------------------------------------------------------------
# keep only required locations
needed_csv <- read.csv(file.path(j_root, "temp/USER/location_ids_needed.csv"))
needed_ids <- unique(needed_csv$location_id)

if (WRITE_FILES == "yes") {
  write.csv(combined_case_draws, file.path(j.version.dir, "02_case_predictions_fixed_for_rescaling.csv"), row.names=FALSE)
}
#*********************************************************************************************************************** 


########################################################################################################################
##### PART TWO: RESCALE CASES ##########################################################################################
########################################################################################################################


#----RESCALE------------------------------------------------------------------------------------------------------------
### run custom rescale function
combined_case_draws <- combined_case_draws %>% data.frame

rescale_GBD2016(input_file=combined_case_draws, measure="case")
rescaled_cases <- rescaled_draws
#*********************************************************************************************************************** 


#----FIX SUBNATIONAL CASE NOTIFICATION----------------------------------------------------------------------------------
### replace USA and JPN subnationals with data we have
JPN_2012 <- read.csv(file.path(home, "FILEPATH/JPN_subnational_notifications_2012.csv"))
JPN_2012$year_id <- 2012
JPN_2013 <- read.csv(file.path(home, "FILEPATH/JPN_subnational_notifications_2013.csv"))
JPN_2013$year_id <- 2013
JPN_2014 <- read.csv(file.path(home, "FILEPATH/JPN_subnational_notifications_2014.csv"))
JPN_2014$year_id <- 2014

JPN_sub_notif <- bind_rows(JPN_2012, JPN_2013, JPN_2014)
JPN_sub_notif <- merge(JPN_sub_notif, subset(locations, select=c("location_name", "location_id")), by="location_name", all.x=TRUE)
JPN_sub_notif <- JPN_sub_notif[, c("location_id", "year_id", "cases")]

USA_sub_notif <- read.csv(file.path(home, "FILEPATH/US_subnational_notifications_compatible.csv"))
USA_sub_notif <- USA_sub_notif[, c("location_id", "year_end", "cases")]
colnames(USA_sub_notif)[colnames(USA_sub_notif)=="year_end"] <- "year_id"

# bring JPN and USA together
new_subs <- rbind(JPN_sub_notif, USA_sub_notif)

new_subs <- na.omit(new_subs)
new_subs <- merge(new_subs, pop_u5, by=c("location_id", "year_id"))
new_subs$inc_rate <- new_subs$cases / new_subs$pop

# generate error
new_subs$SE <- sqrt( (new_subs$inc_rate * (1 - new_subs$inc_rate)) / new_subs$pop )
new_subs$SE[new_subs$cases==0] <- sqrt( ((1 / new_subs$pop[new_subs$cases==0]) * new_subs$inc_rate[new_subs$cases==0] *
                      (1 - new_subs$inc_rate[new_subs$cases==0])) +  ((1 / (4 * (new_subs$pop[new_subs$cases==0])^2)) * (qnorm(0.975))^2) )

# generate 1000 draws of incidence from error term
incdraws_subs <- rnorm(n=1000*length(new_subs$inc_rate), mean=new_subs$inc_rate, sd=new_subs$SE)
inc_draws_subs <- matrix(incdraws_subs, ncol=1000, byrow=FALSE) %>% data.frame
colnames(inc_draws_subs) <- paste0("inc_draw_", 0:999)
new_subs <- cbind(new_subs, inc_draws_subs)

# calculate cases from incidence and population
for (i in 0:999) {
    new_subs[new_subs[, paste0("inc_draw_", i)] < 0, paste0("inc_draw_", i)] <- 0
    new_subs[, paste0("case_draw_", i)] <- new_subs[, paste0("inc_draw_", i)] * new_subs$pop
}

new_subs <- new_subs[, c("location_id", "year_id", colnames(new_subs)[grep("case_draw_", colnames(new_subs))])]
# make df of location-years to remove from modeled subnational estimates
case_subs <- data.frame(location_id=new_subs$location_id, year_id=new_subs$year_id, drop=1)

rescaled_cases_sub <- merge(rescaled_cases, case_subs, by=c("location_id", "year_id"), all.x=TRUE)
rescaled_cases_sub <- rescaled_cases_sub[is.na(rescaled_cases_sub$drop), ]
rescaled_cases_sub <- rescaled_cases_sub[, colnames(rescaled_cases_sub) != "drop"]

rescaled_cases_sub <- rbind(rescaled_cases_sub, new_subs)
#*********************************************************************************************************************** 


#----FIX AND SAVE-------------------------------------------------------------------------------------------------------
needed_csv <- read.csv(file.path(j_root, "FILEPATH/location_ids_needed.csv"))
needed_ids <- unique(needed_csv$location_id)
needed_ids[!needed_ids %in% unique(rescaled_cases_sub$location_id)]

# save results
if (WRITE_FILES == "yes") {
  write.csv(rescaled_cases_sub, file.path(j.version.dir, "03_case_predictions_rescaled_for_split.csv"), row.names=FALSE)
}
#*********************************************************************************************************************** 


if (CALCULATE_NONFATAL == "yes") {

########################################################################################################################
##### PART THREE: CASES AGE-SEX SPLIT ##################################################################################
########################################################################################################################


#----SPLIT--------------------------------------------------------------------------------------------------------------
age_sex_split(acause=acause, input_file=rescaled_cases_sub, measure="case")
split_cases <- split_data_frame
#*********************************************************************************************************************** 


#----SAVE---------------------------------------------------------------------------------------------------------------
if (WRITE_FILES == "yes") {
  # save split draws
  write.csv(subset(split_cases, select=c("location_id", "year_id", "age_group_id", "sex_id", "population",
            colnames(split_cases)[grep("split_case_draw_", colnames(split_cases))])), file.path(j.version.dir, "04_split_case_draws.csv"), 
            row.names=FALSE)
}
#*********************************************************************************************************************** 


########################################################################################################################
##### PART FOUR: CASES TO PREVALENCE AND INCIDENCE #####################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
predictions_conversion <- subset(split_cases, select=c("location_id", "year_id", "age_group_id", "sex_id",
                                     colnames(split_cases)[grep("split_case_draw_", colnames(split_cases))], "population")) %>% data.frame
#***********************************************************************************************************************


#----PREVALENCE AND INCIDENCE---------------------------------------------------------------------------------------------------------
### convert cases to prevalence, given mean duration of 10 days
for (ii in 0:999) {
  predictions_conversion[, paste0("prev_draw_", ii)] <- ( ( predictions_conversion[, paste0("split_case_draw_", ii)] * 
                                                            (10/365) ) / predictions_conversion[, "population"] )
  predictions_conversion[, paste0("inc_draw_", ii)] <- ( predictions_conversion[, paste0("split_case_draw_", ii)] / 
                                                         predictions_conversion[, "population"] )
}

# save results
predictions_prev_save <- subset(predictions_conversion, select=c("location_id", "year_id", "age_group_id", "sex_id",
                                     colnames(predictions_conversion)[grep("prev_draw", colnames(predictions_conversion))]))

predictions_inc_save <- subset(predictions_conversion, select=c("location_id", "year_id", "age_group_id", "sex_id",
                                     colnames(predictions_conversion)[grep("inc_draw", colnames(predictions_conversion))]))
#***********************************************************************************************************************


#----SAVE---------------------------------------------------------------------------------------------------------------
if (WRITE_FILES == "yes") {
  # save incidence
  write.csv(predictions_prev_save, file.path(j.version.dir, "06_prev_draws.csv"), row.names=FALSE)
  # save prevalence
  write.csv(predictions_inc_save, file.path(j.version.dir, "08_inc_draws.csv"), row.names=FALSE)
}
#*********************************************************************************************************************** 


########################################################################################################################
##### PART FIVE: FORMAT FOR COMO #######################################################################################
########################################################################################################################


#----SAVE RESULTS-------------------------------------------------------------------------------------------------------
### format prevalence for como
colnames(predictions_prev_save) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", 0:999))
predictions_prev_save$measure_id <- 5

### format incidence for como
colnames(predictions_inc_save) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", 0:999))
predictions_inc_save$measure_id <- 6

predictions <- rbind(predictions_prev_save, predictions_inc_save)
predictions <- predictions %>% data.table

# keep only needed age groups
predictions <- predictions[predictions$age_group_id %in% c(age_start:age_end), ]

lapply(unique(predictions$location_id), function(x) write.csv(predictions[location_id==x],
                  file.path(cl.version.dir, paste0(x, ".csv")), row.names=FALSE))

print(paste0("nonfatal estimates saved in ", cl.version.dir))
#***********************************************************************************************************************

}


if (CALCULATE_COD == "yes") {

########################################################################################################################
##### PART Six: MODEL CFR ##############################################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
### get locations from covariates database
locations_cov <- get_location_metadata(gbd_round_id=4, location_set_id=22)
locations_cov <- subset(locations_cov, select=c("location_id", "ihme_loc_id", "location_name", "region_id", "super_region_id", "level", "location_type", "parent_id"))

### get covariates
# covariate: LDI_pc, covariate_id=57
ldi_cov <- get_covariate_estimates(covariate_name_short="LDI_pc")
ldi_cov <- ldi_cov[ldi_cov$year_id >= 1980, ]
ldi_cov$ln_LDI <- log(ldi_cov$mean_value)
ldi_cov <- subset(ldi_cov, select=c("location_id", "year_id", "ln_LDI"))

# under-2 malnutrition, covariate_id=66
mal_cov <- get_covariate_estimates(covariate_name_short="underweight_prop_waz_under_2sd")
mal_cov <- mal_cov[mal_cov$year_id >= 1980, ]
mal_cov[mal_cov$mean_value==0, "mean_value"] <- 0.0000000001
mal_cov$ln_mal <- log(mal_cov$mean_value)
mal_cov <- subset(mal_cov, select=c("location_id", "year_id", "ln_mal"))

# healthcare access and quality index
haqi <- get_covariate_estimates(covariate_name_short="haqi")
haqi$HAQI <- haqi$mean_value / 100
haqi <- subset(haqi, select=c("location_id", "year_id", "HAQI"))

# bring covariates together
covariates <- join(ldi_cov, mal_cov, by=c("location_id", "year_id"), type="inner")
covariates <- merge(covariates, subset(locations_cov, select=c("location_id", "ihme_loc_id")), by="location_id", all.x=TRUE)
covariates <- merge(covariates, haqi, by=c("location_id", "year_id"), all.x=TRUE)

### get COD data
# download raw (uncorrected) COD data from the database
cod <- read.csv(file.path(home, "FILEPATH", updated_cod_data_location))
cod <- cod[cod$acause=="measles", ]
cod <- cod[cod$age_group_id %in% c(age_start:age_end), ]
cod <- cod[!is.na(cod$cf_corr), ]
cod <- cod[cod$sample_size != 0, ]
colnames(cod)[colnames(cod)=="year"] <- "year_id"
cod$deaths <- cod$cf_corr * cod$sample_size
cod$deaths <- round(cod$deaths, 0)
cod_sum <- ddply(cod, c("location_id", "year_id"), summarise, deaths=sum(deaths), sample_size=sum(sample_size) )
# merge on locations
cod_sum <- merge(cod_sum, subset(locations_cov, select=c("location_id", "ihme_loc_id", "super_region_id")), by="location_id", all.x=TRUE)


### get WHO notifications
WHO <- join(case_notif, cod_sum, by=c("ihme_loc_id", "year_id"), type="inner")
WHO <- WHO[WHO$super_region_id %in% c(64, 31, 103), ]
WHO <- WHO[, c("ihme_loc_id", "year_id", "deaths", "cases")]
#WHO$cfr <- WHO$deaths / WHO$cases
WHO <- WHO[!is.na(WHO$year_id), ]

### get CFR literature review data
cfr_10 <- read.csv(file.path(j_root, "FILEPATH"))
# data cleaning
colnames(cfr_10)[colnames(cfr_10)=="Country.ISO3.Code"] <- "ihme_loc_id"
colnames(cfr_10)[colnames(cfr_10)=="Year.Start"] <- "year_start"
colnames(cfr_10)[colnames(cfr_10)=="Year.End"] <- "year_end"
colnames(cfr_10)[colnames(cfr_10)=="Age.Start"] <- "age_start"
colnames(cfr_10)[colnames(cfr_10)=="Age.End"] <- "age_end"
colnames(cfr_10)[colnames(cfr_10)=="Parameter.Value"] <- "cfr"
colnames(cfr_10)[colnames(cfr_10)=="Numerator..number.of.cases.with.the.condition."] <- "deaths"
colnames(cfr_10)[colnames(cfr_10)=="Effective.Sample.Size"] <- "cases"
colnames(cfr_10)[colnames(cfr_10)=="Hospital...Yes.1..No.0."] <- "hospital"
colnames(cfr_10)[colnames(cfr_10)=="Outbreak...Yes..1..No.0."] <- "outbreak"
colnames(cfr_10)[colnames(cfr_10)=="Mid.point.year.of.data.collection"] <- "midpointyear"
colnames(cfr_10)[colnames(cfr_10)=="Urbanicity.string"] <- "urbanicitystring"
cfr_10$ihme_loc_id <- as.character(cfr_10$ihme_loc_id)
cfr_10$ihme_loc_id[cfr_10$Country=="Marshall Islands"] <- "MHL"

# drop outliers
cfr_10 <- cfr_10[!(cfr_10$ihme_loc_id=="BEL" & cfr_10$year_start==2009), ]
cfr_10 <- cfr_10[!(cfr_10$ihme_loc_id=="BEL" & cfr_10$year_start==2010), ]
cfr_10 <- cfr_10[!(cfr_10$ihme_loc_id=="GNB" & cfr_10$year_start==1979 & cfr_10$year_end==1982 & cfr_10$age_start==0), ]

# fix urbanicity
cfr_10$urban <- 0
cfr_10$urban[cfr_10$urbanicitystring=="Urban"] <- 1
cfr_10$rural <- 0
cfr_10$rural[cfr_10$urbanicitystring=="Rural"] <- 1
cfr_10 <- subset(cfr_10, select=c("ihme_loc_id", "year_start", "year_end", "age_start", "age_end", "midpointyear",
                                  "deaths", "cases", "hospital", "outbreak", "rural")) 

# add on CFR from literature
cfr_13 <- read.csv(file.path(j_root, "FILEPATH"))
cfr_13$rural <- NA
cfr_13$rural[cfr_13$urbanicity==2] <- 1
cfr_13$rural[is.na(cfr_13$urbanicity)] <- 0
colnames(cfr_13)[colnames(cfr_13)=="iso3"] <- "ihme_loc_id"
cfr_13 <- subset(cfr_13, select=c("ihme_loc_id", "year_start", "year_end", "age_start", "age_end", "midpointyear",
                                  "deaths", "cases", "hospital", "outbreak", "rural"))   

# add on new CFR data from experts
cfr_expert_1 <- read.csv(file.path(j_root, "FILEPATH"))
cfr_expert_1 <- cfr_expert_1[cfr_expert_1$is_raw != "excluded_review", ]
cfr_expert_1$rural <- NA
cfr_expert_1$rural[cfr_expert_1$urbanicity==4] <- 0
colnames(cfr_expert_1)[colnames(cfr_expert_1)=="year_midpoint"] <- "midpointyear"
colnames(cfr_expert_1)[colnames(cfr_expert_1)=="numerator"] <- "deaths"
colnames(cfr_expert_1)[colnames(cfr_expert_1)=="denominator"] <- "cases"
colnames(cfr_expert_1)[colnames(cfr_expert_1)=="iso3"] <- "ihme_loc_id"
cfr_expert_1 <- subset(cfr_expert_1, select=c("ihme_loc_id", "year_start", "year_end", "age_start", "age_end", "midpointyear",
                                              "deaths", "cases", "hospital", "outbreak", "rural"))   

cfr_expert_2 <- read.csv(file.path(j_root, "FILEPATH"))
cfr_expert_2 <- cfr_expert_2[cfr_expert_2$data_status != "outlier", ]
cfr_expert_2 <- cfr_expert_2[cfr_expert_2$issues != "duplicate", ]
cfr_expert_2$rural <- NA
cfr_expert_2$rural[cfr_expert_2$urbanicity_type==4] <- 0
colnames(cfr_expert_2)[colnames(cfr_expert_2)=="midpoint_yr_collection"] <- "midpointyear"
colnames(cfr_expert_2)[colnames(cfr_expert_2)=="numerator"] <- "deaths"
colnames(cfr_expert_2)[colnames(cfr_expert_2)=="denominator"] <- "cases"
colnames(cfr_expert_2)[colnames(cfr_expert_2)=="iso3"] <- "ihme_loc_id"
cfr_expert_2 <- subset(cfr_expert_2, select=c("ihme_loc_id", "year_start", "year_end", "age_start", "age_end", "midpointyear",
                                              "deaths", "cases", "hospital", "outbreak", "rural"))

# add in new CFR data from 2016 literature review
cfr_2016 <- read.csv(file.path(home, "FILEPATH"))
names(cfr_2016)[names(cfr_2016)=="cases"] <- "deaths"
names(cfr_2016)[names(cfr_2016)=="sample_size"] <- "cases"
cfr_2016$midpointyear <- (cfr_2016$year_start + cfr_2016$year_end) / 2
cfr_2016$midpointyear <- round(cfr_2016$midpointyear, 0)
cfr_2016$rural <- NA
cfr_2016$rural[cfr_2016$urbanicity_type=="Rural"] <- 1
cfr_2016$rural[cfr_2016$urbanicity_type %in% c("Mixed/both", "Urban")] <- 0
cfr_2016 <- subset(cfr_2016, select=c("ihme_loc_id", "year_start", "year_end", "age_start", "age_end", "midpointyear", "deaths", "cases", 
                         "hospital", "outbreak", "rural")) #, "cfr"))
cfr_2016 <- ddply(cfr_2016, c("ihme_loc_id", "midpointyear"), summarise, 
                  deaths=sum(deaths), cases=sum(cases), hospital=mean(hospital), outbreak=mean(outbreak), rural=mean(rural, na.rm=TRUE))

# bring together all CFR data
all_cfr <- bind_rows(cfr_10, cfr_13, cfr_expert_1, cfr_expert_2, cfr_2016) 
all_cfr <- bind_rows(all_cfr, WHO)
all_cfr$midpointyear[all_cfr$midpointyear < 1980 & is.na(all_cfr$year_id)] <- 1980
all_cfr[is.na(all_cfr$year_id), "year_id"] <- all_cfr[is.na(all_cfr$year_id), "midpointyear"]
all_cfr[all_cfr$ihme_loc_id=="HKG", "ihme_loc_id"] <- "CHN_354"
all_cfr$deaths <- as.integer(all_cfr$deaths)

# merge on covariates
all_cfr <- merge(all_cfr, covariates, by=c("ihme_loc_id", "year_id"), all.x=TRUE)
all_cfr$cfr <- all_cfr$deaths / all_cfr$cases
all_cfr <- all_cfr[all_cfr$cfr <= 0.4, ]
all_cfr$hospital[is.na(all_cfr$hospital)] <- 0
all_cfr$outbreak[is.na(all_cfr$outbreak)] <- 0
all_cfr$rural[is.na(all_cfr$rural)] <- 0

# remove duplicates
all_cfr$dupes <- all_cfr[c("ihme_loc_id", "year_id", "age_start", "age_end", "cases", "deaths")] %>% duplicated
all_cfr <- all_cfr[all_cfr$dupes==FALSE, ]
all_cfr$dupes <- NULL
all_cfr <- all_cfr[all_cfr$year_id >= 1980 & all_cfr$year_id <= 2015, ]

# add locations
all_cfr <- all_cfr[!is.na(all_cfr$ihme_loc_id), ]
all_cfr <- merge(all_cfr, subset(locations_cov, select=c("ihme_loc_id", "super_region_id")), by="ihme_loc_id", all.x=TRUE)

# remove outliers
all_cfr_no_outliers <- all_cfr
all_cfr_no_outliers <- all_cfr_no_outliers[!(all_cfr_no_outliers$cfr > 0.025 & all_cfr_no_outliers$super_region_id==64), ]

# drop data from hill tribes or ethnic minorities
all_cfr_no_outliers <- all_cfr_no_outliers[!(all_cfr_no_outliers$ihme_loc_id=="THA" & all_cfr_no_outliers$year_start==1984), ]
all_cfr_no_outliers <- all_cfr_no_outliers[!(all_cfr_no_outliers$ihme_loc_id=="ETH" & all_cfr_no_outliers$year_start==1981), ]
all_cfr_no_outliers <- all_cfr_no_outliers[!(all_cfr_no_outliers$ihme_loc_id=="IND" & all_cfr_no_outliers$year_start==1991), ]
all_cfr_no_outliers <- all_cfr_no_outliers[!(all_cfr_no_outliers$ihme_loc_id=="IND" & all_cfr_no_outliers$year_start==1992 & 
                                             all_cfr_no_outliers$cfr > 0.15), ]

# drop data from Senegal, which are from studies conducted in Ibel village (a remote village with difficult transportation), 
# and an unspecified rural area (data collected by lay interviewers and no information on case definition)
all_cfr_no_outliers <- all_cfr_no_outliers[!(all_cfr_no_outliers$ihme_loc_id=="SEN" & all_cfr_no_outliers$year_start==1983), ]
all_cfr_no_outliers <- all_cfr_no_outliers[!(all_cfr_no_outliers$ihme_loc_id=="SEN" & all_cfr_no_outliers$year_start==1985), ]

# drop data from GNB with very high CFR
all_cfr_no_outliers <- all_cfr_no_outliers[!(all_cfr_no_outliers$ihme_loc_id=="GNB" & all_cfr_no_outliers$year_start==1979), ]

# keep only necessary columns
all_cfr_no_outliers <- subset(all_cfr_no_outliers, select=c("deaths", "ln_mal", "ln_LDI", "hospital", "outbreak", "rural", 
                                                            "cases", "ihme_loc_id", "year_id", "HAQI"))
all_cfr_no_outliers <- na.omit(all_cfr_no_outliers)
all_cfr_no_outliers$ihme_loc_id <- as.factor(all_cfr_no_outliers$ihme_loc_id)

# save regression input
write.csv(all_cfr_no_outliers, file.path(j.version.dir.inputs, "cfr_regression_input.csv"), row.names=FALSE)
#*********************************************************************************************************************** 


#----MODEL CFR----------------------------------------------------------------------------------------------------------
if (HAQI_or_LDI == "use_LDI") {
  ### set theta using output from GBD2015
  theta <- exp(1/-.3633593)  ## ln_mal only

  ### mixed effects neg binomial regression
  cfr_model <- MASS::glmmPQL(deaths ~ ln_mal + ln_LDI + hospital + outbreak + rural + 
                      offset(log(cases)), random= ~1 | ihme_loc_id, family=negative.binomial(theta=theta, link=log), data=all_cfr_no_outliers)

  # save log
  capture.output(cfr_model, file = file.path(j.version.dir.logs, "log_cfr_menegbin.txt"), type="output")
  capture.output(summary(cfr_model), file = file.path(j.version.dir.logs, "log_cfr_menegbin_summary.txt"), type="output")

} else if (HAQI_or_LDI == "use_HAQI") {
  ### set theta using output from GBD2015
  theta <- exp(1/-.3633593)  ## ln_mal only

  ### mixed effects neg binomial regression
  cfr_model <- MASS::glmmPQL(deaths ~ ln_mal + HAQI + hospital + outbreak + rural + 
                      offset(log(cases)), random= ~1 | ihme_loc_id, family=negative.binomial(theta=theta, link=log), data=all_cfr_no_outliers)

  # save log
  capture.output(cfr_model, file = file.path(j.version.dir.logs, "log_cfr_menegbin.txt"), type="output")
  capture.output(summary(cfr_model), file = file.path(j.version.dir.logs, "log_cfr_menegbin_summary.txt"), type="output")
}    
#*********************************************************************************************************************** 


#----DRAWS--------------------------------------------------------------------------------------------------------------
set.seed(0311)

### get location data
locations <- get_location_metadata(gbd_round_id=4, location_set_id=22)
locations <- subset(locations, select=c("location_id", "ihme_loc_id", "location_name", "region_id", "super_region_id", "level", "location_type", "parent_id"))

### predict out for all country-year-age-sex
pred_CFR <- merge(covariates, sum_pop_young, by=c("location_id", "year_id"), all.x=TRUE)
pred_CFR <- merge(pred_CFR, subset(locations, select=c("location_id", "super_region_id", "parent_id", "location_type")), 
                  by="location_id", all.x=TRUE)
N <- length(pred_CFR$year_id)

# calculate location and super region random effects
reffect_cfr <- ranef(cfr_model) %>% data.frame
colnames(reffect_cfr) <- "RE_loc"
reffect_cfr$ihme_loc_id <- rownames(reffect_cfr)
# collapse super region RE
reffect_cfr <- merge(reffect_cfr, subset(locations, select=c("ihme_loc_id", "super_region_id")), by="ihme_loc_id", all.x=TRUE)
super_region_re <- ddply(reffect_cfr, c("super_region_id"), summarise, RE_sr=mean(RE_loc) )
reffect_cfr <- subset(reffect_cfr, select=c("ihme_loc_id", "RE_loc"))
# get location and super region standard error
# add on location and super region random effects
pred_CFR <- merge(pred_CFR, reffect_cfr, by="ihme_loc_id", all.x=TRUE)
pred_CFR <- merge(pred_CFR, super_region_re, by="super_region_id", all.x=TRUE)

# add on same RE as parent for subnationals
reffect_subnats <- reffect_cfr
reffect_subnats <- merge(reffect_subnats, subset(locations, select=c("ihme_loc_id", "location_id")), by="ihme_loc_id", all.x=TRUE)
reffect_subnats$parent_id <- reffect_subnats$location_id

# list of locations already included in reffect_cfr
re_locs <- reffect_subnats$location_id

# list subnational locations and their parent_ids
subnats <- pred_CFR[!pred_CFR$location_type %in% c("admin0", "global", "superregion", "region"), c("location_id", "parent_id")]
parent_list <- unique(subnats$parent_id)
subnats <- locations[locations$location_id %in% unique(subnats$location_id), c("location_id", "parent_id")]
# use only reffect of those parents
reffect_subnats <- reffect_subnats[reffect_subnats$location_id %in% parent_list, ]
reffect_subnats$RE_subnat <- reffect_subnats$RE_loc
reffect_subnats <- subset(reffect_subnats, select=c("RE_subnat", "parent_id"))

# level 1
reffect_subnats_level1 <- merge(reffect_subnats, subnats, by="parent_id", all.x=TRUE)

# level 2
reffect_subnats_level2 <- reffect_subnats_level1
reffect_subnats_level2$parent_id <- reffect_subnats_level2$location_id
reffect_subnats_level2 <- subset(reffect_subnats_level2, select=c("parent_id", "RE_subnat"))
reffect_subnats_level2 <- merge(reffect_subnats_level2, subnats, by="parent_id", all.x=TRUE)
reffect_subnats_level2 <- reffect_subnats_level2[!is.na(reffect_subnats_level2$location_id), ]
reffect_subnats_level2 <- reffect_subnats_level2[!reffect_subnats_level2$location_id %in% 
                                                 c(re_locs, reffect_subnats_level1$location_id), ]

# level 3
reffect_subnats_level3 <- reffect_subnats_level2
reffect_subnats_level3$parent_id <- reffect_subnats_level3$location_id
reffect_subnats_level3 <- subset(reffect_subnats_level3, select=c("parent_id", "RE_subnat"))
reffect_subnats_level3 <- merge(reffect_subnats_level3, subnats, by="parent_id", all.x=TRUE)
reffect_subnats_level3 <- reffect_subnats_level3[!is.na(reffect_subnats_level3$location_id), ]
reffect_subnats_level3 <- reffect_subnats_level3[!reffect_subnats_level3$location_id %in% c(re_locs, 
                                                  reffect_subnats_level1$location_id, reffect_subnats_level2$location_id), ]

# bring together SIAs for all subnational levels
reffect_subnats_all <- bind_rows(reffect_subnats_level1, reffect_subnats_level2, reffect_subnats_level3)
reffect_subnats_all <- reffect_subnats_all[, colnames(reffect_subnats_all) != "parent_id"]

# add on subnational REs
pred_CFR <- merge(pred_CFR, reffect_subnats_all, by="location_id", all.x=TRUE)
pred_CFR$RE_loc[is.na(pred_CFR$RE_loc)] <- pred_CFR$RE_subnat[is.na(pred_CFR$RE_loc)]

### fix REs
# missing country random effects are replaced with the average random effect at the global level (i.e., 0)
pred_CFR[is.na(pred_CFR$RE_loc), "RE_loc"] <- pred_CFR[is.na(pred_CFR$RE_loc), "RE_sr"]
pred_CFR[is.na(pred_CFR$RE_loc), "RE_loc"] <- 0

pred_CFR <- subset(pred_CFR, select=c("location_id", "year_id", "ln_mal", "ln_LDI", "RE_loc", "HAQI"))
pred_CFR <- pred_CFR %>% data.frame

### 1000 draws for uncertainty
# keep only intercept and ln_mal coefficient
# coefficient matrix
coefmat <- c(fixef(cfr_model))
names(coefmat)[1] <- "constant"
names(coefmat) <- paste("b", names(coefmat), sep = "_")
coefmat <- matrix(unlist(coefmat), ncol=length(coefmat), byrow=TRUE, dimnames=list(c("coef"), c(as.vector(names(coefmat)))))

coefmat <- coefmat[1, 1:3]

# covariance matrix
vcovmat <- vcov(cfr_model)
vcovmat <- vcovmat[1:3, 1:3]

# create draws of coefficients using mean (from coefficient matrix) and SD (from covariance matrix)
betadraws <- rmvnorm(n=1000, mean=coefmat, sigma=vcovmat)
betas <- t(betadraws)

### estimate deaths
if (HAQI_or_LDI == "use_LDI") {
        # generate draws of the prediction using coefficient draws
        for (i in 0:999) { 
            pred_CFR[, paste0("cfr_draw_", i)] <- exp( betas[1, (i+1)] + 
                                                       ( betas[2, (i+1)] * pred_CFR$ln_mal ) +
                                                       ( betas[3, (i+1)] * pred_CFR$ln_LDI ) +
                                                       pred_CFR$RE_loc )
        }
} else if (HAQI_or_LDI == "use_HAQI") {
        for (i in 0:999) { 
            pred_CFR[, paste0("cfr_draw_", i)] <- exp( betas[1, (i+1)] + 
                                                       ( betas[2, (i+1)] * pred_CFR$ln_mal ) +
                                                       ( betas[3, (i+1)] * pred_CFR$HAQI ) +
                                                       pred_CFR$RE_loc )
        }
}

# save results
CFR_draws_save <- subset(pred_CFR, select=c("location_id", "year_id",
                                             colnames(pred_CFR)[grepl("cfr_draw_", colnames(pred_CFR))]))

if (WRITE_FILES == "yes") {
  write.csv(CFR_draws_save, file.path(j.version.dir, paste0("10_CFR_draws.csv")), row.names=FALSE)
}
#*********************************************************************************************************************** 


########################################################################################################################
##### PART SEVEN: DEATHS ###############################################################################################
########################################################################################################################


#----PREP---------------------------------------------------------------------------------------------------------------
# merge
predictions_deaths <- join(rescaled_cases_sub, CFR_draws_save, by=c("location_id", "year_id"), type="inner")
#*********************************************************************************************************************** 


#----CALCULATE DEATHS---------------------------------------------------------------------------------------------------
for (ii in 0:999) {
  predictions_deaths[, paste0("death_draw_", ii)] <- 
            predictions_deaths[, paste0("cfr_draw_", ii)] * predictions_deaths[, paste0("case_draw_", ii)]
}

# save results
predictions_deaths_save <- predictions_deaths[, c("location_id", "year_id",
                                      colnames(predictions_deaths)[grep("death_draw", colnames(predictions_deaths))])]

if (WRITE_FILES == "yes") {
  write.csv(predictions_deaths_save, file.path(j.version.dir, "11_deaths_for_rescaling.csv"), row.names=FALSE)
}
#*********************************************************************************************************************** 


########################################################################################################################
##### PART EIGHT: RESCALE DEATHS #######################################################################################
########################################################################################################################


#----RESCALE------------------------------------------------------------------------------------------------------------
### run custom rescale function
rescale_GBD2016(input_file=predictions_deaths_save, measure="death")
rescaled_deaths <- rescaled_draws

rescaled_deaths[is.na(rescaled_deaths)] <- 0
#*********************************************************************************************************************** 


#----SAVE---------------------------------------------------------------------------------------------------------------
if (WRITE_FILES == "yes") {
  # save results
  write.csv(rescaled_deaths, file.path(j.version.dir, "12_deaths_rescaled_for_split.csv"), row.names=FALSE)
}
#***********************************************************************************************************************


########################################################################################################################
##### PART NINE: SPLIT DEATHS ##########################################################################################
########################################################################################################################


#----SPLIT--------------------------------------------------------------------------------------------------------------
age_sex_split(acause=acause, input_file=rescaled_deaths, measure="death")
split_deaths <- split_data_frame
#*********************************************************************************************************************** 


#----SAVE---------------------------------------------------------------------------------------------------------------
# save split draws
split_deaths_save <- subset(split_deaths, select=c("location_id", "year_id", "age_group_id", "sex_id", 
            colnames(split_deaths)[grep("split_death_draw_", colnames(split_deaths))]))
if (WRITE_FILES == "yes") {
  write.csv(split_deaths_save, file.path(j.version.dir, "13_split_death_draws.csv"), row.names=FALSE)
  # save split point estimates and CIs
  #collapse_point(acause=acause, input_file=split_deaths, number="14", name="split_death", j.direct=j.version.dir)
}
#*********************************************************************************************************************** 


########################################################################################################################
##### PART TEN: COMBINE CODEm DEATHS ##################################################################################
########################################################################################################################


#----COMBINE------------------------------------------------------------------------------------------------------------
### read CODEm COD results for data-rich countries
cod_M <- read_hdf5_table(file.path("FILEPATH", acause, male_CODEm_version, paste0("draws/deaths_", "male", ".h5")), key="data")
cod_F <- read_hdf5_table(file.path("FILEPATH", acause, female_CODEm_version, paste0("draws/deaths_", "female", ".h5")), key="data")

# combine M/F CODEm results
cod_DR_M <- subset(cod_M, select=c("location_id", "year_id", "age_group_id", "sex_id", colnames(cod_M)[grep("draw_", colnames(cod_M))]))
cod_DR_F <- subset(cod_F, select=c("location_id", "year_id", "age_group_id", "sex_id", colnames(cod_F)[grep("draw_", colnames(cod_F))]))
cod_DR <- rbind(cod_DR_M, cod_DR_F)

# use model for USA and MEX
MEX_USA_subnats <- locations[parent_id %in% c(102, 130), location_id]
cod_DR <- cod_DR[!cod_DR$location_id %in% c(102, 130, MEX_USA_subnats), ]

# hybridize data-rich and custom models
data_rich <- unique(cod_DR$location_id)
split_deaths_glb <- split_deaths_save[!split_deaths_save$location_id %in% data_rich, ]
colnames(split_deaths_glb) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", 0:999))
split_deaths_hyb <- rbind(split_deaths_glb, cod_DR)

# keep only needed age groups
split_deaths_hyb <- split_deaths_hyb[split_deaths_hyb$age_group_id %in% c(age_start:age_end), ]

# save results
if (WRITE_FILES == "yes") {
    write.csv(split_deaths_hyb, file.path(j.version.dir, "14_split_death_draws_with_codem_data_rich.csv"), row.names=FALSE)
}
#***********************************************************************************************************************


########################################################################################################################
##### PART ELEVEN: FORMAT FOR CODCORRECT ###############################################################################
########################################################################################################################


#----SAVE RESULTS-------------------------------------------------------------------------------------------------------
### format for codcorrect
colnames(split_deaths_hyb) <- c("location_id", "year_id", "age_group_id", "sex_id", paste0("draw_", 0:999))

# deaths, measure_id==1
ids <- unique(split_deaths_hyb$location_id)
years <- unique(split_deaths_hyb$year_id)
sexes <- c(1,2)

# save .csv for location-year-sex
write <- function(x,y,z) { fwrite(split_deaths_hyb[split_deaths_hyb$location_id==x & split_deaths_hyb$year_id==y & split_deaths_hyb$sex_id==z, ], 
                                    file.path(cl.death.dir, paste0(x, "_", y, "_", z, ".csv"))) }
for (location_id in ids) {
  for (year in years) {
    for (sex in sexes) {
      write(x=location_id, y=year, z=sex)
    }
  }
}
             
print(paste0("death draws saved in ", cl.death.dir))
#***********************************************************************************************************************  

}