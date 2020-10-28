## Pull in the results from BradMod (1000 draws) of the age-specific estimate of S. pneumoniae PAF
## in the absence of the vaccine. Take those results and calculate the age-year-location draws
## based on vaccine coverage for the final S pneumo PAF draws.
########################################################################################################
## Prep ##
########################################################################################################

library(metafor)
library(plyr)
library(ggplot2)
library(reshape2)
library(boot)
source("filepath/get_covariate_estimates.R")

# Age metadata
  age_meta <- read.csv('filepath')
  age_meta <- subset(age_meta, age_pull==1)
  age_meta <- age_meta[,c("age_group_id","age_start")]
  setnames(age_meta, "age_start", "age_lower")

# Let's only keep estimation years for now
years <- c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019)

## Our own review of extracted data yields a higher VE than GBD assumed value of 0.8 (when using all data on VT-VE)
  pcv_data <- subset(pcv_data, is_outlier==0 & use_review==0)

  pcv_data$lnrr <- log(1-pcv_data$ve_vt_invasive/100)
  pcv_data$lnrr_se <- (log(1-pcv_data$ve_vt_invasive_lower/100) - pcv_data$lnrr) /  qnorm(0.975)
  pcv_effect <- rma(yi=lnrr, sei=lnrr_se, data=pcv_data[pcv_data$study_type=="RCT",], slab=paste0(first,"_",iso3))
  forest(pcv_effect, transf=function(x) 1-exp(x))

v_effectiveness <- as.numeric(1 - exp(pcv_effect$b))

# Prep ages for use later
ages <- read.csv("filepath")
ages <-  subset(ages, !is.na(order) & age_group_id!=33)
ages <- ages[,c("age_group_id","age_group_name")]
ages$covariate_id <- 210

## Import S pneumo PAF age curve ##
  paf_age <- read.csv("filepath")
  setnames(paf_age, paste0("draw",1:1000), paste0("age_",1:1000))
  paf_age <- join(paf_age, age_meta, by="age_lower")
  paf_age$age_group_id[is.na(paf_age$age_group_id)] <- 3

## Import PCV coverage, serotype coverage estimates, previously created ##
pcv_cov <- read.csv("filepath")
  pcv_cov <- subset(pcv_cov, vtype=="PCV13")
  pcv_cov <- subset(pcv_cov, !is.na(covmean))

###########################################
## Making it flexible to do either the "base" Hib paf or final by country/year to define the
## "Hib PAF" used in the final calculation 
#############################################
type <- "base"        ## Must either be "base" or "final"
ve_hib <- 0.8         ## Recall that this value should be equal to that determined in "calculate_hib_paf.R" file (0.8 is GBD 2017 value)
ve_hib <- 0.91        ## Using the value from our internal systematic review and meta-analysis (2/15/19)

## Import Hib final PAF draws ##
if(type=="final"){
  hib_paf <- read.csv("filepath")      ## This one has a different value by country/year depending on vaccine coverage
} else {
  hib_b <- read.csv("filepath")         ## This one is the "base" Hib PAF (single set of draws)
  hib_b$covariate_id <- 47
  hib_cov <- read.csv("filepath")
  hib_cov <- subset(hib_cov, year_id %in% years)
  hib_b <- join(hib_cov, hib_b, by="covariate_id")
  hib_paf <- hib_cov[,c("location_id","year_id")]
  for(i in 1:1000){
    v <- hib_b[,paste0("coverage_",i)]
    p <- hib_b[,paste0("draw_",i)]
    hib_paf[,paste0("draw_",i)] <- (p * v * ve_hib)
  }
}

setnames(hib_paf, paste0("draw_",1:1000), paste0("hib_",1:1000))

# only keep estimation years 
pcv_cov <- subset(pcv_cov, year_id %in% years)

# Generate draws of the serotype-specific vaccine coverage #
pcv_cov$pre_trans <- ifelse(pcv_cov$pcv_vt_cov==0, 0.5, pcv_cov$pcv_vt_cov)
pcv_cov$pre_upper <- pcv_cov$pre_trans + pcv_cov$pcv_vt_cov_std * qnorm(0.975)
pcv_cov$logit_mean <- logit(pcv_cov$pre_trans)
pcv_cov$logit_upper <- logit(pcv_cov$pre_upper)
pcv_cov$logit_std <- (pcv_cov$logit_upper - pcv_cov$logit_mean) / qnorm(0.975)

for(i in 1:1000){
  pcv_cov[,paste0("vtcov_",i)] <- inv.logit(rnorm(length(pcv_cov$covariate_id), pcv_cov$logit_mean, pcv_cov$logit_std))
  pcv_cov[pcv_cov$pcv_vt_cov==0,paste0("vtcov_",i)] <- 0
}
write.csv(pcv_cov, "filepath", row.names=F)

#########################################################################################
## loop through locations and save CSVs ##
#########################################################################################

## Loop through location_ids, save individual CSVs ##
locs <- read.csv("filepath")
locs <- subset(locs, is_estimate==1 & most_detailed==1)
loc_loop <- locs$location_id
n <- 1
for(l in loc_loop){
  print(paste0("On location ", n, " of ",length(loc_loop)))
  df <- expand.grid(age_group_id = ages$age_group_id, year_id=years, location_id=l, sex_id=c(1,2))
  pcv_df <- expand.grid(age_group_id = ages$age_group_id, year_id=years, location_id=l, sex_id=c(1,2))

    pcv_df <- join(pcv_df, hib_paf, by=c("location_id","year_id"))
    pcv_df <- join(pcv_df, pcv_cov, by=c("location_id","year_id"))
    pcv_df <- join(pcv_df, paf_age, by=c("age_group_id"))

  for(i in 1:1000){
    base <- pcv_df[,paste0("age_",i)]
    vt <- pcv_df[,paste0("vtcov_",i)]
    hib <- pcv_df[,paste0("hib_",i)]
    hib <- ifelse(pcv_df$age_group_id < 4, 0, ifelse(pcv_df$age_group_id > 5, 0, hib))
    paf <- base * (1-vt*v_effectiveness) / (1-hib) / (1-base*vt*v_effectiveness/(1-hib))
    df[,paste0("draw_",i)] <- paf

    # If greater than 1, set to 1
    df[, paste0("draw_",i)] <- ifelse(df[,paste0("draw_",i)] < 1, df[,paste0("draw_",i)], 1)

    # If in neonatal period, set to 0
    df[df$age_group_id < 4, paste0("draw_",i)] <- 0

  }

  df$cause_id <- 322
  df$rei_id <- 188
  df$modelable_entity_id <- 1263
  setnames(df, paste0("draw_",1:1000), paste0("draw_",0:999))

  write.csv(df, "filepath", row.names=F)

  n <- n + 1
}
