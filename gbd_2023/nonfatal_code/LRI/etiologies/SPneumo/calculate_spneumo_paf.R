## Pull in the results from BradMod (1000 draws) of the age-specific estimate of S. pneumoniae PAF
## in the absence of the vaccine. Take those results and calculate the age-year-location draws
## based on vaccine coverage for the final S pneumo PAF draws.
########################################################################################################
## Prep ##
########################################################################################################
rm(list=ls())

os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  k <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
  k <- "FILEPATH"
}
pacman::p_load(data.table, openxlsx, reshape2, boot, ggplot2, plyr, dplyr, msm, metafor)
library(crosswalk002, lib.loc = "FILEPATH")
invisible(sapply(list.files("FILEPATH"), source))

# Directories
in_dir <- paste0(j, "FILEPATH")
out_dir <- "FILEPATH"

annual_paf <- T
gbd_round_id <- 7

# Age metadata
age_meta <- get_age_metadata(age_group_set_id=19, gbd_round_id=gbd_round_id)
age_meta$age_mid <- (age_meta$age_group_years_start + age_meta$age_group_years_end)/2
age_meta <- age_meta[,c("age_group_id","age_group_years_start", "age_mid")]
setnames(age_meta, "age_group_years_start", "age_lower")

loc_meta <- get_location_metadata(location_set_id = 35, decomp_step = "iterative", gbd_round_id = 7)

# Let's only keep estimation years for now
demo <- get_demographics('epi', gbd_round_id = gbd_round_id)
locations <- demo$location_id
years <- demo$year_id
if (annual_paf == T) years <- years[1]:years[length(years)]

## RGB
# Pull in Base PAFs
pcv_data <- read.csv(paste0(in_dir, "/mr-brt_predicted_by_age.csv"))
pcv_data <- as.data.table(pcv_data)
pcv_data$age_mid <- pcv_data$age_scaled*100
# subset to RCT
pcv_data <- pcv_data[before_after == 0]
# transform the draws back into linear space * this version of the code, they are coming from logit
pcv_data[, paste0("draw_",0:999) := exp(.SD)/(1+exp(.SD)), .SDcols = paste0("draw_",0:999)] 

DT <- data.table(pcv_data, key = c("age_mid"))
tm <- data.table(age_meta, key = c("age_mid"))

# use join syntax with roll = 'nearest'
paf_age <- DT[tm, roll='nearest']
setnames(paf_age, paste0("draw_",0:999), paste0("age_",1:1000))

## Get COV_PCV and COV_Serotype the same way as in prep_pneumo_study_pafs.R
# Pull PCV coverage #
pcv_cov_pull <- data.frame(get_covariate_estimates(covariate_id=210, location_id=locations, year_id=years, decomp_step="step3", gbd_round_id = gbd_round_id))
setnames(pcv_cov_pull, c("mean_value","lower_value","upper_value"), c("pcv_cov","pcv_cov_lower","pcv_cov_upper"))
pcv_cov <- merge(pcv_cov_pull, loc_meta[,c("location_id","super_region_name","region_name")], by="location_id")

# This file is from <NAME> and gives the estimated fraction of all S. pneumo serotypes covered by
# each of the PCV types (7, 10, 13)
sero_cov <- read.csv("FILEPATH")

# So the regions don't correspond directly to GBD regions, right now they are matched as closely as possible
pcv_cov$region <- with(pcv_cov, ifelse(region_name=="Oceania","Oceania",
                                       ifelse(region_name=="High-income North America","North America",
                                              ifelse(region_name=="Western Europe","Europe",
                                                     ifelse(region_name=="Central Europe", "Europe",
                                                            ifelse(region_name=="Eastern Europe","Europe",
                                                                   ifelse(region_name=="Australasia","North America",
                                                                          ifelse(region_name=="High-income Asia Pacific","Asia",
                                                                                 ifelse(super_region_name=="Sub-Saharan Africa","Africa",
                                                                                        ifelse(super_region_name=="North Africa and Middle East","Africa",
                                                                                               ifelse(super_region_name=="Southeast Asia, East Asia, and Oceania","Asia",
                                                                                                      ifelse(super_region_name=="South Asia","Asia",
                                                                                                             ifelse(super_region_name=="Central Europe, Eastern Europe, and Central Asia","Asia","LAC"))
                                                                                               )))))))))))

pcv_cov <- join(pcv_cov, sero_cov, by=c("region"))

# Create a composite indicator for pcv_vt_cov by multiply GBD coverage (pcv_cov) by estimated fraction of all S. pneumo serotypes covered by vax (covmean)
pcv_cov$pcv_vt_cov <- pcv_cov$pcv_cov * pcv_cov$covmean

# Get error for this estimate by combining products of variances
pcv_cov$pcv_cov_std <- (pcv_cov$pcv_cov_upper - pcv_cov$pcv_cov_lower) / 2 / qnorm(0.975)
pcv_cov$vtcov_std <- (pcv_cov$covupper - pcv_cov$covlower) / 2 / qnorm(0.975)
pcv_cov$pcv_vt_cov_std <- with(pcv_cov, sqrt(vtcov_std^2 * pcv_cov^2 + pcv_cov_std^2 * covmean^2 + vtcov_std^2 * pcv_cov_std^2))

pcv_cov <- subset(pcv_cov, vtype=="PCV13")
pcv_cov <- subset(pcv_cov, !is.na(covmean))

# Let's only keep estimation years for now
pcv_cov <- subset(pcv_cov, year_id %in% years)

# Subset to nonzero pcv_cov values - zero values will keep only the base_PAF
# If coverage is zero in a given year, you we can set those aside
pcv_cov_zeros <- subset(pcv_cov, pcv_vt_cov == 0)
pcv_cov <- subset(pcv_cov, pcv_vt_cov != 0)

# Now we need to generate draws of the serotype-specific vaccine coverage #
logit_cov_df <- as.data.frame(delta_transform(pcv_cov$pcv_vt_cov, pcv_cov$pcv_vt_cov_std, transformation = "linear_to_logit"))
pcv_cov$logit_mean <- logit_cov_df$mean_logit
pcv_cov$logit_std <- logit_cov_df$sd_logit
pcv_cov <- rbind.fill(pcv_cov, pcv_cov_zeros)

for(i in 1:1000){
  pcv_cov[,paste0("vtcov_",i)] <- inv.logit(rnorm(length(pcv_cov$covariate_id), pcv_cov$logit_mean, pcv_cov$logit_std))
  pcv_cov[pcv_cov$pcv_vt_cov==0,paste0("vtcov_",i)] <- 0
}


# Get VE_optimal.
ve_optimal <- read.csv(paste0(in_dir, "ve_pooled_child_mean_se.csv"))
# generate a normal distribution
for(i in 1:1000){
  ve_optimal[,paste0("ve_optimal_",i)] <- rnorm(1, ve_optimal$mean, ve_optimal$se)
}


n <- 1
# EXAMPLE OF NEGATIVE DRAWS - l = 4 & i = 146
for (l in locations){
  print(paste0("On location ", n, " of ",length(locations)))
  df <- expand.grid(age_group_id = age_meta$age_group_id, year_id=years, location_id=l, sex_id=c(1,2))
  pcv_df <- expand.grid(age_group_id = age_meta$age_group_id, year_id=years, location_id=l, sex_id=c(1,2))
  pcv_df <- join(pcv_df, pcv_cov, by=c("location_id","year_id"))
  pcv_df <- join(pcv_df, paf_age, by=c("age_group_id"))
  # adjust down the VE_optimal based on the relative VE in adults as compared to children in child vaccine studies
  # This could be done using the Grijalva study where the VE for children 2-4 was ~74% and for adults 65+ VE was ~20%
  # so multiply by correction VE adult/VE child Where VE_adult/VE_child = .2/.74.
  pcv_df <- as.data.table(pcv_df)
  pcv_df[age_mid>5, paste0("vtcov_",1:1000) := .SD * (.2/.74), .SDcols = paste0("vtcov_",1:1000)] 
  pcv_df <- as.data.frame(pcv_df)
  for(i in 1:1000){
    base <- pcv_df[,paste0("age_",i)]
    vt <- pcv_df[,paste0("vtcov_",i)]
    v_effectiveness <- ve_optimal[,paste0("ve_optimal_",i)]
    paf <- (base * (1-vt*v_effectiveness)) / (1-base*vt*v_effectiveness)
    # add draws to data table
    df[,paste0("draw_",i)] <- paf
    # If in neonatal period, set to 0
    df[df$age_group_id %in% c(2,3), paste0("draw_",i)] <- 0
  }

  df$cause_id <- 322
  df$rei_id <- 188
  df$modelable_entity_id <- 1263
  setnames(df, paste0("draw_",1:1000), paste0("draw_",0:999))
  if (any(is.na(df))) break("NAs present in data frame")
  ## check everything for zeros
  draws <- df[,grep("draw", names(df)), with = F]
  if (any(draws > 1) + any(draws < -1) > 0) {
    print(paste("invalid values for", l, measure)) 
  }

  fwrite(df, paste0(out_dir, "/paf_yll_",l,".csv"))
  fwrite(df, paste0(out_dir, "/paf_yld_",l,".csv"))

  n <- n + 1
}


