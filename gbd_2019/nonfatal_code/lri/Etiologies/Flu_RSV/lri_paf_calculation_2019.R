##################################################################################################
## Purpose: Calculate influenza, RSV PAFs including DisMod output, odds ratios and CFR adjustment
#################################################################################

## Right now, it runs in a loop for each etiology ##

##########################################################################################
## Do some set up and housekeeping ##
print(commandArgs()[1])
print(commandArgs()[2])
print(commandArgs()[3])
print(commandArgs()[4])
location <- commandArgs()[4]

library(plyr)
library(boot)
source() # filepath to get_draws.R
source() # filepath to get_population.R
age_map <- read.csv("filepath")

## Define these here so they are written out just once.
age_group_id <- age_map$age_group_id[!is.na(age_map$id)]
sex_id <- c(1,2)
year_id <- c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019)

## Import odds ratios ##
odds_ratios <- read.csv("filepath")

# rename
for(i in 0:999){
  setnames(odds_ratios, paste0("rr_",i), paste0("odds_",i))
}

## Import case fatality ratios ##
cfr_draws <- read.csv("filepath")

## Import etiology meta-data ##
eti_meta <- read.csv("filepath")
eti_meta <- subset(eti_meta, rei_id %in% c(187,190))

## Import DisMod covariates for inpatient sample population ##
dismod_covs <- read.csv("filepath")
mrbrt_covs <- read.csv("filepath")

## Great, now launch in a loop ##
for(m in eti_meta$modelable_entity_id){
  eti_dir <- eti_meta$rei[eti_meta$modelable_entity_id==m]
  eti_cov <- subset(mrbrt_covs, modelable_entity_id==m)

  # Get etiology proportion draws for DisMod model #
  eti_draws <- data.frame(get_draws(source="epi", 
                                    gbd_id_type="modelable_entity_id", 
                                    gbd_id=m, 
                                    sex_id=sex_id, 
                                    location_id=location, 
                                    year_id=year_id, 
                                    age_group_id=age_group_id, 
                                    decomp_step="step2"))

  # Find the median, take 1% of that for the floor (consistent with DisMod default for linear floor)
  eti_draws$floor <- apply(eti_draws[,6:1005], 1, median)
  eti_draws$floor <- eti_draws$floor * 0.01

  eti_draws <- join(eti_draws, odds_ratios, by=c("age_group_id","modelable_entity_id"))

  eti_fatal <- join(eti_draws, cfr_draws, by=c("age_group_id"))

  ## Do PAF calculation ##
  fatal_df <- eti_fatal[,c("location_id","year_id","age_group_id","sex_id")]
  non_fatal_df <- eti_draws[,c("location_id","year_id","age_group_id","sex_id")]

  for(i in 0:999){
    draw <- eti_draws[,paste0("draw_",i)]
    or <- eti_draws[,paste0("odds_",i)]
    cfr <- eti_fatal[,paste0("scalarCFR_",i)]
    scalar <- eti_cov[,paste0("scalar_",i)]

    # No disease in children in neonatal age period
    draw <- ifelse(eti_draws$age_group_id < 4, 0, draw)

    # Multiply by severe scalar (for fatal PAF)
    # Convert to logit, adjust for inpatient status #
    fdraw <- inv.logit(logit(draw) - scalar)

    # Adjust for differential case fatality
    fdraw <- fdraw * cfr

    # Make sure it stays in range
    fdraw <- ifelse(fdraw > 1, 1, ifelse(fdraw < 0, eti_draws$floor, fdraw))

    # Calculate PAF
    fdraw <- fdraw * (1 - 1/or)
    draw <- draw * (1 - 1/or)

    # Make sure it stays in range
    fdraw <- ifelse(fdraw < 0, eti_draws$floor, fdraw)
    draw <- ifelse(draw < 0, eti_draws$floor, draw)

    fatal_df[,paste0("draw_",i)] <- fdraw
    non_fatal_df[,paste0("draw_",i)] <- draw
  }
  fatal_df$modelable_entity_id <- m
  fatal_df$cause_id <- 322
  write.csv(fatal_df, "filepath", row.names=F)

  non_fatal_df$modelable_entity_id <- m
  non_fatal_df$cause_id <- 322
  write.csv(non_fatal_df, "filepath", row.names=F)

}
