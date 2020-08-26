###############################################################################################
## Purpose: Calculate diarrhea PAFs including DisMod output, misclassification
## correction, and odds ratios from GEMS and MALED. This particular version of this
##  file is for case definition of qPCR Ct value below lowest inversion in accuracy.
## It also uses the fixed effects only from the mixed effects logistic regression models.
###############################################################################################
## Right now, it runs in a loop for each etiology ##
##########################################################################################
## Setup ##
print(commandArgs()[1])
print(commandArgs()[2])
print(commandArgs()[3])
print(commandArgs()[4])
location <- commandArgs()[4]

library(plyr)
library(boot)
library(msm)
source() # filepaths to central functions for data upload/download

age_map <- read.csv("filepath")

## Define these here so they are written out just once.
  age_group_id <- age_map$age_group_id[!is.na(age_map$id)]
  sex_id <- c(1,2)
  year_id <- c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019)

## Import sensitivity/specificity adjustment scalars ##
  adj_matrix <- read.csv("filepath")
# rename
  for(i in 1:1000){
    setnames(adj_matrix, paste0("specificity_",i), paste0("specificity_",i-1))
    setnames(adj_matrix, paste0("sensitivity_",i), paste0("sensitivity_",i-1))
  }

## Import odds ratios ##
  odds_ratios <- read.csv("filepath")
# rename
  for(i in 1:1000){
    setnames(odds_ratios, paste0("odds_",i), paste0("odds_",i-1))
  }
  odds_gems <- subset(odds_ratios, study=="GEMS")
  odds_maled <- subset(odds_ratios, study=="MALED")

## Import etiology meta-data ##
  eti_meta <- read.csv("filepath")
  eti_meta <- subset(eti_meta, source=="GEMS")

## Import MR_BRT covariates for inpatient sample population ##
  dismod_covs <- read.csv("filepath")
  mrbrt_covs <- read.csv("filepath")

#################################################################

############################################################################################
## Great, now launch in a loop ##
#for(m in eti_meta$modelable_entity_id){
for(v in 1:11){
  m <- eti_meta$modelable_entity_id[v]
  name <- as.character(eti_meta$name_colloquial[v])
  eti_dir <- eti_meta$rei[v]
  #eti_dir <- eti_meta$rei[eti_meta$modelable_entity_id==m]
  eti_cov <- subset(mrbrt_covs, etiology == name)
  #eti_cov <- subset(mrbrt_covs, modelable_entity_id==m)

  print(paste0("Calculating attributable fractions for ",name))

  # Get etiology proportion draws for DisMod model #
  eti_draws <- data.frame(get_draws(source="epi", 
                                    gbd_id_type="modelable_entity_id", 
                                    gbd_id=m, 
                                    sex_id=sex_id, 
                                    location_id=location, 
                                    year_id=year_id, 
                                    age_group_id=age_group_id, 
                                    decomp_step="step3"))

  # Find the median, take 1% of that for the floor (consistent with DisMod default for linear floor)
	  eti_draws$floor <- apply(eti_draws[,6:1005], 1, median)
	  eti_draws$floor <- eti_draws$floor * 0.01

	  eti_draws <- join(eti_draws, adj_matrix, by="modelable_entity_id")

	  eti_fatal <- join(eti_draws, odds_gems, by=c("age_group_id","modelable_entity_id"))
	  eti_non_fatal <- join(eti_draws, odds_maled, by=c("age_group_id","modelable_entity_id"))

  ## Do PAF calculation ##
    fatal_df <- eti_fatal[,c("location_id","year_id","age_group_id","sex_id")]
    non_fatal_df <- eti_non_fatal[,c("location_id","year_id","age_group_id","sex_id")]

  for(i in 0:999){
    draw <- eti_fatal[,paste0("draw_",i)]
    sp <- eti_fatal[,paste0("specificity_",i)]
    se <- eti_fatal[,paste0("sensitivity_",i)]
    or_fatal <- eti_fatal[,paste0("odds_",i)]
    or_non_fatal <- eti_non_fatal[,paste0("odds_",i)]
    scalar <- eti_cov[,paste0("scalar_",i)]

    # Convert to logit, adjust for inpatient status #
      fdraw <- inv.logit(logit(draw) - scalar)

    # Adjust for diagnostics
    fdraw <- (fdraw + sp - 1) / (sp + se - 1)
    draw <- (draw + sp - 1) / (sp + se - 1)

    # Make sure it stays in range
    fdraw <- ifelse(fdraw > 1, 1, ifelse(fdraw < 0, eti_draws$floor, fdraw))
    draw <- ifelse(draw > 1, 1, ifelse(draw < 0, eti_draws$floor, draw))

    # Calculate PAF
    fdraw <- fdraw * (1 - 1/or_fatal)
    draw <- draw * (1 - 1/or_non_fatal)

    # Make sure it stays in range
    fdraw <- ifelse(fdraw < 0, eti_draws$floor, fdraw)
    draw <- ifelse(draw < 0, eti_draws$floor, draw)

    fatal_df[,paste0("draw_",i)] <- fdraw
    non_fatal_df[,paste0("draw_",i)] <- draw
  }
  fatal_df$modelable_entity_id <- m
  fatal_df$cause_id <- 302
  write.csv(fatal_df, "filepath", row.names=F)

  non_fatal_df$modelable_entity_id <- m
  non_fatal_df$cause_id <- 302
  write.csv(non_fatal_df, "filepath", row.names=F)

}

