###############################################################################################
## Purpose: Calculate diarrhea PAFs including DisMod output, misclassification
## correction, and odds ratios from GEMS and MALED. This particular version of this
##  file is for case definition of qPCR Ct value below lowest inversion in accuracy.
## It also uses the fixed effects only from the mixed effects logistic regression models.
###############################################################################################
## Runs in a loop for each etiology ##
##########################################################################################
## Setup ##
print(commandArgs()[1])
print(commandArgs()[2])
print(commandArgs()[3])
print(commandArgs()[4])
print(commandArgs()[5])
print(commandArgs()[6])
location <- commandArgs()[6]

library(plyr)
library(boot)
library(msm)
source() # filepaths to central functions for data upload/download

age_map <- read.csv("filepath")

## Define these here 
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

###########################################################
## Create draws of rotavirus vaccine impact ##
  rcov <- get_covariate_estimates(covariate_id=1075, location_id=location, year_id=year_id, gbd_round_id=6, decomp_step="step4")
    rcov$mean_coverage <- rcov$mean_value
    rcov$mean_value <- ifelse(rcov$mean_value==0,0.01,rcov$mean_value)
    rcov$logit_cov <- logit(rcov$mean_value)
    rcov$sd <- (rcov$upper_value - rcov$lower_value) / 2 / qnorm(0.975)
    rcov$sd <- ifelse(rcov$sd == 0, 0.01, rcov$sd)
    # Convert to logit space to take draws
      rcov$logit_cov_sd <- sapply(1:nrow(rcov), function(i) {
        ratio_i <- rcov[i, "mean_value"]
        ratio_se_i <- rcov[i, "sd"]
        deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
      })
  # Predicted rotavirus VE
  rota_ve <- read.csv("filepath")
    rota_ve$year <- as.numeric(rota_ve$year_id)
    rota_ve <- subset(rota_ve, location_id %in% location)
    rota_ve <- rota_ve[rota_ve$year %in% year_id,]
    rota_ve$logit_ve <- logit(rota_ve$pred_ve)
    # Convert to logit space to take draws
      rota_ve$logit_ve_sd <- sapply(1:nrow(rota_ve), function(i) {
        ratio_i <- rota_ve[i, "pred_ve"]
        ratio_se_i <- rota_ve[i, "pred_se"]
        deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
      })

  # Create draws
    rv_impact <- data.frame(year_id=year_id)
    for(i in 0:999){
      ve <- inv.logit(rnorm(length(year_id), rota_ve$logit_ve, rota_ve$logit_ve_sd))
      cov <- inv.logit(rnorm(length(year_id), rcov$logit_cov, rcov$logit_cov_sd))
      cov <- ifelse(rcov$mean_coverage == 0, 0, cov)
      rv_impact[,paste0("rv_impact_",i)] <- ve * cov
    }

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
                                    gbd_id=m, sex_id=sex_id, 
                                    location_id=location, 
                                    year_id=year_id, 
                                    age_group_id=age_group_id, 
                                    decomp_step="step4"))

  # Find the median, take 1% of that for the floor (consistent with DisMod default for linear floor)
	  eti_draws$floor <- apply(eti_draws[,6:1005], 1, median)
	  eti_draws$floor <- eti_draws$floor * 0.01

	  eti_draws <- join(eti_draws, adj_matrix, by="modelable_entity_id")

	# If rotavirus, join with impact draws #
	  if(m == 1219){
	    eti_draws <- join(eti_draws, rv_impact, by="year_id")
	  }

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

# Adjusting for rotavirus vaccine after calculating the fatal and non-fatal PAFs. A very simple
  # simulation shown here suggests that adjusting the proportion before calculating PAFs systematically
  # but marginally increases the final PAF.
  # Example: Say your proportion is 10%, Odds is 10, and the vaccine impact is 60%.
    # Fill in the equation such that
    # prop_adj = 0.1 * (1-0.6) / (1 - 0.1 * 0.6) = 0.046
    # prop_adj_paf = prop_adj * (1-1/10) = 0.0383
  # Contrast that with adjusting the PAFs
    # paf = 0.1 * (1/10) = 0.09
    # paf_adj = paf * (1 - 0.6) / (1 - paf * 0.6) = 0.0381

    # Convert to logit, adjust for inpatient status #
      fdraw <- inv.logit(logit(draw) - scalar)

##----------------------------------------------------------------------------------------------------
## We have adjusted for sensitivity/specificity before modeling in GBD 2019
    # Adjust for diagnostics
    # fdraw <- (fdraw + sp - 1) / (sp + se - 1)
    # draw <- (draw + sp - 1) / (sp + se - 1)
    #
    # # Make sure it stays in range
    # fdraw <- ifelse(fdraw > 1, 1, ifelse(fdraw < 0, eti_draws$floor, fdraw))
    # draw <- ifelse(draw > 1, 1, ifelse(draw < 0, eti_draws$floor, draw))
##-----------------------------------------------------------------------------------------------------

    # Calculate PAF
    fdraw <- fdraw * (1 - 1/or_fatal)
    draw <- draw * (1 - 1/or_non_fatal)

    # Make sure it stays in range
    fdraw <- ifelse(fdraw < 0, eti_draws$floor, fdraw)
    draw <- ifelse(draw < 0, eti_draws$floor, draw)

  # Adjust the PAFs for rotavirus, not the proportions #
    if(m == 1219){
  # Make sure to include the denominator in this adjustment
  # Adjustment is PAFfinal = DisModPAF * (1-Rota_Cov * Rota_VE) / (1 - DisModPAF * Rota_Cov * Rota_VE)
      fdraw_v <- fdraw * (1 - eti_draws[,paste0("rv_impact_",i)]) / (1 - fdraw * eti_draws[,paste0("rv_impact_",i)])
      fdraw <- ifelse(eti_draws$age_group_id %in% c(4,5), fdraw_v, fdraw)

      draw_v <- draw * (1 - eti_draws[,paste0("rv_impact_",i)]) / (1 - draw * eti_draws[,paste0("rv_impact_",i)])
      draw <- ifelse(eti_draws$age_group_id %in% c(4,5), draw_v, draw)
    }

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
