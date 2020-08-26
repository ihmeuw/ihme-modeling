## Combine GEMS, GEMS-1A and MALED lab data ##
## and use these data to determine a hospital inpatient scalar
## for GBD 2019
######################################################

library(plyr)
library(ggplot2)
library(data.table)

### Set standard names ###
eti_info <- read.csv("/filepath/eti_rr_me_ids.csv")
eti_info <- subset(eti_info, model_source=="dismod" & rei_id!=183 & cause_id==302)

names.gbd <- c("Adenovirus","Aeromonas","Campylobacter enteritis","Cryptosporidiosis","Amoebiasis","Enteropathogenic E coli infection",
               "Norovirus","Rotaviral enteritis","Other salmonella infections","Shigellosis","Enterotoxigenic E coli infection", "Cholera")
base_names <- c("adenovirus","aeromonas","campylobacter","cryptosporidium","ehist","epec","etec","norovirus","rotavirus","salmonella","shigella","cholera")
lab_names <- paste0("lab_",base_names)

## Get MAL-ED Data ##
surv <- read.csv("filepath")
  surv$agedays <- surv$age
micro <- read.csv("filepath")
  lab_maled <- c("adeno","aeromonas","campy","crypto","ehist","epec","etec","nrv","rota","salmonella","shigella","vibrio")
  keep.micro <- micro[,c("datesurv","country_id","pid","agedays","flag_diarrhea","gemsdef",lab_maled)]
  keep.micro <- subset(keep.micro, flag_diarrhea==1)

maled <- join(keep.micro, surv[,c("pid","agedays","hosp")], by=c("pid","agedays"))

## Get GEMS data ##
lab_gems <- c("F18_RES_ADENO4041", "F16_AEROMONAS", "F16_CAMPY_JEJUNI_COLI", "F18_RES_CRYPTOSPOR",
              "F18_RES_ENTAMOEBA", "F17_tEPEC","F17_ETEC", "F19_NOROVIRUS", "F18_RES_ROTAVIRUS", "F16_SALM_NONTYPHI", "F16_SHIG_SPP","F16_VIB_CHOLERAE")
gems_data <- read.csv("/filepath/gems_final.csv")
gems_data <- subset(gems_data, case==1)
gems_original <- gems_data[,c("age","site.names",lab_gems,"F4B_ADMIT")]

## Get GEMS 1A data ##
gems <- read.csv("filepath")
gems <- subset(gems, case==1)

  # Global (all) campylobacter sub-species
  gems$F16_CAMPY <- with(gems, F16_CAMPY_JEJUNI+F16_CAMPY_COLI+F16_CAMPY_NONJEJ+F16_CAMPY_NONSPEC)
  gems$F16_CAMPY[gems$F16_CAMPY==2] <- 1
  # Combine Norovirus I and II
  gems$F19_NORO <- gems$F19_NORO_GI + gems$F19_NORO_GII
  gems$F19_NORO[gems$F19_NORO==2] <- 1
  # ESTA is a marker for heat-stable (ST) and ELTB for heat-labile (LT)
  gems$F17_ETEC <- gems$F17_RESULT_ESTA + gems$F17_RESULT_ELTB
  gems$F17_ETEC[gems$F17_ETEC==2] <- 1
  gems$F17_ETEC[is.na(gems$F17_ETEC)] <- 0
  # BFPA with or without EAE is tEPEC
  gems$F17_tEPEC <- gems$F17_RESULT_BFPA
  gems$F17_tEPEC[is.na(gems$F17_tEPEC)] <- 0
  # ADENO4041 is a subset of ADENOVIRUS positive samples
  gems$F18_RES_ADENO4041[gems$F18_RES_ADENO4041==3] <- 0
  gems$F18_RES_ADENO4041[is.na(gems$F18_RES_ADENO4041)] <- 0

  names.gems <- c("F18_RES_ADENO4041", "F16_AEROMONAS", "F16_CAMPY", "F18_RES_CRYPTOSPOR",
                  "F18_RES_ENTAMOEBA","F17_tEPEC", "F19_NORO", "F18_RES_ROTAVIRUS", "F16_SALM_NONTYPHI",
                  "F16_SHIG_SPP", "F17_ETEC", "F16_VIB_CHOLERAE")

  ########################################
  ## Match site names ##
  ########################################
  site.names <- c("The Gambia","Mali","Mozambique","Kisumu","West Bengal, Urban","Bangladesh","Pakistan")
  site.match <- data.frame(site.names, SITE=1:7)
  gems <- join(gems, site.match, by="SITE")

gems_1a <- gems[,c("agegroup","site.names",names.gems,"F4B_ADMIT","Study")]

#######################################################################################################################
## All data imported, clean and append ##
#######################################################################################################################
maled$msd <- ifelse(maled$gemsdef==1,1,0)
maled$inpatient <- ifelse(maled$hosp==1,1,0)
maled$site.names <- maled$country_id
maled$Study <- "MALED"
setnames(maled, lab_maled, base_names)

gems_original$msd <- 1
gems_original$inpatient <- ifelse(gems_original$F4B_ADMIT==1,1,0)
gems_original$Study <- "GEMS"
setnames(gems_original, lab_gems, base_names)

gems_1a$msd <- ifelse(gems_1a$Study=="MSD",1,0)
gems_1a$inpatient <- ifelse(gems_1a$F4B_ADMIT==1,1,0)
gems_1a$Study <- "GEMS-1A"
setnames(gems_1a, names.gems, base_names)

df <- rbind.fill(maled, gems_original, gems_1a)
df$sample_size <- 1
df$site.names <- paste0(df$Study,"_",df$site.names)
df$severe <- ifelse(df$Study=="GEMS",1,0)

write.csv(df, "filepath", row.names=F)

# How frequently are episodes hospitalized in general?
  hosp <- aggregate(cbind(sample_size, inpatient) ~ site.names + Study, data=df, function(x) sum(x, na.rm=T))
  hosp$mean <- hosp$inpatient / hosp$sample_size
  hosp$se <- sqrt(hosp$mean * (1-hosp$mean) / hosp$sample_size)
  hosp <- subset(hosp, se>0)
    m <- rma(yi=mean, sei=se, data=subset(hosp, Study!="GEMS"), method="DL", slab=site.names)
    summary(m)
    forest(m)

    m <- rma(yi=mean, sei=se, data=subset(hosp, Study=="MALED"), method="DL", slab=site.names)
    summary(m)
    forest(m)

    m <- rma(yi=mean, sei=se, data=subset(hosp, Study=="GEMS"), method="DL", slab=site.names)
    summary(m)
    forest(m)
######################################################################################################
######################################################################################################

pdf("filepath", height=6, width=8)
for(n in 1:11){
  name <- eti_info$name_colloquial[n]
  me_name <- eti_info$modelable_entity[n]
  me_id <- eti_info$modelable_entity_id[n]
  bundle <- eti_info$bundle_id[n]
  rei_name <- eti_info$rei_name[n]
  b <- base_names[n]
  df$cases <- df[,b]
  print(paste0("Regressing for ",name))
  for(t in c("msd","inpatient")){
    crosswalk <- ifelse(t=="msd","Moderate to Severe","Hospitalized")
    df$crosswalk <- df[,t]
    ## For fun, metafor on the proportion by site
    # Flip reference/non-reference to be consistent with DisMod values (>1 means more likely)
    lref <- aggregate(cbind(sample_size, cases) ~ site.names + Study, data=df[df$crosswalk==0,], FUN=sum)
    lnref <- aggregate(cbind(sample_size, cases) ~ site.names + Study, data=df[df$crosswalk==1,], FUN=sum)

      setnames(lnref, c("sample_size","cases"),c("n_sample_size","n_cases"))
      lmean <- merge(lref, lnref, by=c("site.names","Study"))
      lmean$mean <- lmean$cases/lmean$sample_size
      lmean$n_mean <- lmean$n_cases/lmean$n_sample_size

      # If data mean = 0, set to linear floor
      l_floor <- median(lmean$mean[lmean$mean>0]) * 0.01
        lmean$mean[lmean$mean==0] <- l_floor
      ln_floor <- median(lmean$n_mean[lmean$n_mean>0]) * 0.01
        lmean$n_mean[lmean$n_mean==0] <-ln_floor

      lmean$standard_error <- sqrt(lmean$mean * (1-lmean$mean)/lmean$sample_size)
      lmean$n_standard_error <- sqrt(lmean$n_mean * (1-lmean$n_mean)/lmean$n_sample_size)

      lmean$ratio <- lmean$mean / lmean$n_mean
      lmean$se <- sqrt(lmean$mean^2 / lmean$n_mean^2 * (lmean$standard_error^2/lmean$mean^2 + lmean$n_standard_error^2/lmean$n_mean^2))

      ## Use delta method to transform linear to log
      lmean$log_ratio <- log(lmean$ratio)
      lmean$delta_log_se <- sapply(1:nrow(lmean), function(i) {
        ratio_i <- lmean[i, "ratio"]
        ratio_se_i <- lmean[i, "se"]
        deltamethod(~log(x1), ratio_i, ratio_se_i^2)
      })

      ## Calculate the ratio in difference logit space
        lmean$se_logit_mean_1 <- sapply(1:nrow(lmean), function(i) {
          ratio_i <- lmean[i, "mean"]
          ratio_se_i <- lmean[i, "standard_error"]
          deltamethod(~log(x1/(1-x1)), ratio_i, ratio_se_i^2)
        })

        #calcuating standard error of dx2 in logit space
        lmean$se_logit_mean_2 <- sapply(1:nrow(lmean), function(a) {
          ratio_a <- lmean[a, "n_mean"]
          ratio_se_a <- lmean[a, "n_standard_error"]
          deltamethod(~log(x1/(1-x1)), ratio_a, ratio_se_a^2)
        })

        lmean$logit_ratio <- logit(lmean$mean) - logit(lmean$n_mean)
        lmean$logit_ratio_se <- with(lmean, sqrt(se_logit_mean_1^2 + se_logit_mean_2^2))

      # Drop where cases or n_cases = 0 but not where they both equal zero
      lmean$drop_var <- ifelse(lmean$cases == 0 & lmean$n_cases != 0, 1, ifelse(lmean$cases != 0 & lmean$n_cases == 0, 1, 0))

      lmean <- subset(lmean, se > 0 & drop_var == 0 & delta_log_se < 10)
        lmean <- lmean[, !names(lmean) %in% "drop_var"]

      # Drop if reference data have fewer than 10 samples
        lmean <- subset(lmean, n_sample_size >= 10)

      meta_reg <- rma(yi=log_ratio, sei=delta_log_se, data=lmean, method="DL", slab=site.names)
      summary(meta_reg)
      forest(meta_reg, transf=function(x) exp(x))
      title(paste0(crosswalk,": ",name))

    ## Prep data for use with all etiology inpatient data ##

     # dat_out <- lmean[lmean$Study!="GEMS",]
      dat_out <- lmean
      dat_out$location_match <- dat_out$Study # 1:length(dat_out$cases)

      # have separate NIDs for each site
      dat_out$Study <- dat_out$site.names

      setnames(dat_out, c("Study"), c("nid"))

      dat_out$age_bin <- "(0,5]"
      dat_out$year_bin <- "(2005,2015]"
      dat_out$age_start <- 0
      dat_out$age_end <- ifelse(dat_out$nid=="MALED",2,5)
      dat_out$year_start <- ifelse(dat_out$nid=="GEMS-1A",2010,2009)
      dat_out$year_end <- ifelse(dat_out$nid=="GEMS-1A",2015,2012)

    dat_out <- dat_out[,-which(names(dat_out) %in% c("site.names"))]

    if(t == "inpatient"){
      write.csv(dat_out, paste0("filepath"), row.names=F)
    } else {
      write.csv(dat_out, paste0("filepath"), row.names=F)
    }
  }
}
dev.off()

######################################################################################################
## Compare GEMS as the severe definition to GEMS-1A and MALED
######################################################################################################
msd <- aggregate(cbind(adenovirus, sample_size) ~ severe, data=df, FUN=sum)
msd
msd$mean <- msd$adenovirus / msd$sample_size

