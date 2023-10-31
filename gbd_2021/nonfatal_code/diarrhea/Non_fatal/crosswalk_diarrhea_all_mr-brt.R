rm(list = ls())

library(metafor, lib.loc="FILEPATH")
library(msm)
library(plyr)
library(boot)
library(ggplot2)
library(openxlsx)
library(readxl)


source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

`%notin%` <- Negate(`%in%`)

loc_set <- get_ids("location_set")
locs <- as.data.table(get_location_metadata(location_set_id = 35, gbd_round_id = 7))

output_vals <- data.frame()

gbd_round_id = 7

## get SDI as a potential predictor

source("FILEPATH")
sdi <- get_covariate_estimates(location_id="all", covariate_id=881, decomp_step="iterative", year_id=1980:2020)
sdi <- join(sdi, locs[,c("location_id","ihme_loc_id")], by="location_id")
sdi$sdi <- sdi$mean_value

haqi <- get_covariate_estimates(location_id="all", covariate_id=1099, decomp_step="iterative", year_id=1980:2020)
haqi <- join(haqi, locs[,c("location_id","ihme_loc_id")], by="location_id")
haqi$haqi <- haqi$mean_value


## load exclusions tables

exclusions <- as.data.frame(read_excel("FILEPATH"))

study_nids <- c()

## create exclusions list

setnames(exclusions,"Include?","Include")
ex1 <- unique(subset(exclusions,Include==0)$NID)
ex2 <- unique(study_nids)
ex_all <- c(ex1,ex2)

##----------------------------------------------------------------------------
## Pull all data
##----------------------------------------------------------------------------

## Only do this once at start of round ##

## Save bundle version

bundle_id <- 3
decomp_step <- 'iterative'
gbd_round_id <- 7

result_bv <- save_bundle_version(bundle_id, decomp_step, gbd_round_id, include_clinical = c("claims, inpatient only","inpatient"))
print(result_bv) ## -----> use this bundle version in the save_crosswalk_version process at the very end

## Use this code if you need to add data from previous round to bundle
## check for most recent bundle

iterative_bvid <- result_bv$bundle_version_id

## Pull old/step2 data

dt_drh <- as.data.table(get_bundle_version(bundle_version_id = iterative_bvid, export = FALSE, fetch = "all"))
dt_new <- as.data.table(get_bundle_version(bundle_version_id = iterative_bvid, export = FALSE, fetch = "new"))
dt_2019 <- as.data.table(get_bundle_version(bundle_version_id = 13046, export = FALSE, fetch = "all"))

write.csv(dt_drh, "FILEPATH", row.names=F)

################################################################
## STEP 2 -- RECODE NEW DATA, CLINICAL DATA, and GBD ROUND
################################################################

#dt_new$gbd_2020_new <- 1
#dt_new$gbd_round <- as.integer(dt_drh$gbd_round)
#dt_new$gbd_round <- 2020

table(dt_drh$gbd_round)
nid_2019 <- unique(dt_2019$nid)
seq_2019 <- unique(dt_2019$seq)

# recode gbd_round to include new data, where gbd_round == 2020 for new seqs

#dt_lri[seq %in% c(seq_new), gbd_round := 2020]
#dt_lri[nid %in% c(nid_new), gbd_round := 2020]
#dt_lri[is.na(gbd_round), gbd_round := 2019]

dt_drh <- dt_drh[seq %notin% seq_2019 & clinical_data_type == "", gbd_round := 2020]
#dt_drh$gbd_round <- ifelse(dt_drh$seq %in% seq_2019, dt_drh$gbd_round, ifelse(dt_drh$clinical_data_type != "", dt_drh$gbd_round, 2020))
dt_drh <- dt_drh[gbd_round == 2020, gbd_2020_new := 1]
table(dt_drh$gbd_round)

# lit recode

nid_new <- unique(dt_new$nid)

dt_drh <- dt_drh[nid %in% nid_new, gbd_round := 2020]
dt_drh <- dt_drh[nid %in% nid_new, gbd_2020_new := 1]

# clinical recode

table(dt_drh$clinical_data_type)

dt_drh <- dt_drh[clinical_data_type == "claims, inpatient only", clinical_data_type := "claims"]

# recode cv_ columns to properly aligned with the right type of clinical

dt_drh <- dt_drh[clinical_data_type == "inpatient", cv_inpatient := 1]
dt_drh <- dt_drh[clinical_data_type == "claims", cv_marketscan := 1]

View(dt_drh[field_citation_value %like% "Truven" & clinical_data_type == ""]) 
maybe_claims <- dt_drh[field_citation_value %like% "Claims"]
unique(maybe_claims$field_citation_value)
dt_drh[field_citation_value %like% "Claims", cv_marketscan := 1]
dt_drh[field_citation_value %like% "Claims", clinical_data_type := "claims"]

#dt_drh <- dt_drh[clinical_data_type =="claims" | clinical_data_type == "inpatient", gbd_2020_new := 1]
#dt_drh <- dt_drh[clinical_data_type =="claims" | clinical_data_type == "inpatient", gbd_round := 2020]

# verify - number should match total of df_new

table(dt_drh$gbd_round)
table(dt_drh$gbd_2020_new)

# save!

write.csv(dt_drh, "FILEPATH", row.names=F)

#'%!in%' <- function(x,y)!('%in%'(x,y))

#step1_bvid <- 1964
#step1_data <- as.data.table(get_bundle_version(bundle_version_id = step1_bvid, export = T))
#step1_nid <- unique(step1_data$nid)

#View(all_data[nid %in% step1_nid])
#View(df_step2[nid %in% step1_nid])
#View(df_step2[nid %in% step1_nid & gbd_2019_new == 0])

## CLINICAL crosswalk

# All data
all_data <- copy(dt_drh)

non_clin_df <- subset(all_data, clinical_data_type == "")
non_clin_df$cv_hospital <- ifelse(non_clin_df$cv_hospital==1,1,ifelse(non_clin_df$cv_clin_data==1,1,0))
non_clin_df$cv_clin_data <- 0
non_clin_df$mean_original <- non_clin_df$mean

# Calculate cases and sample size (missing in some data)

sample_size <- with(non_clin_df, mean*(1-mean)/standard_error^2)
cases <- non_clin_df$mean * sample_size
non_clin_df$cases <- as.numeric(ifelse(is.na(non_clin_df$cases), cases, non_clin_df$cases))
non_clin_df$sample_size <- as.numeric(ifelse(is.na(non_clin_df$sample_size), sample_size, non_clin_df$sample_size))

clin_df <- subset(all_data, clinical_data_type != "")

clin_df$is_reference <- 0
clin_df$cases <- clin_df$mean * clin_df$sample_size
clin_df$age_end <- ifelse(clin_df$age_end > 99, 99, clin_df$age_end)
clin_df$age_mid <- floor((as.numeric(clin_df$age_start) + clin_df$age_end)/2)

diarrhea <- rbind.fill(non_clin_df, clin_df)

## Convert incidence to prevalence
prevalence_means <- convert_inc_prev(clin_df, duration = 4.2, duration_lower = 4.1, duration_upper = 4.4)
clin_df$mean <- prevalence_means$mean_prevalence
clin_df$standard_error <- prevalence_means$standard_error_prevalence

# Calculate cases and sample size (missing in inpatient data)
sample_size <- with(clin_df, mean*(1-mean)/standard_error^2)
cases <- with(clin_df, mean * sample_size)
clin_df$cases <- ifelse(is.na(clin_df$cases), cases, clin_df$cases)
clin_df$sample_size <- ifelse(is.na(clin_df$sample_size), sample_size, clin_df$sample_size)

clin_df$cases <- clin_df$mean * clin_df$sample_size
clin_df$lower <- ""
clin_df$upper <- ""
clin_df$cv_inpatient <- ifelse(clin_df$clinical_data_type=="inpatient",1,0)

# The data in Japan are extremely high, try not crosswalking
jpn_locs <- locs$location_id[locs$parent_id==67]
clin_df$cv_inpatient <- ifelse(clin_df$location_id %in% jpn_locs, 0, clin_df$cv_inpatient)

clin_df$cv_marketscan <- ifelse(clin_df$clinical_data_type=="claims",1,0)
clin_df$cv_clin_data <- 1

# Don't adjust Taiwain claims data
clin_df$cv_marketscan[clin_df$location_name=="Taiwan" & clin_df$cv_marketscan==1] <- 0
clin_df$measure <- "prevalence"

# Calculate cases and sample size (missing in inpatient data) ## -----> was already done above
sample_size <- with(clin_df, mean*(1-mean)/standard_error^2)
cases <- clin_df$mean * sample_size
clin_df$cases <- ifelse(is.na(clin_df$cases), cases, clin_df$cases)
clin_df$sample_size <- ifelse(is.na(clin_df$sample_size), sample_size, clin_df$sample_size)
clin_df$mean_original <- clin_df$mean

diarrhea <- rbind.fill(non_clin_df, clin_df)
#table(diarrhea$nid %in% step1_nid)

##-----------------------------------------------------------------------------
# Recoding Clinical Informatics
# inpatient data to be 'cv_inpatient = 1' and identifying that by extractor.
# I will leave a cv_miscoded = 1 if cv_inpatient = 1 or cv_inpatient_sample = 1
# but the extractor isn't "x or y".
diarrhea$cv_inpatient_lit <- as.numeric(with(diarrhea, ifelse(cv_clin_data==1,0,ifelse(cv_inpatient==1,1,ifelse(cv_inpatient_sample==1,1,0)))))
diarrhea$cv_inpatient_sample <- 0

diarrhea$is_reference <- ifelse(diarrhea$cv_hospital==0 & diarrhea$cv_clin_data==0 & diarrhea$cv_inpatient_lit==0,1,0)

diarrhea_full <- diarrhea

# Join with SDI as it is a predictor for cv_hospital
diarrhea_full$year_id <- floor((diarrhea_full$year_start + diarrhea_full$year_end) / 2)
diarrhea_full <- join(diarrhea_full, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
diarrhea_full$sdi <- round(diarrhea_full$sdi,2)


View(diarrhea_full[field_citation_value %like% "Claims" & is.na(clinical_data_type)])

# Lots of hospital data in UK UTLAs, doesn't match in current code
# Set to England
# diarrhea_full <- join(diarrhea_full, locs[,c("location_id","parent_id")], by="location_id")
# diarrhea_full$location_id <- ifelse(diarrhea_full$parent_id == 4749, 4749, diarrhea_full$location_id)

## Set some parameters ##
#ages_2020 = get_age_metadata(age_group_set_id=19, gbd_round_id=7)
#age_bins_2020 <- ages_2020$age_group_years_start

## GBD 2020 age bins
#age_bins <- c(0,0.0192,0.0767,0.5,1,2,5,20,40,60,80,100)
#age_bins <- c(0,0.0192,0.0767,0.5,1,2,seq(5,100,5))

## GBD 2019 age bins
age_bins <- c(0,1,5,10,20,40,60,80,100)
age_bins <- c(0,1,seq(5,100,5))

## Subset to working data frame ##
df <- diarrhea_full[,c("location_name","location_id","nid","age_start","sex","age_end","year_start","year_end","mean","standard_error","cases","sample_size",
                       "cv_diag_selfreport","cv_hospital","cv_inpatient","cv_clin_data","is_reference","is_outlier","group_review","cv_marketscan", "gbd_round","gbd_2019_new")]

df <- join(df, locs[,c("location_id","region_name","super_region_name","ihme_loc_id")], by="location_id")


## Recode necessary varaibles that should be numeric for computational purposes later (make sure they aren't characters)
df <- as.data.table(df)
df[is.na(df)] <- ""
df$mean<-as.numeric(df$mean)
df$sample_size<-as.numeric(df$sample_size)
df$age_start<-as.numeric(df$age_start)
df$age_end<-as.numeric(df$age_end)
df <- as.data.frame(df)

######################################################################
## Clinical data crosswalk. This is for data from the clinical
## informatics team that produces estimates of the incidence of diarrhea.
######################################################################

#################################################################################
## Separate crosswalks for inpatient, claims clinical data ##
#################################################################################
##----------------------------------------------------------------------------------------------------------

cv_inpatient <- bundle_crosswalk_collapse(df, covariate_name="cv_inpatient", age_cut=age_bins, year_cut=c(seq(1980,2015,5),2020), merge_type="between", location_match="exact", include_logit = T)

cv_marketscan <- bundle_crosswalk_collapse(df, covariate_name="cv_marketscan", age_cut=age_bins, year_cut=c(seq(1980,2015,5),2020), merge_type="between", location_match="exact", include_logit = T)

cv_inpatient$year_id <- floor((cv_inpatient$year_end + cv_inpatient$year_start)/2)
cv_inpatient$location_id <- cv_inpatient$location_match
cv_inpatient$age_mid <- floor((cv_inpatient$age_end + cv_inpatient$age_start)/2)

cv_inpatient <- join(cv_inpatient, sdi[,c("location_id","year_id","sdi")], by=c("year_id","location_id"))
#cv_inpatient <- join(cv_inpatient, haqi[,c("location_id","year_id","haqi","ihme_loc_id")], by=c("year_id","ihme_loc_id"))
cv_inpatient <- join(cv_inpatient, locs[,c("location_id","super_region_name","ihme_loc_id")], by="location_id")
cv_inpatient$high_income <- ifelse(cv_inpatient$super_region_name=="High-income",1,0)
cv_inpatient$study_id <- paste0(cv_inpatient$location_match,"_",cv_inpatient$age_bin)

cv_marketscan$year_id <- floor((cv_marketscan$year_end + cv_marketscan$year_start)/2)
cv_marketscan$location_id <- cv_marketscan$location_match
cv_marketscan$age_mid <- floor((cv_marketscan$age_end + cv_marketscan$age_start)/2)

# # Test a couple linear models for predictors
ggplot(cv_inpatient, aes(x=age_mid, y=log_ratio)) + geom_point() + stat_smooth(method="loess", se=F)
summary(lm(log_ratio ~ age_mid^2 + age_mid + sdi, data=cv_inpatient))
ggplot(cv_inpatient, aes(x=sdi, y=log_ratio)) + geom_point(aes(col=super_region_name)) + stat_smooth(method="loess", se=F)
summary(lm(log_ratio ~ sdi, data=cv_inpatient))

#############

cv_inpatient <- fread("FILEPATH")
cv_marketscan <- fread("FILEPATH")

# cv_inpatient <- cv_inpatient[match_id %notin% out_match]

##########################################################################
############################## Inpatient! ################################
cv_inpatient$match_id <- paste0(cv_inpatient$nid,"_",cv_inpatient$n_nid)
cv_inpatient$age_u5 <- ifelse(cv_inpatient$age_end <=5, 1, 0)

# ## Run an MR-BRT model. ##

fit1 <- run_mr_brt(
  output_dir = paste0("FILEPATH"),
  model_label = "cv_inpatient_2020",
  data = cv_inpatient, #[cv_inpatient$ratio>1,],
  mean_var = "logit_ratio",
  se_var = "logit_ratio_se",
  # covs = list(cov_info("age_mid","X",
  #                      degree = 3, n_i_knots=4,
  #                      l_linear=FALSE, r_linear = FALSE, bspline_gprior_mean = "0,0,0,0,0", bspline_gprior_var = "inf,inf,inf,inf,inf")),
  # covs = list(cov_info("age_mid","X",
  #                      degree = 2, n_i_knots = 3,
  #                      l_linear=TRUE, r_linear = FALSE, bspline_gprior_mean = "0,0,0,0", bspline_gprior_var = "inf,inf,inf,inf"),
  #             cov_info("sdi","X")),
  # covs = list(cov_info("sdi","X",
  #                      degree = 3, n_i_knots=4,
  #                      l_linear=TRUE, r_linear = FALSE, bspline_gprior_mean = "0,0,0,0,0", bspline_gprior_var = "inf,inf,inf,inf,inf")),
  overwrite_previous = TRUE,
  study_id = "match_id",
  method = "trim_maxL",
  trim_pct = 0.1
)

df_pred <- data.frame(intercept=1, age_mid=seq(0,100,1))
#df_pred <- data.frame(expand.grid(intercept=1, sdi = seq(0,1,0.01), age_mid=seq(0,100,1)))
# df_pred <- data.frame(expand.grid(intercept=1, sdi = seq(0.2,1,0.01), age_u5=c(0,1)))
#  df_pred$age_u5 <- ifelse(df_pred$age_mid < 5, 1, 0)
pred1 <- predict_mr_brt(fit1, newdata = df_pred)

pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries
#preds$X_age_mid <- seq(0,100,1)
preds$age_mid <- df_pred$age_mid
# preds$sdi <- preds$X_sdi
# preds$age_u5 <- preds$X_age_u5

# Calculate log se
preds$inpatient_se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
#
# # Convert the mean and standard_error to linear space
preds$inpatient_linear_se <- sapply(1:nrow(preds), function(i) {
  ratio_i <- preds[i, "Y_mean"]
  ratio_se_i <- preds[i, "inpatient_se"]
  deltamethod(~exp(x1)/(1+exp(x1)), ratio_i, ratio_se_i^2)
})
preds$inpatient_ratio <- inv.logit(preds$Y_mean)
preds$inpatient_logit <- preds$Y_mean

## Create essentially a forest plot
mod_data$label <- with(mod_data, paste0(nid,"_",ihme_loc_id,"_",age_start,"-",age_end,"_",year_start,"-",year_end))
f3 <- ggplot(mod_data[mod_data$outlier==0,], aes(ymax=exp(log_ratio + delta_log_se*1.96)+1, ymin=exp(log_ratio - delta_log_se*1.96)+1)) + geom_point(aes(y=exp(log_ratio)+1, x=label)) +
  geom_errorbar(aes(x=label), width=0) + scale_y_continuous("Linear ratio", limits=c(0,10)) +
  theme_bw() + xlab("") + coord_flip() + ggtitle(paste0("Diarrhea inpatient ratio (",round(preds$inpatient_ratio,3),")")) +
  geom_hline(yintercept=1) + geom_hline(yintercept=preds$inpatient_ratio, col="purple") +
  geom_rect(data=preds, aes(ymin=exp(Y_mean_lo)+1, ymax=exp(Y_mean_hi)+1, xmin=0, xmax=length(mod_data$label[mod_data$outlier==0])+1), alpha=0.1, fill="purple")
print(f3)

### Rejoin inpatient ratio with clinical data  ###
clin_df$age_mid <- ceiling((clin_df$age_end + clin_df$age_start)/2)
# clin_df <- clin_df[,-which(names(clin_df) %in% c("inpatient_ratio","inpatient_linear_se","inpatient_logit"))]
clin_df$year_id <- round((clin_df$year_start + clin_df$year_end)/2,0)
clin_df <- join(clin_df, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
clin_df$sdi <- round(clin_df$sdi, 2)

#clin_df <- join(clin_df, preds[,c("age_mid","inpatient_ratio","inpatient_linear_se","inpatient_logit","sdi","inpatient_se")], by=c("sdi","age_mid"))
clin_df <- cbind(clin_df, preds[1,c("inpatient_ratio","inpatient_linear_se","inpatient_logit","inpatient_se")])

#  # clin_df <- clin_df[,-which(names(clin_df) %in% c("inpatient_ratio","inpatient_linear_se","inpatient_logit"))]
# clin_df <- join(clin_df, preds[,c("sdi","inpatient_ratio","inpatient_linear_se","inpatient_logit")], by="sdi")

adf <- clin_df[,c("mean","age_start","age_end","inpatient_logit","location_id")]
adf_median <- median(adf$mean)
bdf <- adf
bdf_median <- median(bdf$mean)
cdf <- non_clin_df[,c("mean","age_start","age_end","location_id")]
cdf_median <- median(cdf$mean)
cdf$age_start <- round(cdf$age_start, 0)
adf$type <- "Unadjusted"
bdf$type <- "Adjusted"
cdf$type <- "Non-clinical"
bdf$mean <- inv.logit(logit(bdf$mean) + bdf$inpatient_logit)

ddf <- data.frame(type="Filler", age_start=seq(0,98,2), age_end=seq(1,99,2), mean=0.01, location_id=1)

#ggplot(rbind.fill(adf,bdf,cdf,ddf), aes(x=factor(age_start), y=mean, col=type)) + geom_boxplot() + theme_bw() + scale_y_log10() +
#   geom_hline(yintercept=0.005) + geom_hline(yintercept=0.01) + ggtitle("All locations, age curve")

ggplot(subset(rbind.fill(adf,bdf,cdf,ddf), location_id==520), aes(x=factor(age_start), y=mean, col=type)) + geom_boxplot() + theme_bw() + scale_y_log10() +
  geom_hline(yintercept=bdf_median, col="red") + geom_hline(yintercept=cdf_median, col="#00CC00") + geom_hline(yintercept=adf_median, col="#3399FF") + geom_hline(yintercept=0.10) + ggtitle("Yunnan")
ggplot(subset(rbind.fill(adf,bdf,cdf,ddf), location_id==35460), aes(x=factor(age_start), y=mean, col=type)) + geom_boxplot() + theme_bw() + scale_y_log10() +
  geom_hline(yintercept=bdf_median, col="red") + geom_hline(yintercept=cdf_median, col="#00CC00") + geom_hline(yintercept=adf_median, col="#3399FF") + geom_hline(yintercept=0.10) + ggtitle("Kagawa")
ggplot(subset(rbind.fill(adf,bdf,cdf,ddf), location_id==4726), aes(x=factor(age_start), y=mean, col=type)) + geom_boxplot() + theme_bw() + scale_y_log10() +
  geom_hline(yintercept=bdf_median, col="red") + geom_hline(yintercept=cdf_median, col="#00CC00") + geom_hline(yintercept=adf_median, col="#3399FF") + geom_hline(yintercept=0.10) + ggtitle("Bali")
ggplot(subset(rbind.fill(adf,bdf,cdf,ddf), location_id==570), aes(x=factor(age_start), y=mean, col=type)) + geom_boxplot() + theme_bw() + scale_y_log10() +
  geom_hline(yintercept=bdf_median, col="red") + geom_hline(yintercept=cdf_median, col="#00CC00") + geom_hline(yintercept=adf_median, col="#3399FF") + geom_hline(yintercept=0.10) + ggtitle("Washington")
ggplot(subset(rbind.fill(adf,bdf,cdf,ddf), location_id==44676), aes(x=factor(age_start), y=mean, col=type)) + geom_boxplot() + theme_bw() + scale_y_log10() +
  geom_hline(yintercept=bdf_median, col="red") + geom_hline(yintercept=cdf_median, col="#00CC00") + geom_hline(yintercept=adf_median, col="#3399FF") + geom_hline(yintercept=0.10) + ggtitle("Blackpool")
ggplot(subset(rbind.fill(adf,bdf,cdf,ddf), location_id==59), aes(x=factor(age_start), y=mean, col=type)) + geom_boxplot() + theme_bw() + scale_y_log10() +
  geom_hline(yintercept=bdf_median, col="red") + geom_hline(yintercept=cdf_median, col="#00CC00") + geom_hline(yintercept=adf_median, col="#3399FF") + geom_hline(yintercept=0.10) + ggtitle("Latvia")


##########################################################################
######################## Claims! ################################
cv_marketscan$match_id <- paste0(cv_marketscan$nid,"_",cv_marketscan$n_nid)
cv_marketscan$study_id <- paste0(cv_marketscan$location_match,"_",cv_marketscan$age_bin)
cv_marketscan$age_mid <- floor((cv_marketscan$age_end + cv_marketscan$age_start)/2)

#cv_marketscan$age_mid <- cv_marketscan$age_end
# ## Run an MR-BRT model. ##
fit1 <- run_mr_brt(
  output_dir = paste0("FILEPATH"),
  model_label = "cv_marketscan",
  data = cv_marketscan, #[cv_marketscan$ratio>1,],
  mean_var = "logit_ratio",
  se_var = "logit_ratio_se",
  covs = list(cov_info("age_mid","X",
                       degree = 2, n_i_knots = 3,
                       l_linear=FALSE, r_linear = TRUE, bspline_gprior_mean = "0,0,0,0", bspline_gprior_var = "inf,inf,inf,inf")),
  #i_knots=c("2,5,20,60"))), #, bspline_cvcv = "concave")),
  overwrite_previous = TRUE,
  study_id = "study_id",
  method = "trim_maxL",
  trim_pct = 0.1
)
check_for_outputs(fit1)
df_pred <- data.frame(intercept=1, age_mid=seq(0,100,1))
pred1 <- predict_mr_brt(fit1, newdata = df_pred)
check_for_preds(pred1)
pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries
preds$age_mid <- preds$X_age_mid

mod_data <- fit1$train_data
mod_data$outlier <- floor(abs(mod_data$w - 1))
ggplot(preds, aes(x=X_age_mid, y=exp(Y_mean))) + geom_line() +
  geom_ribbon(aes(ymin=exp(Y_mean_lo), ymax=exp(Y_mean_hi)), alpha=0.3) +
  geom_point(data=mod_data, aes(x=age_mid, y=exp(log_ratio), size=1/se^2, col=factor(outlier))) + guides(size=F) + scale_color_manual(values=c("black","red")) + guides(col=F) +
  geom_hline(yintercept=1, lty=2, col="red") + scale_y_continuous("Log ratio") + scale_x_continuous("Age", limits=c(0,100)) + theme_bw() + ggtitle("Claims ratio")

# Calculate log se
preds$marketscan_se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
# # Convert the mean and standard_error to linear space
preds$marketscan_linear_se <- sapply(1:nrow(preds), function(i) {
  ratio_i <- preds[i, "Y_mean"]
  ratio_se_i <- preds[i, "marketscan_se"]
  deltamethod(~exp(x1)/(1+exp(x1)), ratio_i, ratio_se_i^2)
})
preds$marketscan_ratio <- inv.logit(preds$Y_mean)
preds$marketscan_logit <- preds$Y_mean

### Rejoin marketscan ratio with clinical data  ###
clin_df <- join(clin_df, preds[,c("age_mid","marketscan_ratio","marketscan_linear_se","marketscan_logit","marketscan_se")], by="age_mid")

# Keep a record of the values
p <- preds[,c("X_age_mid","marketscan_ratio","marketscan_linear_se","Y_mean","Y_mean_lo","Y_mean_hi")]
p <- unique(p)
# p$variable <- c("cv_marketscan","cv_inpatient")
# p$count_obs <- c(length(cv_inpatient$ratio), length(cv_marketscan$ratio) + length(cv_ms_inp$ratio))
output_vals <- rbind.fill(output_vals, p)

#clin_df <- clin_df[, -which(names(clin_df) %in% c("age_mid","ratio","linear_se","parent_id","ratio","linear_se"))]

#################################################################################
## Perform the crosswalks for the clinical data ##
#################################################################################
clin_df$logit_mean <- logit(clin_df$mean)
clin_df$logit_se <- sapply(1:nrow(clin_df), function(i) {
  ratio_i <- as.numeric(clin_df[i, "mean"])
  ratio_se_i <- as.numeric(clin_df[i, "standard_error"])
  deltamethod(~log(x1/(1-x1)), ratio_i, ratio_se_i^2)
})

# Taiwan exception
clin_df$cv_marketscan <- ifelse(clin_df$location_name == "Taiwan", 0, clin_df$cv_marketscan)
clin_df$mean <- ifelse(clin_df$cv_inpatient==1, inv.logit(clin_df$logit_mean + clin_df$inpatient_logit),
                       ifelse(clin_df$cv_marketscan==1, inv.logit(clin_df$logit_mean + clin_df$marketscan_logit), clin_df$mean))

#clin_df$mean <- ifelse(clin_df$location_name=="Taiwan", clin_df$mean,
#                     ifelse(clin_df$cv_inpatient == 1, inv.logit(clin_df$logit_mean + clin_df$inpatient_logit), inv.logit(clin_df$logit_mean + clin_df$marketscan_logit)))

std_inpatient <- sqrt(with(clin_df, standard_error^2 * inpatient_linear_se^2 + standard_error^2*inpatient_ratio^2 + inpatient_linear_se^2*mean^2))
std_marketscan <- sqrt(with(clin_df, standard_error^2 * marketscan_linear_se^2 + standard_error^2*marketscan_ratio^2 + marketscan_linear_se^2*mean^2))

# std_inpatient <- inv.logit(sqrt(clin_df$logit_se^2 + clin_df$inpatient_se^2))
# std_marketscan <- inv.logit(sqrt(clin_df$logit_se^2 + clin_df$marketscan_se^2))

clin_df$standard_error <- ifelse(clin_df$cv_inpatient == 1 , std_inpatient, ifelse(clin_df$cv_marketscan == 1, std_marketscan, clin_df$standard_error))

write.csv(clin_df, "FILEPATH")

## Rbind back with full dataset ##
diarrhea <- rbind.fill(clin_df, non_clin_df)

##-----------------------------------------------------------------------------
# Recoding Clinical Informatics
# inpatient data to be 'cv_inpatient = 1' and identifying that by extractor.
# I will leave a cv_miscoded = 1 if cv_inpatient = 1 or cv_inpatient_sample = 1
# but the extractor isn't "x and y".
diarrhea$cv_inpatient_lit <- as.numeric(with(diarrhea, ifelse(cv_clin_data==1,0,ifelse(cv_inpatient==1,1,ifelse(cv_inpatient_sample==1,1,0)))))

diarrhea$cv_inpatient_sample <- 0

diarrhea$is_reference <- ifelse(diarrhea$cv_hospital==0 & diarrhea$cv_clin_data==0 & diarrhea$cv_inpatient_lit==0,1,0)

#######################################################################################################################
## Now crosswalk for the literature indications (hospitalized, clinical data) ##

## Subset to working data frame ##
df <- diarrhea[,c("location_name","location_id","nid","age_start","sex","age_end","year_start","year_end","mean","standard_error","cases","sample_size",
                  "cv_inpatient_lit","cv_hospital","cv_inpatient","is_reference","group_review","is_outlier", "gbd_round")]
df <- join(df, locs[,c("location_id","region_name","super_region_name","ihme_loc_id")], by="location_id")
df<-as.data.frame(df)

######################################################################
## Hospital data crosswalk. This is for data that report the incidence
## of severe or hospitalized diarrhea. There are plenty of sources that
## report the incidence of both clinician-diagnosed (reference) and
## hospitalized diarrhea so we are going to use within study-age-location
## matching for preparing these data for the meta-regression.
######################################################################
cv_hospital <- bundle_crosswalk_collapse(df, covariate_name="cv_hospital", age_cut=age_bins, year_cut=c(seq(1980,2015,5),2020), merge_type="within", location_match="exact", include_logit=T)

cv_hospital$year_id <- floor((cv_hospital$year_end + cv_hospital$year_start)/2)
cv_hospital$location_id <- cv_hospital$location_match
cv_hospital$age_mid <- floor((cv_hospital$age_end + cv_hospital$age_start)/2)

cv_hospital <- join(cv_hospital, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
cv_hospital <- join(cv_hospital, locs[,c("location_id","super_region_name","location_name","ihme_loc_id")], by="location_id")

# Values here should not be greater than 1 (inpatient incidence cannot be greater than population incidence),
# so I am going to shift the values by -1.
# Save a file for investigation
write.csv(cv_hospital[cv_hospital$ratio<=1,], "FILEPATH", row.names=F)

cv_hospital <- subset(cv_hospital, ratio > 1)
cv_hospital$log_ratio <- cv_hospital$log_ratio - 1
write.csv(cv_hospital, "FILEPATH", row.names=F)

# Test a couple linear models for predictors
summary(lm(log_ratio ~ age_mid, data=cv_hospital))
summary(lm(log_ratio ~ sdi, data=cv_hospital))

fit2 <- run_mr_brt(
  output_dir = paste0("FILEPATH"),
  model_label = "cv_hospital_2020",
  data = cv_hospital[cv_hospital$ratio < 20, ],
  mean_var = "log_ratio",
  se_var = "delta_log_se",
  overwrite_previous = TRUE,
  study_id = "nid",
  method = "trim_maxL",
  trim_pct = 0.1
)

check_for_outputs(fit2)
df_pred <- data.frame(expand.grid(intercept = 1))
pred1 <- predict_mr_brt(fit2, newdata = df_pred)
check_for_preds(pred1)
pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries
mod_data <- fit2$train_data

# Calculate log se
preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
# Convert the mean and standard_error to linear space
preds$linear_se <- deltamethod(~exp(x1), preds$Y_mean, preds$se^2)
preds$ratio <- exp(preds$Y_mean) + 1

# Pull in DisMod 2017 values
dismod <- data.frame(mean=1/0.18, lower=1/0.16, upper=1/0.20)

## Produce an approximation of a funnel plot
mod_data$outlier <- abs(mod_data$w - 1)
f1 <- ggplot(mod_data, aes(x=log_ratio, y=delta_log_se, col=factor(outlier))) + geom_point(size=3) + geom_vline(xintercept=preds$Y_mean) + scale_y_reverse("Standard error") +
  theme_bw() + scale_x_continuous("Log ratio") + scale_color_manual("", labels=c("Used","Trimmed"), values=c("#0066CC","#CC0000")) + ggtitle(paste0("Diarrhea", " Hospital ratio")) +
  geom_vline(xintercept=preds$Y_mean_lo, lty=2) + geom_vline(xintercept=preds$Y_mean_hi, lty=2)
print(f1)

## Create essentially a forest plot
mod_data$label <- with(mod_data, paste0(nid,"_",ihme_loc_id,"_",age_start,"-",age_end,"_",year_start,"-",year_end,"_",sex))
f3 <- ggplot(mod_data[mod_data$outlier==0,], aes(ymax=exp(log_ratio + delta_log_se*1.96)+1, ymin=exp(log_ratio - delta_log_se*1.96)+1)) + geom_point(aes(y=exp(log_ratio)+1, x=label)) +
  geom_errorbar(aes(x=label), width=0) + scale_y_continuous("Linear ratio", limits=c(0,10)) +
  theme_bw() + xlab("") + coord_flip() + ggtitle(paste0("Diarrhea hospital ratio (",round(preds$ratio,3),")")) +
  geom_hline(yintercept=1) + geom_hline(yintercept=preds$ratio, col="purple") +
  geom_rect(data=preds, aes(ymin=exp(Y_mean_lo)+1, ymax=exp(Y_mean_hi)+1, xmin=0, xmax=length(mod_data$label[mod_data$outlier==0])+1), alpha=0.1, fill="purple")
print(f3)

# Calculate log se
preds$log_se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
# Convert the mean and standard_error to linear space
preds$hospital_linear_se <- sapply(1:nrow(preds), function(i) {
  ratio_i <- preds[i, "Y_mean"]
  ratio_se_i <- preds[i, "log_se"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
})
preds$hospital_ratio <- exp(preds$Y_mean) + 1
# rename
preds$log_hospital_ratio <- preds$Y_mean
preds$log_hospital_se <- preds$log_se

# Keep a record of the values
p <- preds[,c("hospital_ratio","hospital_linear_se","Y_mean","Y_mean_lo","Y_mean_hi")]
p <- unique(p)
p$count_obs <- c(length(cv_hospital$ratio))

output_vals <- rbind.fill(output_vals, p)

# Join back with master data_frame
diarrhea$hospital_ratio <- preds$hospital_ratio
diarrhea$hospital_linear_se <- preds$hospital_linear_se
diarrhea$log_hospital_ratio <- preds$log_hospital_ratio
diarrhea$log_hospital_se <- preds$log_hospital_se

######################################################################
## Scientific literature inpatient crosswalk.
## This is for data that report the incidence of hospitalized diarrhea.
######################################################################
cv_inp_lit <- bundle_crosswalk_collapse(df, covariate_name="cv_inpatient_lit", age_cut=age_bins, year_cut=c(seq(1980,2015,5),2020), merge_type="between", location_match="exact")

cv_inp_lit$year_id <- floor((cv_inp_lit$year_end + cv_inp_lit$year_start)/2)
cv_inp_lit$location_id <- cv_inp_lit$location_match
cv_inp_lit$age_mid <- floor((cv_inp_lit$age_end + cv_inp_lit$age_start)/2)

#cv_inp_lit$delta_log_se <- cv_inp_lit$delta_log_se * 10

#cv_inp_lit <- join(cv_inp_lit, sdi[,c("location_id","year_id","sdi")], by=c("location_id","year_id"))
cv_inp_lit <- join(cv_inp_lit, locs[,c("location_id","super_region_name","location_name","ihme_loc_id")], by="location_id")
cv_inp_lit$high_income <- ifelse(cv_inp_lit$super_region_name=="High-income",1,0)

# Values here should not be greater than 1 (inpatient incidence cannot be greater than population incidence),
# so I am going to shift the values by -1.
# Save a file for investigation
write.csv(cv_inp_lit[cv_inp_lit$ratio<=1,], "FILEPATH", row.names=F)

cv_inp_lit <- subset(cv_inp_lit, ratio > 1)
cv_inp_lit$log_ratio <- cv_inp_lit$log_ratio - 1
write.csv(cv_inp_lit, "FILEPATH", row.names=F)

fit1 <- run_mr_brt(
  output_dir = paste0("FILEPATH"),
  model_label = "cv_inp_lit_2020",
  data = cv_inp_lit,
  mean_var = "log_ratio",
  se_var = "delta_log_se",
  overwrite_previous = TRUE,
  method = "trim_maxL",
  trim_pct = 0.1
)

check_for_outputs(fit1)
df_pred <- data.frame(expand.grid(intercept = 1))
pred1 <- predict_mr_brt(fit1, newdata = df_pred)
check_for_preds(pred1)
pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries
mod_data <- fit1$train_data

# Calculate log se
preds$se <- (preds$Y_mean_hi - preds$Y_mean_lo) / 2 / qnorm(0.975)
# Convert the mean and standard_error to linear space
preds$linear_se <- deltamethod(~exp(x1) + 1, preds$Y_mean, preds$se^2)
preds$ratio <- exp(preds$Y_mean) + 1

# Keep a record of the values
p <- preds[,c("ratio","linear_se","Y_mean","Y_mean_lo","Y_mean_hi")]
p <- unique(p)
p$count_obs <- c(length(cv_inp_lit$ratio))

output_vals <- rbind.fill(output_vals, p)

diarrhea$inpatient_lit_ratio <- preds$ratio
diarrhea$inpatient_lit_linear_se <- preds$linear_se
diarrhea$log_inpatient_lit_ratio <- preds$Y_mean
diarrhea$log_inpatient_lit_se <- preds$se

## Create essentially a forest plot
mod_data$outlier <- abs(mod_data$w - 1)
mod_data$location_id <- mod_data$location_match
mod_data <- join(mod_data, locs[,c("location_id","location_name","ihme_loc_id")], by="location_id")
mod_data$label <- with(mod_data, paste0(location_name,"_",age_start,"-",age_end,"_",year_start,"-",year_end))
f4 <- ggplot(mod_data[mod_data$outlier==0,], aes(ymax=exp(log_ratio + delta_log_se*1.96)+1, ymin=exp(log_ratio - delta_log_se*1.96)+1)) + geom_point(aes(y=exp(log_ratio)+1, x=label)) +
  geom_errorbar(aes(x=label), width=0) +
  theme_bw() + ylab("Linear ratio") + xlab("") + coord_flip() + ggtitle(paste0("Diarrhea inpatient literature ratio (",round(preds$ratio,3),")")) +
  geom_hline(yintercept=1) + geom_hline(yintercept=preds$ratio, col="purple") +
  geom_rect(data=preds, aes(ymin=exp(Y_mean_lo)+1, ymax=exp(Y_mean_hi)+1, xmin=0, xmax=length(mod_data$label[mod_data$outlier==0])+1), alpha=0.1, fill="purple")
print(f4)

################################################################################
## Perform actual crosswalk, save results for upload ##
################################################################################
diarrhea$raw_mean <- diarrhea$mean
diarrhea$raw_standard_error <- diarrhea$standard_error

## calculate the adjusted standard_errors outside of ifelse statement
std_hospital <- sqrt(with(diarrhea, standard_error^2 * hospital_linear_se^2 + standard_error^2*hospital_ratio^2 + hospital_linear_se^2*mean))
std_inp_lit <- sqrt(with(diarrhea, standard_error^2 * inpatient_lit_linear_se^2 + standard_error^2*inpatient_lit_ratio^2 + inpatient_lit_linear_se^2*mean))

#####################################
## Convert the mean by the crosswalk!
# Hospital
diarrhea$cv_hospital[is.na(diarrhea$cv_hospital)] <- 0
diarrhea$mean <- ifelse(diarrhea$cv_hospital==1, diarrhea$mean * diarrhea$hospital_ratio, diarrhea$mean)
diarrhea$standard_error <- ifelse(diarrhea$cv_hospital==1, std_hospital, diarrhea$standard_error)
diarrhea$note_modeler <- ifelse(diarrhea$cv_hospital==1, paste0("This data point was adjusted for being in a clinical sample population. The code to produce
                                                                this estimated ratio is from diarrhea_crosswalk_hospital_mr-brt.R. This occurred in MR-BRT
                                                                during decomposition step 2. The original mean was ", round(diarrhea$raw_mean,3),". ", diarrhea$note_modeler), diarrhea$note_modeler)

# Inpatient from literature
diarrhea$cv_inpatient_lit[is.na(diarrhea$cv_inpatient_lit)] <- 0
diarrhea$mean <- ifelse(diarrhea$cv_inpatient_lit==1, diarrhea$mean * diarrhea$inpatient_lit_ratio, diarrhea$mean)
diarrhea$standard_error <- ifelse(diarrhea$cv_inpatient_lit==1, std_inp_lit, diarrhea$standard_error)
diarrhea$note_modeler <- ifelse(diarrhea$cv_inpatient_lit==1, paste0("This data point was adjusted for being in a hospitalized inpatient sample population from the literature. The code to produce
                                                                     this estimated ratio is from diarrhea_crosswalk_hospital_mr-brt.R. This occurred in MR-BRT
                                                                     during decomposition step 2. The original mean was ", round(diarrhea$raw_mean,3),". ", diarrhea$note_modeler), diarrhea$note_modeler)

diarrhea$crosswalk_type <- ifelse(diarrhea$cv_hospital==1, "Hospitalized", ifelse(diarrhea$cv_inpatient_lit==1,"Inpatient from Literature",
                                                                                  ifelse(diarrhea$cv_marketscan==1,"Claims",ifelse(diarrhea$cv_inpatient==1,"Inpatient","No crosswalk"))))
diarrhea$crosswalk_type[is.na(diarrhea$crosswalk_type)] <- "No crosswalk"

ggplot(diarrhea, aes(x=mean_original, y=mean, col=crosswalk_type)) + geom_point() + scale_y_log10(limits=c(0.000001,1)) + scale_x_log10(limits=c(0.000001,1))

## Save out crosswalk values ##
xwvals <- data.frame(cv_inpatient_lit = unique(diarrhea$inpatient_lit_ratio), cv_inpatient_lit_se = unique(diarrhea$inpatient_lit_linear_se),
                     cv_hospital = unique(diarrhea$hospital_ratio), cv_hospital_se = unique(diarrhea$hospital_linear_se),
                     cv_inpatient = unique(diarrhea$inpatient_ratio), cv_marketscan = unique(diarrhea$marketscan_ratio))
xwvals <- rbind.fill(xwvals, output_vals)
write.csv(xwvals, "FILEPATH", row.names=F)

## Data cleaning ##
# This NID had some duplicate rows group_reviewed, undo that.
diarrhea$group_review <- ifelse(diarrhea$nid==id, "", diarrhea$group_review)
diarrhea <- subset(diarrhea, !is.na(mean))
diarrhea$mean <- as.numeric(diarrhea$mean)
diarrhea$cases <- diarrhea$mean * diarrhea$sample_size

## Both sex data should be duplicated ##
source("FILEPATH")
diarrhea$seq_parent <- ""
diarrhea$crosswalk_parent_seq <- ""

sex_df <- duplicate_sex_rows(diarrhea)

sex_df$mean <- ifelse(is.na(sex_df$mean), sex_df$cases / sex_df$sample_size, sex_df$mean)
sex_df$group_review[is.na(sex_df$group_review)] <- ""
sex_df <- subset(sex_df, group_review != 0)
sex_df[is.na(sex_df)] <- ""
sex_df$standard_error <- as.numeric(sex_df$standard_error)
sex_df$group_review <- ifelse(sex_df$specificity=="","",1)
sex_df$uncertainty_type <- ifelse(sex_df$lower=="","", as.character(sex_df$uncertainty_type))
sex_df$uncertainty_type_value <- ifelse(sex_df$lower=="","",sex_df$uncertainty_type_value)

sex_df_1 <- sex_df
sex_df <- subset(sex_df, !is.na(standard_error))
sex_df <- as.data.frame(sex_df)

sex_df <- sex_df[, -which(names(sex_df) %in% c("cv_dhs","cv_whs","cv_nine_plus_test","cv_explicit_test","duration","duration_lower","duration_upper",
                                               "cv_clin_data","unnamed..75","unnamed..75.1","survey","original_mean","cv_diag_selfreport","is_reference",
                                               "age_mid","ratio","linear_se","std_clinical","cv_inpatient_lit","hospital_ratio","hospital_linear_se",
                                               "log_hospital_ratio","log_hospital_se","inpatient_lit_ratio","cv_miscoded","inpatient_lit_linear_se","log_inpatient_lit_ratio",
                                               "log_inpatient_lit_se", "raw_mean","raw_standard_error","crosswalk_type","parent_id","cv_hospital_child","level","cv_low_income_hosp","incidence_corrected",
                                               "cv_marketscan_inp_2000","cv_marketscan_all_2000"))]

# Assign seq_parent and crosswalk_parent_seq
#sex_df$seq_parent <- ifelse(sex_df$seq_parent=="", sex_df$seq, sex_df$seq_parent)
#sex_df$crosswalk_parent_seq <- ifelse(sex_df$crosswalk_parent_seq=="", sex_df$seq, sex_df$crosswalk_parent_seq)

# Subset if data are greater than 1
sex_df <- subset(sex_df, mean < 1)
# Make sure cases < sample size
sex_df$sample_size <- as.numeric(sex_df$sample_size)
sex_df$cases <- as.numeric(sex_df$cases)
sex_df$cases <- ifelse(sex_df$cases > sex_df$sample_size, sex_df$sample_size * 0.9, sex_df$cases)

## Opportunity to quickly look for outliers ##
source("FILEPATH")
source("FILEPATH")
library(scales)
sex_df <- join(sex_df, locs[,c("location_id","parent_id","level","region_name","super_region_name")], by="location_id")

map_dismod_input(sex_df, type = "mean", subnationals = T, data_range = c(0, 0.006), title = "Unadjusted data")
map_dismod_input(sex_df[sex_df$cv_inpatient==1,], type = "mean", subnationals = T, data_range = c(0, 0.006), title = "Clinical inpatient")
#map_dismod_input(sex_df[sex_df$cv_diag_selfreport==1,], type = "mean", subnationals = T, data_range = c(0, 0.006), title = "Survey data")

input_scatter_sdi(sex_df, error = (-100), title = "All data")
input_scatter_sdi(sex_df[sex_df$cv_inpatient==1,], error = (-0.75), title = "Inpatient data")
#input_scatter_sdi(sex_df[sex_df$cv_diag_selfreport==1,], error = (-0.75), title = "Survey data")

for(s in unique(sex_df$region_name)){
  p <- ggplot(subset(sex_df, level ==3 & region_name==s & is_outlier == 0), aes(x=location_name, y=mean)) + geom_boxplot() + geom_point(alpha=0.4) + scale_y_log10(label=percent) + ggtitle(s) + theme_bw() +
    geom_hline(yintercept = median(sex_df$mean[sex_df$level==3 & sex_df$region_name==s]), col="red") +
    geom_hline(yintercept = quantile(sex_df$mean[sex_df$level==3 & sex_df$region_name==s], 0.25), col="red", lty=2) +
    geom_hline(yintercept = quantile(sex_df$mean[sex_df$level==3 & sex_df$region_name==s], 0.75), col="red", lty=2)
  print(p)
}

for(l in unique(sex_df$parent_id[sex_df$level==4])){
  p <- ggplot(subset(sex_df, level==4 & parent_id==l & is_outlier==0), aes(x=location_name, y=mean)) + geom_boxplot() + geom_point(alpha=0.4) + scale_y_log10(label=percent) + theme_bw() +
    theme(axis.text.x=element_text(angle=90, hjust=1)) + geom_hline(yintercept = median(sex_df$mean[sex_df$level==4 & sex_df$parent_id==l]), col="red") +
    geom_hline(yintercept = quantile(sex_df$mean[sex_df$level==4 & sex_df$parent_id==l], 0.25), col="red", lty=2) +
    geom_hline(yintercept = quantile(sex_df$mean[sex_df$level==4 & sex_df$parent_id==l], 0.75), col="red", lty=2)
  print(p)
}
dev.off()

## Join with SDI for some plotting
sex_df$year_id <- floor((sex_df$year_end + sex_df$year_start)/2)
sex_df <- join(sex_df, sdi[,c("sdi","year_id","location_id")], by=c("location_id","year_id"))

## Mark outliers
# This study has an incompatable case definition
drop_nids <- c(id)
sex_df_1 <- copy(sex_df) ## -----> save copy without outliers


## SET ALL OUTLIERS TO 0
sex_df$is_outlier <- 0

#### THIS IS WHERE THE OUTLIERING PROCESS BEGINS ####
#### DON'T INCLUDE IF YOU WANT TO RUN A MODEL WITHOUT OUTLIERS ###

# Create an outlier table

table(sex_df$is_outlier)
sex_df$is_outlier <- ifelse(sex_df$nid %in% drop_nids, 1, sex_df$is_outlier)

## Keep outliers from best model crosswalk

#source("FILEPATH")
#crosswalk_version_id <- id
#dt_xwalk <- as.data.table(get_crosswalk_version(crosswalk_version_id))
#aggregate(dt_xwalk$is_outlier, by = list(dt_xwalk$source_type), FUN = sum) 
#xwalk_out <- dt_xwalk[is_outlier == 1 & source_type != "Facility - inpatient" & source_type != "Facility - other/unknown", c(nid)]

#sex_df <- as.data.table(sex_df)
#sex_df_2 <- sex_df[nid %in% xwalk_out, is_outlier := 1]
#table(sex_df_2$is_outlier)

outlier_table <- data.frame(drop_nids, data_outliered=c("all"), reason_outliered=c("Case definition"))
write.csv(outlier_table, "FILEPATH", row.names=F)

## Taiwan exception
sex_df$is_outlier <- ifelse(sex_df$location_name=="Taiwan" & sex_df$clinical_data_type=="claims", 1, sex_df$is_outlier)

sex_df$year_end <- ifelse(sex_df$nid==19950, 1999, sex_df$year_end)

sex_df_1 <- copy(sex_df)
sex_df <- subset(sex_df, standard_error < 1)

sex_df$step2_location_year <- "These data may have been changed in the data processing"

sex_df$is_outlier <- ifelse(log(sex_df$mean) < (-10), 1, ifelse(sex_df$mean > 0.10, 1, sex_df$is_outlier))

ggplot(sex_df, aes(x=sdi, y=mean, col=factor(is_outlier))) + geom_point() + scale_y_log10() + scale_color_manual(values=c("black","red")) +
  geom_hline(yintercept=0.05, col="purple")

sex_df_1 <- copy(sex_df)

## Add outliers from current best crosswalk version

source("FILEPATH")
crosswalk_version_id <- ID

df_xwalk <- get_crosswalk_version(crosswalk_version_id)

table(df_xwalk$is_outlier)
table(sex_df$is_outlier)

out_seq <- df_xwalk[is_outlier == 1, seq]

sex_df <- as.data.table(sex_df)

sex_df[seq %in% c(out_seq), is_outlier := 1]

table(sex_df$is_outlier)



# India outlier

table(sex_df$is_outlier)

sex_df <- as.data.table(sex_df)
india_out <- ID
sex_df <- sex_df[nid == ID, is_outlier := 1]
table(sex_df$is_outlier == 0, sex_df$nid %in% india_out)

# Location outliering

# Iceland, Norway and subnationals, Finland

loc_1 <- c(79,83,90,51)
locs <- as.data.table(locs)
norway_sub <- locs[parent_id == 90]
norway_sub <- unique(norway_sub$location_id)

table(sex_df$is_outlier)
sex_df <- sex_df[location_id %in% c(loc_1, norway_sub) & age_end == 0.999 & clinical_data_type == "inpatient" | location_id %in% c(loc_1, norway_sub) & age_end == 4.000  & clinical_data_type == "inpatient"| location_id %in% c(loc_1, norway_sub) & age_end == 9.000  & clinical_data_type == "inpatient", is_outlier := 1]
table(sex_df$is_outlier)

# Germany, Austria, Switzerland, Czech - all ages

View(locs[location_name %in% c("Germany","Austria","Switzerland","India")])
View(locs[location_name %like% "Czech"])
loc_2 <- c(47,75,81,94,163)

View(locs[parent_id == 163])
india_sub <- locs[parent_id == 163]
india_sub <- unique(india_sub$location_id)

sex_df_1 <- sex_df
sex_df <- sex_df[location_id %in% c(loc_2, india_sub) & clinical_data_type == "inpatient", is_outlier := 1]
table(sex_df$is_outlier)

# Slovakia

View(locs[location_name %like% "Slovakia"])
loc_3 <- 54
sex_df <- sex_df[location_id %in% c(loc_3) & clinical_data_type == "inpatient", is_outlier := 1]
table(sex_df$is_outlier)

sex_df$sdi[is.na(sex_df$sdi)] <- 0.5
child_outliers <- sex_df
uq_ref_5 <- quantile(sex_df$mean[sex_df$age_end < 5 & sex_df$cv_inpatient==0 & sex_df$cv_marketscan==0], 0.75, na.rm=T)
med_ref_5 <- quantile(sex_df$mean[sex_df$age_end < 5 & sex_df$cv_inpatient==0 & sex_df$cv_marketscan==0], 0.5, na.rm=T)
u90_ref_5 <- quantile(sex_df$mean[sex_df$age_end < 5 & sex_df$cv_inpatient==0 & sex_df$cv_marketscan==0], 0.9, na.rm=T)

child_outliers$is_outlier <- ifelse(child_outliers$age_end < 5 & child_outliers$mean > uq_ref_5 & child_outliers$sdi > 0.75, 1, child_outliers$is_outlier)
child_outliers$is_outlier <- ifelse(child_outliers$age_start == 5 & child_outliers$age_end == 9 & child_outliers$mean > med_ref_5, 1, child_outliers$is_outlier)
child_outliers$is_outlier <- ifelse(child_outliers$age_end < 1 & child_outliers$mean > u90_ref_5, 1, child_outliers$is_outlier)

ggplot(child_outliers, aes(x=sdi, y=mean, col=factor(is_outlier))) + geom_point() + scale_y_log10() + scale_color_manual(values=c("black","red")) +
  geom_hline(yintercept=uq_ref_5, col="purple", lty=2)
ggplot(child_outliers[child_outliers$location_id==4755,], aes(x=age_start, y=mean, col=factor(is_outlier))) + geom_point() + scale_color_manual(values=c("black","red")) +
  geom_hline(yintercept=uq_ref_5, col="purple", lty=2)

## Verify exclusions have been removed; remove if necessary

child_outliers <- as.data.table(child_outliers)
table(child_outliers$nid %in% ex_all)
child_outliers <- child_outliers[!child_outliers$nid %in% ex_all]
table(child_outliers$nid %in% ex_all)

sex_df <- as.data.table(sex_df)
table(sex_df$nid %in% ex_all)
sex_df <- sex_df[!sex_df$nid %in% ex_all]
table(sex_df$nid %in% ex_all)



#### BEGIN UPLOAD HERE FOR DATA SET WITH NO OUTLIERS ####

## CLEAN

sex_df <- sex_df[is.na(sex_df$representative_name), representative_name := "Unknown"]

##------------------------------------------------------------------##
## SAVE!

write.csv(sex_df, "FILEPATH", row.names=F)

sex_df <- as.data.table(sex_df)
#child_outliers <- as.data.table(child_outliers)

#decomp2 <- sex_df[sex_df$gbd_2020_new != 1] #& nid %!in% step2_nid_new,]
#decomp4 <- child_outliers[child_outliers$gbd_2020_new == 1] #| nid %in% step2_nid_new,]
iterative <- sex_df
#iterative_u5 <- child_outliers

## Version with child outliers; all data

write.xlsx(iterative, "FILEPATH", sheetName="extraction")


##--------------------------------------------------------------------------------
## UPLOAD!

source("FILEPATH")

## UPLOAD version with no crosswalk
bvid <- result_bv$bundle_version   ## -----> this needs to the bundle version that you used to create the crosswalk (check output from save_bundle_version)
data_filepath <- "FILEPATH"
description <- "" ## ------> change description to fit version

save_crosswalk_version(bundle_version_id=iterative_bvid
                       , data_filepath=data_filepath
                       , description=description
)