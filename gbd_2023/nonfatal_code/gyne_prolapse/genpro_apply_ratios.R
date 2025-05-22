#################################################################################################
#' Name: APPLY CLINICAL & NETWORK RATIOS TO GENITAL PROLAPSE
################################################################################################

#SETUP-----------------------------------------------------------------------------------------------
#rm(list=ls())
source("FILEPATH")
pacman::p_load(data.table, openxlsx, ggplot2, dplyr, tidyr, car, LaplacesDemon)
library(haven)
library(mortdb, lib = "FILEPATH")
library(msm, lib.loc = paste0(j_root, "FILEPATH"))

#SOURCE FUNCTIONS----------------------------------------------------------------------------------------------------
source_shared_functions(c("get_location_metadata", "get_bundle_data", "upload_bundle_data",
                          "save_bundle_version", "get_bundle_version", "save_crosswalk_version", "get_crosswalk_version"))

#ARGS & DIRS----------------------------------------------------------------------------------------------------------
temp_dir <- "FILEPATH"
coefs_dir <- "FILEPATH"
cause_group <- "gyne"
cause_name <- "genital_prolapse"

out_dir <- "FILEPATH"
dir.create(out_dir, recursive = T)
decomp <- "iterative"
draws <- paste0("draw_", 0:999)

#GET CORRECT IDS & FILEPATHS---------------------------------------------------------------------------------
bundles <- data.table(read.xlsx(paste0(temp_dir,"FILEPATH"), sheet = "cause"))[group == cause_group]
names(bundles)

bun_id <- bundles[cause == cause_name, bundle_id]
iter_bvid <- bundles[cause == cause_name, iterative_bvid]
print(bun_id)
print(iter_bvid)

summ_clinical <- bundles[cause == cause_name, fpath_clinical]
summ_network <- bundles[cause == cause_name, fpath_network]
print(summ_clinical)
print(summ_network)

#########################################################################################
##'GET BUNDLE VERSION
#bv_data <- get_bundle_version(bundle_version_id = iter_bvid, fetch = "all", export = FALSE)
dim(bv_data)
names(bv_data)
unique(bv_data$sex)

#GET CLINICAL CROSSWALK COEFFICIENTS
coefs_clinical <- data.table(read.csv(summ_clinical))
head(coefs_clinical)
names(coefs_clinical)

beta0 <- coefs_clinical$Y_mean # predicted ratios by age
beta0_se_tau <-  (coefs_clinical$Y_mean_hi - coefs_clinical$Y_mean_lo) / 3.92 # standard
X_age <- coefs_clinical$X_age
test <- as.data.frame(cbind(beta0, beta0_se_tau, X_age))
print(test)

##PREP COVARIATES (clinical/inpatient)
dat_original <- copy(bv_data)
dat_original[clinical_data_type == "inpatient" | clinical_data_type_gbd19 == "inpatient", reference := 0]
dat_original[clinical_data_type == "claims" | clinical_data_type_gbd19 == "claims", reference := 1]
dat_original[, X_age := round((age_start + age_end)/2)]

#vast majority of data is clinical
nrow(dat_original[reference %in% c(0,1)]) + 
  nrow(dat_original[reference %in% c(NA)]) == nrow(dat_original) 

#STANDARDIZE VARIABLE NAMES IN ORIG DT
reference_value <- 1
reference_var <- "reference"
mean_var <- "mean"
se_var <- "standard_error"
cov_names <- "X_age"
orig_vars <- c(mean_var, se_var, reference_var, cov_names)

#GET APPROPRIATE DTs
adjust_dt <- dat_original[reference == 0 & mean != 0]
noadjust_dt <- dat_original[reference %in% c(1, NA) | (reference == 0 & mean == 0)]
nrow(adjust_dt) + nrow(noadjust_dt) == nrow(dat_original)

#LOGIT TRANSFORM ORIGINAL BUNDLE DATA
tmp_orig <- copy(adjust_dt)
dim(tmp_orig)
setnames(tmp_orig, orig_vars, c("mean", "se", "ref", "X_age"))

tmp_orig$mean_logit <- logit(tmp_orig$mean)
tmp_orig$se_logit <- sapply(1:nrow(tmp_orig), function(i) {
  mean_i <- tmp_orig[i, "mean"]
  se_i <- tmp_orig[i, "se"]
  deltamethod(~log(x1), mean_i, se_i^2)
})

#MERGE ORIG DAT & CLINICAL COEFS
tmp_logit <- merge(tmp_orig, test, by = "X_age", all.x = TRUE)
dim(tmp_logit)

#APPLY LOGIT ADJUSTMENT
#Estimate new variance in logit space, new variance of adjusted data point is the sum of variances
#the adjustment is a sum rather than a product in log space
#adjust the mean estimate: log(mean_original) - (log(alt) - log(ref))
tmp_adj <- copy(tmp_logit)
tmp_adj[ ,`:=` (mean_logit_tmp = mean_logit - beta0, var_logit_tmp = se_logit^2 + beta0_se_tau^2)]
tmp_adj[, se_logit_tmp := sqrt(var_logit_tmp)]
names(tmp_adj)

#SELECT DESIRED MEAN VALUES
tmp_adj2 <- tmp_adj[ ,`:=` (mean_logit_adjusted = if_else(ref == 1, mean_logit, mean_logit_tmp),
                            se_logit_adjusted = if_else(ref == 1, se_logit, se_logit_tmp))]

tmp_adj2[ ,`:=` (lo_logit_adjusted = mean_logit_adjusted - 1.96 * se_logit_adjusted,
                 hi_logit_adjusted = mean_logit_adjusted + 1.96 * se_logit_adjusted,
                 mean_adjusted = invlogit(mean_logit_adjusted))]

tmp_adj2[ , `:=` (lo_adjusted = invlogit(lo_logit_adjusted),
                  hi_adjusted = invlogit(hi_logit_adjusted))]

#ESTIMATE ADJUSTED SE USING DELTA METHOD
tmp_adj2$se_adjusted <- sapply(1:nrow(tmp_adj2), function(i) {
  ratio_i <- tmp_adj2[i, "mean_logit_adjusted"]
  ratio_se_i <- tmp_adj2[i, "se_logit_adjusted"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
})
names(tmp_adj2)
dim(tmp_adj2)

#CHECK FOR NaN SE
check <- tmp_adj2[ ,c("X_age", "ref",  "mean", "mean_adjusted", "se", "se_adjusted", "lo_adjusted", "hi_adjusted", "mean_logit", "se_logit", "mean_logit_adjusted" , "se_logit_adjusted")]
check[is.na(se_adjusted) | se_adjusted == "Inf"]

#FINAL DATA (original extracted data w/ the new variables)
final_data <- copy(tmp_adj2)
dim(final_data)
setnames(final_data, "se", "standard_error")
names(final_data)
nrow(final_data[is.na(mean_adjusted)])

#CHECK CORRECTIONS
check <- final_data[ ,c("X_age", "ref",  "mean", "mean_adjusted", "standard_error", "se_adjusted", "lo_adjusted", "hi_adjusted", "mean_logit", "se_logit", "mean_logit_adjusted" , "se_logit_adjusted")]
check[is.na(se_adjusted) | se_adjusted == "Inf"]
check[ref == 0 & mean == 0]

##DELETE DUPLICATIVE MEAN & SE COLUMNS FOR MODELING
nrow(final_data) == nrow(final_data[mean != 0])
final_data[mean != 0 ,`:=` (mean = mean_adjusted, lower = lo_adjusted, upper = hi_adjusted, standard_error = se_adjusted)]
final_data[ref == 0, crosswalk_parent_seq := seq]
final_data[ref == 0, seq := NA]

if ("note_modeler" %in% names(final_data)){
  final_data[ref == 0,note_modeler := paste0(note_modeler,"crosswalked inpatient to claims data")]
} else{
  final_data[ref == 0,note_modeler := "crosswalked inpatient to claims data"]
}

final_data[ ,c("X_age", "mean_logit","se_logit","beta0", "beta0_se_tau", "mean_logit_tmp", "var_logit_tmp", "se_logit_tmp",
               "mean_logit_adjusted", "se_logit_adjusted", "lo_logit_adjusted", "hi_logit_adjusted" ,
               "mean_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted") := NULL]

dim(final_data)
names(final_data)
unique(final_data$seq)
setnames(final_data, "ref", "reference")

#INFLATE SE ONLY OF MEAN = 0
inflate_dt <- copy(noadjust_dt)
inflate_ages <- (unique(inflate_dt$X_age))
inflate_coefs <- coefs_clinical[X_age %in% inflate_ages]

inflate_coefs[ ,`:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]
inflate_coefs[, `:=` (Y_se_norm = (deltamethod(~exp(x1)/(1+exp(x1)), Y_mean, Y_se^2))), by=1:nrow(inflate_coefs)]
names(inflate_coefs)

inflate_cols <- inflate_coefs[ ,c("X_age", "Y_se_norm")]

inflate_merge <- merge(inflate_dt, inflate_cols, by = "X_age", all.x = TRUE)
dim(noadjust_dt)
dim(inflate_merge)

inflate_merge[reference == 0 & mean == 0, se_adjusted := sqrt(standard_error^2 + Y_se_norm^2)]
nrow(inflate_merge[reference == 0 & mean == 0])
summary(inflate_merge[reference == 0 & mean == 0, mean])
summary(inflate_merge[reference == 0 & mean == 0, standard_error])
summary(inflate_merge[reference == 0 & mean == 0, se_adjusted])

inflate_merge[reference == 0 & mean == 0, `:=` (standard_error = se_adjusted, crosswalk_parent_seq = seq)]
inflate_merge[reference == 0 & mean == 0, `:=` (lower = NA, upper = NA, seq = NA)]

if ("note_modeler" %in% names(inflate_merge)){
  inflate_merge[reference == 0 & mean == 0,note_modeler := paste0(note_modeler,"original mean = 0, adjusted the SE only")]
} else{
  inflate_merge[reference == 0 & mean == 0,note_modeler := "original mean = 0, adjusted the SE only"]
}

#RECOMBINE THE DTS
dim(final_data)
dim(inflate_merge)
setdiff(names(inflate_merge), names(final_data))
inflate_merge[ ,c("X_age", "Y_se_norm", "se_adjusted") := NULL]

adjusted_clinical <- rbind(final_data, inflate_merge)
nrow(adjusted_clinical) == nrow(dat_original)
nrow(adjusted_clinical[reference == 0]) == nrow(adjusted_clinical[!is.na(crosswalk_parent_seq)])

#WRITE TO FILE
adjusted_clinical_fpath <- paste0(out_dir, "FILEPATH")
print(adjusted_clinical_fpath)
writexl::write_xlsx(adjusted_clinical, path = adjusted_clinical_fpath, col_names = TRUE, format_headers = FALSE)

##################################################
#NOW DO THE NETWORK ANALYSIS######################
##################################################

#GET NETWORK CROSSWALK COEFFICIENTS
coefs_network <- data.table(read.csv(summ_network))
coefs_network <- unique(coefs_network)
head(coefs_network)
names(coefs_network)

cov_names <- c("X_cv_clinical") #alts: clinical, self-report, symptomatic
beta0 <- coefs_network$Y_mean # predicted ratios by age
beta0_se_tau <-  (coefs_network$Y_mean_hi - coefs_network$Y_mean_lo) / 3.92 # standard
test <- as.data.frame(cbind(coefs_network[ ,..cov_names],beta0, beta0_se_tau))
test <- data.table(test)
test <- test[X_cv_clinical == 1]
test

#ADD COVARIATES
network_dt <- copy(adjusted_clinical)
network_dt[clinical_data_type %in% c("inpatient", "claims") | clinical_data_type_gbd19 %in% c("inpatient", "claims") ,`:=` (X_cv_clinical = 1)]
nrow(network_dt[X_cv_clinical == 1])
network_dt[is.na(X_cv_clinical) ,`:=` (X_cv_clinical = 0 )]

#GET APPROPRIATE DTs
adjust_dt <- network_dt[(X_cv_clinical == 1  & mean != 0)] #the mean 0 inpatient data won't be adjusted, & SE has already been inflated
noadjust_dt <- network_dt[X_cv_clinical %in% c(NA, 0) | (X_cv_clinical ==1 & mean == 0) ] #inflate the SE of mean zero claims data at the bottom
nrow(adjust_dt) + nrow(noadjust_dt) == nrow(network_dt)

# -- identify mean and standard error variables and covariates
reference_var <- "reference"
reference_value <- 1
mean_var <- "mean"
se_var <- "standard_error"
cov_names <- c("X_cv_clinical")

#LOGIT TRANSFORM MEAN VALUES
adjusted <- copy(adjust_dt)
dim(adjusted)

adjusted$mean_logit <- logit(adjusted$mean)
adjusted$se_logit <- sapply(1:nrow(adjusted), function(i) {
  mean_i <- adjusted[i, "mean"]
  se_i <- adjusted[i, "standard_error"]
  deltamethod(~log(x1), mean_i, se_i^2)
})

#MERGE ORIG DAT & CLINICAL COEFS
print(test)
print(cov_names)
adjusted <- merge(adjusted, test, by = cov_names, all.x = TRUE)
names(adjusted)

#APPLY LOGIT ADJUSTMENT
adjusted[ ,`:=` (mean_logit_adjusted = mean_logit - beta0, var_logit_tmp = se_logit^2 + beta0_se_tau^2)]
adjusted[, se_logit_adjusted := sqrt(var_logit_tmp)]
names(adjusted)

#SELECT DESIRED MEAN VALUES
adjusted[ ,`:=` (lo_logit_adjusted = mean_logit_adjusted - 1.96 * se_logit_adjusted,
                 hi_logit_adjusted = mean_logit_adjusted + 1.96 * se_logit_adjusted,
                 mean_adjusted = invlogit(mean_logit_adjusted))]

adjusted[ , `:=` (lo_adjusted = invlogit(lo_logit_adjusted),
                  hi_adjusted = invlogit(hi_logit_adjusted))]

#ESTIMATE ADJUSTED SE USING DELTA METHOD
adjusted$se_adjusted <- sapply(1:nrow(adjusted), function(i) {
  ratio_i <- adjusted[i, "mean_logit_adjusted"]
  ratio_se_i <- adjusted[i, "se_logit_adjusted"]
  deltamethod(~exp(x1), ratio_i, ratio_se_i^2)
})

#CHECK FOR NAN SEs
adjusted[is.na(se_adjusted) | se_adjusted == "Inf"]
adjusted[mean == 0]

##DELETE DUPLICATIVE MEAN & SE COLUMNS FOR MODELING
final_adjusted <- copy(adjusted)
final_adjusted[ ,`:=` (mean = mean_adjusted, lower = lo_adjusted, upper = hi_adjusted, standard_error = se_adjusted)]

nrow(final_adjusted[clinical_data_type == "claims" | clinical_data_type_gbd19 == "claims"]) == nrow(final_adjusted[!is.na(seq)])
final_adjusted[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
final_adjusted[ ,seq := NA]

final_adjusted[ ,c("mean_logit","se_logit","beta0", "beta0_se_tau", "var_logit_tmp",
                   "mean_logit_adjusted", "se_logit_adjusted", "lo_logit_adjusted", "hi_logit_adjusted" ,
                   "mean_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted") := NULL]
#ADD A USEFUL NOTE
final_adjusted[ ,note_modeler := paste0(note_modeler," | crosswalked all clinical to medical exam dx reference")]

dim(final_adjusted)
names(final_adjusted)
unique(final_adjusted$seq)

#INFLATE SE ONLY OF MEAN = 0
inflate_dt <- copy(noadjust_dt)
inflate_coefs <- (coefs_network[X_cv_clinical == 1])
inflate_coefs[ ,c("X_cv_self_report", "X_cv_symptomatic") := NULL]

inflate_coefs[ ,`:=` (Y_se = (Y_mean_hi - Y_mean_lo)/(2*qnorm(0.975,0,1)))]
inflate_coefs[, `:=` (Y_se_norm = (deltamethod(~exp(x1)/(1+exp(x1)), Y_mean, Y_se^2))), by=1:nrow(inflate_coefs)]
inflate_coefs
names(inflate_coefs)

inflate_cols <- inflate_coefs[ ,c("X_cv_clinical","Y_se_norm")]

inflate_merge <- merge(inflate_dt, inflate_cols, by = "X_cv_clinical", all.x = TRUE)
dim(noadjust_dt)
dim(inflate_merge)

nrow(inflate_merge[X_cv_clinical == 1 & (clinical_data_type == "claims" | clinical_data_type_gbd19 == "claims") & mean == 0])
inflate_merge[X_cv_clinical == 1 & (clinical_data_type == "claims" | clinical_data_type_gbd19 == "claims") & mean == 0, se_adjusted := sqrt(standard_error^2 + Y_se_norm^2)]

inflate_merge[X_cv_clinical == 1 & (clinical_data_type == "claims" | clinical_data_type_gbd19 == "claims") & mean == 0, `:=` (standard_error = se_adjusted)]
inflate_merge[X_cv_clinical == 1 & (clinical_data_type == "claims" | clinical_data_type_gbd19 == "claims") & mean == 0 & is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
inflate_merge[X_cv_clinical == 1 & (clinical_data_type == "claims" | clinical_data_type_gbd19 == "claims") & mean == 0, `:=` (lower = NA, upper = NA, seq = NA)]

if ("note_modeler" %in% names(inflate_merge)){
  inflate_merge[X_cv_clinical == 1 & (clinical_data_type == "claims" | clinical_data_type_gbd19 == "claims") & mean == 0, note_modeler := paste0(note_modeler,"original mean = 0, adjusted the SE only")]
} else{
  inflate_merge[X_cv_clinical == 1 & (clinical_data_type == "claims" | clinical_data_type_gbd19 == "claims") & mean == 0, note_modeler := "original mean = 0, adjusted the SE only"]
}

#RECOMBINE THE DTS
dim(final_adjusted)
dim(inflate_merge)
setdiff(names(inflate_merge), names(final_adjusted))
setdiff(names(final_adjusted), names(inflate_merge))

inflate_merge[ ,c("Y_se_norm", "se_adjusted") := NULL]

adjusted_network <- rbind(final_adjusted, inflate_merge)
nrow(adjusted_network) == nrow(dat_original)
nrow(adjusted_network[X_cv_clinical == 1]) == nrow(adjusted_network[!is.na(crosswalk_parent_seq)])

adjusted_network


