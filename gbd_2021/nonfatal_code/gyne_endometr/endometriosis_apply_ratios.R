#################################################################################################
#' Name: ADJUST ENDOMETRIOSIS DATA
################################################################################################

#SETUP-----------------------------------------------------------------------------------------------
#rm(list=ls())
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}
user <- Sys.info()["user"]
source("FILEPATH") 


pacman::p_load(data.table, openxlsx, ggplot2, dplyr, tidyr, car, LaplacesDemon)
library(haven)
library(mortdb, lib = "FILEPATH")
library(msm, lib.loc = paste0(j_root, "FILEPATH"))


#SOURCE FUNCTIONS----------------------------------------------------------------------------------------------------
functions_dir <- "FILEPATH"
functs <- c("get_location_metadata", "get_bundle_data", "upload_bundle_data",
            "save_bundle_version", "get_bundle_version", "save_crosswalk_version", "get_crosswalk_version")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R")))) 


repo_dir <- "FILEPATH"
mrbrt_functs <- c("run_mr_brt_function.R", "cov_info_function.R", "check_for_outputs_function.R",
                  "load_mr_brt_outputs_function.R", "predict_mr_brt_function.R", "check_for_preds_function.R",
                  "load_mr_brt_preds_function.R", "plot_mr_brt_function.R")
invisible(lapply(mrbrt_functs, function(x) source(paste0(repo_dir, x))))

#ARGS & DIRS----------------------------------------------------------------------------------------------------------
source("FILEPATH")
temp_dir <- "FILEPATH"
coefs_dir <- "FILEPATH"

out_dir <- paste0(temp_dir, "FILEPATH")
dir.create(out_dir, recursive = T)
decomp <- "iterative"
draws <- paste0("draw_", 0:999)

#GET CORRECT IDS & FILEPATHS---------------------------------------------------------------------------------
bundles <- data.table(read.xlsx(paste0(temp_dir,"FILEPATH"), sheet = "cause"))
bundles
names(bundles)

cause_name <- "endometriosis"
bun_id <- bundles[cause == cause_name, bundle_id]
iter_bvid <- bundles[cause == cause_name, iterative_bvid]
summ_clinical <- bundles[cause == cause_name, fpath_clinical]
summ_network <- bundles[cause == cause_name, fpath_network]

#########################################################################################
##'GET BUNDLE VERSION
#bv_data <- get_bundle_version(bundle_version_id = iter_bvid, fetch = "all", export = FALSE)
dim(bv_data)
names(bv_data)
unique(bv_data$sex)

##PREP COVARIATES (clinical/inpatient)
dat_original <- copy(bv_data)
dat_original[clinical_data_type == "inpatient" | clinical_data_type_gbd19 == "inpatient", reference := 0]
dat_original[clinical_data_type == "claims" | clinical_data_type_gbd19 == "claims", reference := 1]
dat_original[cv_literature == 1, reference := NA]
dat_original[, X_age := round((age_start + age_end)/2)]

#STANDARDIZE VARIABLE NAMES IN ORIG DT
reference_value <- 1
reference_var <- "reference"
mean_var <- "mean"
se_var <- "standard_error"
cov_names <- "X_age"
orig_vars <- c(mean_var, se_var, reference_var, cov_names)

#LOGIT TRANSFORM ORIGINAL BUNDLE DATA
tmp_orig <- copy(dat_original)
dim(tmp_orig)
setnames(tmp_orig, orig_vars, c("mean", "se", "ref", "X_age"))

tmp_orig$mean_logit <- logit(tmp_orig$mean)
tmp_orig$se_logit <- sapply(1:nrow(tmp_orig), function(i) {
  mean_i <- tmp_orig[i, "mean"]
  se_i <- tmp_orig[i, "se"]
  deltamethod(~log(x1), mean_i, se_i^2)
})

#GET CLINICAL CROSSWALK COEFFICIENTS
coefs_clinical <- data.table(read.csv(summ_clinical))
head(coefs_clinical)

beta0 <- coefs_clinical$Y_mean # predicted ratios by age
beta0_se_tau <-  (coefs_clinical$Y_mean_hi - coefs_clinical$Y_mean_lo) / 3.92 # standard
X_age <- coefs_clinical$X_age
test <- as.data.frame(cbind(beta0, beta0_se_tau, X_age))

#MERGE ORIG DAT & CLINICAL COEFS
tmp_logit <- merge(tmp_orig, test, by = "X_age", all.x = TRUE)
head(tmp_logit)
tail(tmp_logit)

#APPLY LOGIT ADJUSTMENT
tmp_adj <- copy(tmp_logit)
tmp_adj[ ,`:=` (mean_logit_tmp = mean_logit - beta0, var_logit_tmp = se_logit^2 + beta0_se_tau^2)]
tmp_adj[, se_logit_tmp := sqrt(var_logit_tmp)]

head(tmp_adj)
tail(tmp_adj)

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
head(tmp_adj2)
tail(tmp_adj2)

#CHECK FOR NaN SE
check <- tmp_adj2[ ,c("X_age", "ref",  "mean", "mean_adjusted", "se", "se_adjusted", "lo_adjusted", "hi_adjusted", "mean_logit", "se_logit", "mean_logit_adjusted" , "se_logit_adjusted")]
head(check)

check[is.na(se_adjusted) | se_adjusted == "Inf"]

# CORRECT NaN SE
tmp_adj3 <- copy(tmp_adj2)
tmp_adj3[ ref == 0 & mean == 0, se_adjusted := se]
tmp_adj3[ ref == 0 & mean == 0 & lo_adjusted == 0, lo_adjusted := (mean_adjusted - 1.96 * se)]
tmp_adj3[ ref == 0 & mean == 0 & hi_adjusted == 0, hi_adjusted := (mean_adjusted + 1.96 * se)]

#CORRECT INFINITE ADJUSTED SE
tmp_adj3[ ref == 0 & se_adjusted == "Inf", se_adjusted := se]
head(tmp_adj3)

#CHECK CORRECTIONS
check <- tmp_adj3[ ,c("X_age", "ref",  "mean", "mean_adjusted", "se", "se_adjusted", "lo_adjusted", "hi_adjusted", "mean_logit", "se_logit", "mean_logit_adjusted" , "se_logit_adjusted")]
check[is.na(se_adjusted) | se_adjusted == "Inf"]
check[ ref == 0 & mean == 0]

#FINAL DATA (original extracted data w/ the new variables)
final_data <- copy(tmp_adj3)
dim(final_data)
setnames(final_data, "se", "standard_error")

#LEAVE LIT & CLAIMS DATA AS IS
final_data[(ref == 1 | is.na(ref)) ,`:=` (mean_adjusted = mean, se_adjusted = standard_error, lo_adjusted = lower, hi_adjusted = upper)]

#LEAVE AGES>95 AS IS B/C MEAN_ADJUSTED==NA
final_data[age_start >= 80,`:=` (mean_adjusted = mean, se_adjusted = standard_error, lo_adjusted = lower, hi_adjusted = upper)]

#LEAVE AS IS IF MEAN == 0
final_data[ref == 0 & mean == 0, `:=` (se_adjusted = standard_error, hi_adjusted = upper, lo_adjusted = lower)]

#CHECK CORRECTIONS
check <- final_data[ ,c("X_age", "ref",  "mean", "mean_adjusted", "standard_error", "se_adjusted", "lo_adjusted", "hi_adjusted", "mean_logit", "se_logit", "mean_logit_adjusted" , "se_logit_adjusted")]
check[is.na(se_adjusted) | se_adjusted == "Inf"]
check[ ref == 0 & mean == 0]


#UPDATE OUTLIERING STATUS
final_data[year_start == 2000 & ref == 1, is_outlier := 1] 
final_data[mean == 0 & ref == 0, is_outlier := 1] 
final_data[age_start >= 80, is_outlier := 1] 


##DELETE DUPLICATIVE MEAN & SE COLUMNS FOR MODELING
for_modeling <- copy(final_data)
for_modeling[ ,`:=` (mean = mean_adjusted, lower = lo_adjusted, upper = hi_adjusted, standard_error = se_adjusted)]
for_modeling[ref == 0, crosswalk_parent_seq := seq]
for_modeling[ref == 0, seq := NA]

if ("note_modeler" %in% names(for_modeling)){
  for_modeling[ref == 0,note_modeler := paste0(note_modeler,"crosswalked inpatient to claims data, logit MR-BRT")]
} else{
  for_modeling[ref == 0,note_modeler := "crosswalked inpatient to claims data, logit MR-BRT"]
}

for_modeling[ ,c("X_age", "mean_logit","se_logit","beta0", "beta0_se_tau", "mean_logit_tmp", "var_logit_tmp", "se_logit_tmp",
                 "mean_logit_adjusted", "se_logit_adjusted", "lo_logit_adjusted", "hi_logit_adjusted" ,
                 "mean_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted") := NULL]

dim(for_modeling)
names(for_modeling)

#WRITE TO FILE
write.xlsx(for_modeling,
           file = paste0(out_dir, "FILEPATH"),
           row.names = FALSE, sheetName = "extraction")

##################################################
#NOW DO THE NETWORK ANALYSIS######################
##################################################

#ADD COVARIATES
network_dt <- copy(for_modeling)
network_dt[clinical_data_type %in% c("inpatient", "claims") | clinical_data_type_gbd19 %in% c("inpatient", "claims") ,`:=` (X_cv_clinical = 1, X_cv_self_report = 0)]
nrow(network_dt[X_cv_clinical == 1])

network_dt[is.na(ref) & nid %in%self_report_nids ,`:=` (X_cv_self_report = 1, X_cv_clinical = 0 )]
nrow(network_dt[X_cv_self_report == 1])

#GET APPROPRIATE DTs
adjust_dt <- network_dt[(X_cv_clinical == 1  & mean != 0)| (X_cv_self_report == 1 & mean != 0)]
noadjust_dt <- network_dt[X_cv_clinical %in% c(NA) | X_cv_self_report %in% c(NA) | mean == 0 ]

# -- identify mean and standard error variables and covariates
reference_var <- "reference"
reference_value <- 1
mean_var <- "mean"
se_var <- "standard_error"
cov_names <- c("X_cv_clinical", "X_cv_self_report")

#LOGIT TRANSFORM MEAN VALUES
adjusted <- copy(adjust_dt)
dim(adjusted)

adjusted$mean_logit <- logit(adjusted$mean)
adjusted$se_logit <- sapply(1:nrow(adjusted), function(i) {
  mean_i <- adjusted[i, "mean"]
  se_i <- adjusted[i, "standard_error"]
  deltamethod(~log(x1), mean_i, se_i^2)
})

#GET NETWORK CROSSWALK COEFFICIENTS
coefs_network <- data.table(read.csv(summ_network))
coefs_network <- unique(coefs_network)
coefs_network <- coefs_network[X_cv_clinical != X_cv_self_report]

beta0 <- coefs_network$Y_mean # predicted ratios by age
beta0_se_tau <-  (coefs_network$Y_mean_hi - coefs_network$Y_mean_lo) / 3.92 # standard
test <- as.data.frame(cbind(coefs_network[ ,..cov_names],beta0, beta0_se_tau))
test

#MERGE ORIG DAT & CLINICAL COEFS
adjusted <- merge(adjusted, test, by = cov_names, all.x = TRUE)
head(adjusted)

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
head(adjusted)

#CHECK FOR NAN SEs
adjusted[is.na(se_adjusted) | se_adjusted == "Inf"]
adjusted[mean == 0]

##DELETE DUPLICATIVE MEAN & SE COLUMNS FOR MODELING
final_adjusted <- copy(adjusted)

final_adjusted[ ,`:=` (mean = mean_adjusted, lower = lo_adjusted, upper = hi_adjusted, standard_error = se_adjusted)]
final_adjusted[is.na(crosswalk_parent_seq), crosswalk_parent_seq := seq]
final_adjusted[ ,seq := NA]

final_adjusted[ ,c("mean_logit","se_logit","beta0", "beta0_se_tau", "var_logit_tmp",
                   "mean_logit_adjusted", "se_logit_adjusted", "lo_logit_adjusted", "hi_logit_adjusted" ,
                   "mean_adjusted", "se_adjusted", "lo_adjusted", "hi_adjusted") := NULL]
#ADD A USEFUL NOTE
final_adjusted[ ,note_modeler := paste0(note_modeler," | crosswalked clinical to acog, or self_report to acog")]

#RECOMBINE THE DTS
dim(final_adjusted)
dim(noadjust_dt)

full_dt <- rbind(final_adjusted, noadjust_dt)

write.xlsx(full_dt,
          file = paste0(out_dir, "FILEPATH"),
          sheetName = "extraction")




