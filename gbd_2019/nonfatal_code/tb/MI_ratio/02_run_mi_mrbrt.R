## EMPTY THE ENVIRONMENT
rm(list = ls())

## SET UP FOCAL DRIVES
os <- .Platform$OS.type
if (os=="windows") {
  ADDRESS <-"ADDRESS"
  ADDRESS <-"ADDRESS"
  ADDRESS <-"ADDRESS"
} else {
  ADDRESS <-"ADDRESS"
  ADDRESS <-paste0("ADDRESS/", Sys.info()[7], "/")
  ADDRESS <-"ADDRESS"
}

## LOAD FUNCTIONS AND PACKAGES
source(paste0(ADDRESS, "FILEPATH/get_covariate_estimates.R"))
source(paste0(ADDRESS, "FILEPATH/get_location_metadata.R"))
source(paste0(ADDRESS, "FILEPATH/get_age_metadata.R"))
library(readstata13)
library(ggplot2)
library(gtools)
library(msm)

## SOURCE MR-BRT
repo_dir <- paste0(ADDRESS, "FILEPATH")
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
source(paste0(repo_dir, "plot_mr_brt_function.R"))

#############################################################################################
###                               HELPER OBJECTS AND FUNCTIONS                            ###
#############################################################################################

## HELPER OBJECTS
mvid         <- "v.1"
decomp_step  <- "step4"
max_98       <- 1.267439007759094
data_dir     <- paste0(ADDRESS, "FILEPATH")
save_dir     <- paste0(ADDRESS, "FILEPATH")
brt_out_dir  <- paste0(ADDRESS, "FILEPATH")

## CREATE DIRECTORIES
ifelse(!dir.exists(brt_out_dir), dir.create(brt_out_dir), FALSE)

#############################################################################################
###                          DERIVE HAQ BANGELORE VALUE FOR 1960                          ###
#############################################################################################

## GET HAQ
haq <- get_covariate_estimates(covariate_id = 1099, decomp_step = decomp_step)
haq <- haq[, .(location_id, year_id, age_group_id, sex_id, mean_value)]

## GET SDI
sdi <- get_covariate_estimates(covariate_id = 881, decomp_step = decomp_step)
sdi <- sdi[, .(location_id, year_id, age_group_id, sex_id, mean_value)]

## PULL GBD LOCATION METADATA
locs <- get_location_metadata(location_set_id = 35)

## GET RANDOM EFFECTS
loc_lvl <- locs[, .(location_id, parent_id, level, path_to_top_parent)]
loc_lvl[, paste0("level_", seq(0, max(loc_lvl$level))) := tstrsplit(path_to_top_parent, ",", fixed=TRUE)]
loc_lvl[, path_to_top_parent := NULL]

## CLEAN
loc_lvl <- loc_lvl[, lapply(.SD, as.integer)]
loc_lvl <- loc_lvl[level > 2]
locs    <- locs[, .(ihme_loc_id, location_name, location_id)]

## PREP FOR MERGE
setnames(haq, "mean_value", "haqi")
setnames(sdi, "mean_value", "sdi")

## MERGE DATA
data <- merge(sdi, haq, all.x = T)
data <- merge(data, locs, by = "location_id")
data <- merge(data, loc_lvl, by="location_id")

## ASSESS ASSOCIATION
mod_1 <- lmer(formula = haqi ~ sdi + (1|level_1/level_2/level_3), data = data)
summary(mod_1)

## PREDICT
data[, pred_re := predict(mod_1, data)]
data[location_name %like% "Karn" & year_id == 1960]

## PREDICT WITHOUT RANDOM EFFECTS
mod_2 <- lm(formula = haqi~sdi, data=data)
data[, pred_no_re := predict(mod_2, data)]
data[location_name %like% "Karn" & year_id == 1960]

#############################################################################################
###                                PREP DATA FOR MR-BRT                                   ###
#############################################################################################

## GET DATA
dt <- as.data.table(read.dta13(paste0(data_dir, "MI_input_", mvid, ".dta")))

## GET LOCATION METADATA
locs <- get_location_metadata(location_set_id = 35)
locs <- locs[, .(location_id, super_region_name)]
dt   <- merge(dt, locs)

## CLEAN DATA SET FOR INPUT
input <- copy(dt)
input <- input[, .(location_id, location_name, iso3, year_id, female, inc_cases, CFR, logit_prop, haq,
                   age0to4, age5to14, age15to24, age25to34, age35to44, age45to54, age55to64, age65plus,
                   central_eur, high_income, latin_am, north_af, africa, asia)]

## FIX BANGALORE STUDY
input[year_id==1960, inc_cases := 95]
input <- input[complete.cases(input)]

## CREATE UNCERTAINTY
input[, cfr_new := CFR]
input[cfr_new > max_98, cfr_new := max_98]
input[, cfr_new := cfr_new / max_98]
input[cfr_new == 1, cfr_new := 0.99]
input[, se := sqrt((cfr_new*(1-cfr_new))/inc_cases)]

## LOGIT TRANSFORM SE
input$se_logit <- sapply(1:nrow(input), function(i) {
  cfr_i    <- input[i, cfr_new]
  cfr_se_i <- input[i, se]
  deltamethod(~log(x1/(1-x1)), cfr_i, cfr_se_i^2)
})

#############################################################################################
###                                    FIT THE MODEL                                      ###
#############################################################################################

## CREATE COV MATRIX
covariates <- c("age0to4", "age5to14", "age15to24", "age35to44", "age45to54", "age55to64",
                "age65plus", "female", "haq", "central_eur", "high_income", "latin_am")
covs1      <- list()
for (nm in covariates) covs1 <- append(covs1, list(cov_info(nm, "X")))

## FIT THE MODEL
fit1 <- run_mr_brt(
  output_dir  = brt_out_dir,
  model_label = "sr_dummy",
  data        = input,
  covs        = covs1,
  mean_var    = "logit_prop",
  se_var      = "se_logit",
  method      = "trim_maxL",
  trim_pct    = 0.01,
  overwrite_previous = TRUE
)

## CHECK FOR OUTPUTS
check_for_outputs(fit1)

## COMPUTE PLOT
plot_mr_brt(fit1, continuous_vars = "haq", dose_vars = "haq")

#############################################################################################
###                                    GET PREDICTIONS                                    ###
#############################################################################################

## PREDICT
pred1 <- predict_mr_brt(fit1, newdata = dt)

## CHECK FOR PREDICTIONS
check_for_preds(pred1)
pred_object <- load_mr_brt_preds(pred1)

## GET PREDICTIONS
preds <- as.data.table(pred_object$model_summaries)

## EXTRACTION MEAN AND SE
preds[, mean_prop := inv.logit(Y_mean)*max_98]
preds[, se := (inv.logit(Y_mean_hi) - inv.logit(Y_mean_lo))/3.92]

## COMPUTE UI
preds[, lower_prop := mean_prop - 1.96*se]
preds[, upper_prop := mean_prop + 1.96*se]
preds[lower_prop < 0, lower_prop := 0][upper_prop > 1, upper_prop := 1]

## MERGE PREDICTIONS
summaries <- preds[, .(mean_prop, lower_prop, upper_prop, se)]
dt <- cbind(dt, summaries)

## SAVE
write.csv(dt, paste0(save_dir, "cfr_pred_mrbrt_age_dummy_haq_sr_", mvid, ".csv"), row.names=F)

#############################################################################################
###                                 PLOT RESULTS BY SDI                                   ###
#############################################################################################

## GET METADATA
ages <- get_age_metadata(age_group_set_id = 12, gbd_round_id=6)
ages <- ages[, .(age_group_years_start, age_group_years_end, age_group_weight_value)]
setnames(ages, old = names(ages), new = c("age_start", "age_end", "age_weight"))

## PREP FOR AGGREGATION
for(my_age in seq(5, 55, 10)) ages[age_start %in% c(my_age, my_age + 5), age := my_age]
ages[age_start < 5, age := 0]
ages[age_start > 60,age := 65]

## AGGREGATE
ages <- ages[, .(age_weight = sum(age_weight)), by = "age"]

## MERGE
dt <- fread(paste0(save_dir, "cfr_pred_mrbrt_age_dummy_haq_sr_", mvid, ".csv"))
dt <- merge(dt, ages, by = "age")

## AGE STANDARDIZE
dt[, age_std_cfr := mean_prop*age_weight]
dt <- dt[, .(age_std_cfr = sum(age_std_cfr)), by = .(iso3, location_id, year_id, sex_id)]
dt <- dt[order(location_id, year_id, sex_id)]
setnames(dt, old = "iso3", new = "ihme_loc_id")

## GET LOCATION DATA
locs <- get_location_metadata(location_set_id = 35, gbd_round_id=6)
locs <- locs[, .(location_id, super_region_name)]
dt   <- merge(dt, locs)

## GET SDI
sdi <- get_covariate_estimates(covariate_id = 881, location_id = unique(dt$location_id), year_id = unique(dt$year_id), decomp_step = decomp_step)
sdi <- sdi[, .(location_id, year_id, mean_value)]
setnames(sdi, old = "mean_value", new = "sdi")

## MERGE SDI
dt <- merge(dt, sdi, by = c("location_id", "year_id"))

## PLOTTING
plotting_dt <- copy(dt)
plotting_dt <- plotting_dt[(year_id == 2017) & (!ihme_loc_id %like% "_")]
plotting_dt[sex_id == 1, sex := "Male"][sex_id == 2, sex := "Female"]

## PLOT
ggplot(data = plotting_dt, aes(x = sdi, y = age_std_cfr, colour = super_region_name)) +
  stat_smooth(method ="lm", se = FALSE, alpha = 0.5, colour = 'black', size = .5)+
  geom_text(aes(label = ihme_loc_id), nudge_y = 0.004, color = "black") +
  labs(x = "Socio-demographic Index", y = "Age-standardized predicted MI ratio", colour = "") +
  facet_wrap(~sex) + geom_point(size = 4.25, alpha = 0.75) + theme_bw() +
  theme(legend.position = "bottom", legend.text=element_text(size=16), strip.text = element_text(size=18),
        axis.title = element_text(size=22), axis.text = element_text(size = 20))

#############################################################################################
###                               PLOT MRBRT FITS TO DATA                                 ###
#############################################################################################

## CREATE PREDICTION TABLE
pred_dt <- as.data.table(expand.grid(intercept=1, haq=seq(0, 100, 0.5), female=0:1, age0to4=0, age5to14=0,
                                     age15to24=0, age25to34=0, age35to44=0, age45to54=0, age55to64=0,
                                     age65plus=0, central_eur=0, high_income=0, latin_am=0))

## CREATE AGE TABLE
orig_dt <- copy(pred_dt)
for(age in names(pred_dt)[names(pred_dt) %like% "age"]) {
  tmp <- copy(orig_dt)
  tmp[, (age) := 1]
  pred_dt <- rbind(pred_dt, tmp)
}

## CREATE SR TABLE
orig_dt <- copy(pred_dt)
for(sr in c("central_eur", "high_income", "latin_am")) {
  tmp <- copy(orig_dt)
  tmp[, (sr) := 1]
  pred_dt <- rbind(pred_dt, tmp)
}

## GET PREDICTIONS
pred1   <- predict_mr_brt(fit1, newdata = pred_dt)

## LOAD PREDICTIONS
pred_object <- load_mr_brt_preds(pred1)
preds       <- as.data.table(pred_object$model_summaries)

## CLEAN PREDICTIONS
preds[, `:=` (Y_negp=NULL, Y_mean_fe=NULL, Y_negp_fe=NULL, Y_mean_lo_fe=NULL, Y_mean_hi_fe=NULL)]
setnames(preds,
         old = names(preds)[names(preds) %like% "X_"],
         new = gsub("X_", "", names(preds)[names(preds) %like% "X_"]))

## GET INPUT DATA
preds            <- unique(preds)
mod_data         <- as.data.table(fit1$train_data)
mod_data$outlier <- floor(abs(mod_data$w - 1))

## PREP FOR PLOTTING
preds[age0to4==0 & age5to14==0 & age15to24==0 & age35to44==0 & age45to54==0 & age55to64==0 & age65plus==0, age25to34 := 1]
preds[central_eur==0 & high_income==0 & latin_am==0, africa_asia := 1]
preds[is.na(age25to34), age25to34 := 0]
preds[is.na(africa_asia), africa_asia := 0]

## PREP FOR PLOTTING
mod_data[africa==1 | asia==1 | north_af==1, africa_asia := 1]
mod_data[is.na(africa_asia), africa_asia := 0]

## CREATE SUPER-REGION INDICATOR FOR PREDS
preds[central_eur == 1, sr := "Central Europe"]
preds[high_income == 1, sr := "High Income"]
preds[latin_am == 1, sr := "Latin America"]
preds[africa_asia == 1, sr := "Reference"]

## CREATE SUPER-REGION INDICATOR FOR INPUT DATA
mod_data[central_eur == 1, sr := "Central Europe"]
mod_data[high_income == 1, sr := "High Income"]
mod_data[latin_am == 1, sr := "Latin America"]
mod_data[africa_asia == 1, sr := "Reference"]

## LOOP THROUGH AGE GROUP AND SEX TO PLOT FACETED BY REGION
pdf(file=paste0(brt_out_dir, "/mrbrt_fit_to_data.pdf"), width = 11)
  for(age in names(pred_dt)[names(pred_dt) %like% "age"]){
    for(sex in 0:1){
      # subset data
      plotdt  <- preds[get(age) == 1 & female == sex]
      plotmod <- mod_data[get(age) == 1 & female == sex]
      plotdt  <- plotdt[sr %in% unique(plotmod$sr)]
      # sex title
      if(sex==0) label_sex <- "Male"
      if(sex==1) label_sex <- "Female"
      # plot
      print(
        ggplot(data = plotdt, aes(x = haq, y = Y_mean)) +
          geom_line(color = "blue", size = 1.2, alpha = 0.8) + facet_wrap(~sr) +
          geom_ribbon(aes(ymin = Y_mean_lo, ymax = Y_mean_hi), alpha = 0.20) +
          geom_point(data = plotmod, aes(x = haq, y = logit_prop, size = 1/se_logit^2, shape = factor(outlier), col=sr), alpha = 0.5, size=3.25) +
          labs(x = "Healthcare access and quality index", y = "Logit MI ratio", shape = "Trimmed", col="Super-region", size="Inverse variance") +
          scale_shape_manual(values=c(19, 4)) + theme_bw() +
          ggtitle(paste0("Predicted MI ratios across HAQ index, ", label_sex, ", ", age)) +
          theme(legend.position = "bottom", legend.text=element_text(size=10), legend.title = element_text(size=11),
                title = element_text(size=14), axis.title = element_text(size=13),
                axis.text = element_text(size = 12), strip.text = element_text(size=12))
      )
    }
  }
dev.off()
