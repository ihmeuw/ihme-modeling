#' @author 
#' @date 2019/05/08
#' @description preliminary model of CFR on HAQi

rm(list=ls())

pacman::p_load(data.table, ggplot2, boot, plotly)


# USER INPUTS -------------------------------------------------------------
main_dir         <- # filepath
dem_dir          <- paste0(main_dir, "data/")          # dir for input data
offset_dir       <- paste0(main_dir, "offset_values/") # dir to output offset values
plot_dir         <- paste0(main_dir, "model_plots/")   # dir to output model plots
model_object_dir <- paste0(main_dir, "model_objects/") # dir to output MR-BRT model objects
mrbrt_dir        <- paste0(main_dir, "mrbrt_outputs/") # dir to output MR-BRT model outputs

bundle_id <- 7181
ds <- 'step4'

date <- gsub("-", "_", Sys.Date())
# name prefix used in offset csv, model object, MR-BRT directory, and plots
model_prefix <- paste0(date, "_one_hot_encode_etiologies")

etiologies <- c("meningitis_pneumo", 
                "meningitis_hib", 
                "meningitis_meningo", 
                "meningitis_other")

# SOURCE FUNCTIONS --------------------------------------------------------
k <- # filepath
source(paste0(k, "current/r/get_covariate_estimates.R"))
source(paste0(k, "current/r/get_location_metadata.R"))
source(paste0(k, "current/r/get_population.R"))
source(paste0(k, "current/r/get_age_metadata.R"))
source(paste0(k, "current/r/get_ids.R"))
source(paste0(k, "current/r/get_bundle_data.R"))

repo_dir <- # filepath
source(paste0(repo_dir, "mr_brt_functions.R"))

# DEFINE FUNCTIONS --------------------------------------------------------
# MARKS ROWS FOR AGE-SEX AGGREGATION
find_all_age_both_sex <- function(raw_dt) {
  dt <- copy(raw_dt)
  dt[, agg_age_sex := 0]
  # mark all clinical data for aggregation
  dt[cv_inpatient == 1, agg_age_sex := 1]
  # loop over literature data studies
  lit_nids <- unique(dt[cv_inpatient != 1 | is.na(cv_inpatient), nid])
  for (n in lit_nids) {
    study_etiologies <- dt[nid == n, unique(case_name)]
    # check for each etiology that the study contains ages between 0 and 95 and 
    # either both sex or male and female
    for (e in study_etiologies) {
      row_min_age <- min(dt[nid == n & case_name == e, age_start])
      row_max_age <- max(dt[nid == n & case_name == e, age_end])
      row_sexes <- dt[nid == n & case_name == e, unique(sex)]
      if (row_min_age == 0 & row_max_age >= 95) {
        if (length(row_sexes) == 2) {
          if (isTRUE(all.equal(row_sexes, c("Male", "Female")))) {
            dt[nid == n & case_name == e, agg_age_sex := 1]
          }
        } else if (length(row_sexes) == 1) {
          if (row_sexes == "Both") {
            dt[nid == n & case_name == e, agg_age_sex := 1]
          }
        }
      }
    }
  }
  return(dt)
}

# AGGREGATE OVER ALL AGES AND BOTH SEXES
age_sex_agg <- function(raw_dt) {
  dt <- copy(raw_dt)
  dt <- dt[agg_age_sex == 1]
  dt[, `:=` (age_start = 0, age_end = 99, sex = "both")]
  col_names <- names(dt)[!names(dt) %in% c("cases", "sample_size")]
  dt <- dt[, lapply(.SD, sum, na.rm=TRUE), 
          by = c("nid", "location_id", "location_name", "age_start", "age_end", 
                 "sex", "year_start", "year_end", "case_name", "measure", 
                 "cv_inpatient", "lower", "upper", "clinical_data"), 
          .SDcols = c("cases", "sample_size")]
  dt[, mean := cases / sample_size]
  # some rows may be missing mean if they don't have cases and sample_size
  null_row_count <- nrow(dt[is.na(mean)])
  message(paste("Dropping", null_row_count, 
                "rows that could not be aggregated"))
  dt <- dt[!is.na(mean)]
  return(dt)
}

# CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
get_se <- function(raw_dt) {
  dt <- copy(raw_dt)
  dt$standard_error <- as.double(dt$standard_error)
  z <- qnorm(0.975)
  dt[is.na(standard_error) & !is.na(sample_size) & measure == "cfr", 
     standard_error := sqrt(mean * (1 - mean) / sample_size 
                            + z^2 / (4 * sample_size^2))]
  null_row_count <- nrow(dt[is.na(standard_error)])
  message(paste("Dropping", null_row_count, 
                "rows that do not have standard error"))
  dt <- dt[!is.na(standard_error)]
  return(dt)
}

# GET LOCATION METADATA
get_loc_metadata <- function(dt) {
  loc_meta <- get_location_metadata(location_set_id = 35, gbd_round_id = 6)
  loc_meta <- loc_meta[, .(location_id, region_id, region_name, 
                           super_region_id, super_region_name)]
  dt <- merge(dt, loc_meta, by = "location_id", all.x = T)
  return(dt)
}

# GET HAQi
get_haqi <- function(raw_dt) { 
  haqi_dt <- get_covariate_estimates(covariate_id = 1099, 
                                    location_id = 'all', 
                                    year_id = 'all', 
                                    gbd_round_id = 6, 
                                    decomp_step = 'step3')
  haqi_dt <- haqi_dt[, .(location_id, year_id, haqi = mean_value)]
  # Merge by location and year midpoint
  dt[, year_id:= floor((year_start + year_end) / 2)]
  dt <- merge(dt, haqi_dt, by = c("location_id", "year_id"), all.x = T)
  # for year midpoints less than 1980 there are no HAQ values
  null_row_count <- nrow(dt[is.na(haqi)])
  message(paste("Dropping", null_row_count, "rows that do not have HAQi"))
  dt <- dt[!is.na(haqi)]
  return(dt)
}

# DROP MEAN == 1 FOR LOGISTIC REGRESSION
drop_mean_1 <- function(raw_dt) {
  dt <- copy(raw_dt)
  message(paste("Dropping", nrow(dt[mean == 1]), "rows that have mean = 1"))
  dt <- dt[mean != 1]
}

# OFFSET MEAN == 0 FOR LOGISTIC REGRESSION
# WRITES OFFSET CSV and saves in offset_dir
offset_mean_0 <- function(raw_dt) {
  dt <- copy(raw_dt)
  offset_list <- c()
  for(e in etiologies) {
    offset <- 0.01 * median(dt[mean != 0 & case_name == e, mean])
    dt[mean == 0 & case_name == e, mean := mean + offset]
    offset_list <- c(offset, offset_list)
  }
  offset_dt <- data.table(etiology = etiologies, offset = offset_list)
  offset_filename <- paste0(offset_dir, model_prefix, "_offset_values.csv")
  print("Offset values:")
  print(offset_dt)
  fwrite(offset_dt, offset_filename)
  return(dt)
}

# LOGIT TRANSFORM MEAN AND SE
logit_transform <- function(raw_dt) {
  dt <- copy(raw_dt)
  # transform mean and SE into logit space using delta method
  dt[, logit_mean := log(mean / (1 - mean))]
  dt[, delta_logit_se := sqrt((1/(mean - mean^2))^2 * standard_error^2)]
  return(dt)
}

# CREATE DUMMY VARIABES FOR ETIOLOGIES
onehot_encode <- function(raw_dt) {
  dt <- copy(raw_dt)
  # one-hot encoding for etiologies
  dt[, etio_pneumo  := ifelse(case_name == "meningitis_pneumo",  1, 0)]
  dt[, etio_hib     := ifelse(case_name == "meningitis_hib",     1, 0)]
  dt[, etio_meningo := ifelse(case_name == "meningitis_meningo", 1, 0)]
  dt[, etio_other   := ifelse(case_name == "meningitis_other",   1, 0)]
  return(dt)
}

# RUN FUNCTIONS -----------------------------------------------------------
dt <- get_bundle_data(bundle_id, ds)
clinical_nids <- c(222560, 222563, 212493, 3822, 67132, 285520, 121334, 121405, 
                   121407, 121408, 121415, 121416, 121417, 121418, 121419, 121420, 
                   121421, 121422, 121423, 121424, 121425, 121444, 121445, 121446, 
                   121447, 121448, 121449, 121450, 121451, 121452, 121453, 121454, 
                   193857, 86901, 86902, 86903, 86904, 86905, 86906, 86907, 86908, 
                   86909, 86910, 86911, 86912, 86913, 86914, 86915, 86916, 86917, 
                   86886, 86887, 86888, 86889, 86890, 86891, 86892, 86893, 86894, 
                   86895, 86896, 86897, 86898, 86899, 86900, 86997, 86998, 86999, 
                   87000, 87001, 87002, 87003, 87004, 87005, 87006, 87007, 87008, 
                   87009, 87010, 87011, 114876, 160484, 237756, 331084, 317423, 
                   130051, 130054, 292436, 292435, 292437, 316184, 133665, 
                   282493, 282496, 282497, 336860, 90314, 90315, 90316, 90317, 
                   90318, 90319, 90322, 86950, 86951, 86952, 86953, 86954, 
                   86956, 86958, 94171, 94170, 86949, 86955, 86957, 121282, 
                   206640, 104246, 104247, 104248, 104249, 104250, 104251, 104252, 
                   104253, 104254, 87014, 87013, 87012, 26333, 104255, 104256, 
                   104257, 104258, 221323, 237832, 281543, 333360, 281819, 
                   149500, 149501, 149502, 149503, 149504, 336193, 336195, 
                   336197, 336198, 336199, 336200, 331137, 331138, 331139, 331140, 331141, 331142, 331143, 331144, 331145, 331146, 331147, 331148, 134187, 121272, 121273, 121277, 121278, 121279, 121280, 121281, 265423, 265424, 265425, 121274, 121275, 121276, 220786, 220787, 220788, 220789, 220790, 220791, 220792, 220793, 220794, 220795, 220796, 220797, 220798, 220799, 220800, 293984, 121917, 121841, 121842, 121843, 121844, 121845, 121846, 121847, 121848, 121849, 121850, 121851, 121831, 121854, 121855, 121856, 121857, 121858, 121859, 121860, 121862, 121863, 121832, 128781, 205019, 239353, 68535, 68367, 333358, 333359, 333361)
dt[, clinical_data := 0]
dt[nid %in% clinical_nids, clinical_data := 1]
dt <- find_all_age_both_sex(dt)
dt <- age_sex_agg(dt)
dt <- get_se(dt)
dt <- get_loc_metadata(dt)
dt <- get_haqi(dt)
dt <- drop_mean_1(dt)
dt <- offset_mean_0(dt)
dt <- logit_transform(dt)
dt <- onehot_encode(dt)

# concatenate nid and location to use as MR-BRT study ID
dt[, nid_location := paste0(nid, "_", location_id)]

# RUN MR-BRT --------------------------------------------------------------
# set MR-BRT X-covs and Z-covs
covs1 <- list(cov_info("haqi",          "X", type = "continuous"),
              cov_info("etio_pneumo",   "X", type = "categorical"),
              cov_info("etio_hib",      "X", type = "categorical"),
              cov_info("etio_meningo",  "X", type = "categorical"),
              cov_info("etio_pneumo",   "Z", type = "categorical"),
              cov_info("etio_hib",      "Z", type = "categorical"),
              cov_info("etio_meningo",  "Z", type = "categorical"))

train_dt <- dt[, .(nid, location_id, nid_location, age_start, age_end, 
                   year_start, year_end, sex,mean, cases, sample_size, 
                   standard_error, logit_mean, delta_logit_se, haqi, 
                   etio_pneumo, etio_hib, etio_meningo, etio_other)]

model_fit <- run_mr_brt(
  output_dir  = mrbrt_dir,
  model_label = model_prefix,
  data        = train_dt,
  mean_var    = "logit_mean",
  se_var      = "delta_logit_se",
  method      = "trim_maxL",
  study_id    = "nid_location",
  covs        = covs1,
  trim_pct    = 0.1,
  max_iter    = 200,
  overwrite_previous = TRUE
)
plot_mr_brt(model_fit)
saveRDS(model_fit, paste0(model_object_dir, model_prefix, "_UPDATE_dir_model_fit.RDS"))


# PREDICT MR-BRT ----------------------------------------------------------
# create datasets to make predictions on x-covs and z-covs
x_pred_dt <- expand.grid(intercept    = 1,
                         haqi         = seq(0, 100, 0.01),
                         etio_pneumo  = c(0, 1),
                         etio_meningo = c(0, 1),
                         etio_hib     = c(0, 1))
setDT(x_pred_dt)
# drop rows with more than 1 indicator variable marked 1
x_pred_dt[, sum := etio_pneumo + etio_meningo + etio_hib]
x_pred_dt <- x_pred_dt[sum <= 1]
z_pred_dt <- expand.grid(intercept    = 1,
                         etio_pneumo  = c(0, 1),
                         etio_meningo = c(0, 1),
                         etio_hib     = c(0, 1))
setDT(z_pred_dt)
z_pred_dt[, sum := etio_pneumo + etio_meningo + etio_hib]
z_pred_dt <- z_pred_dt[sum <= 1]

pred1 <- predict_mr_brt(model_fit, newdata = x_pred_dt, z_newdata = z_pred_dt)
pred_object <- load_mr_brt_preds(pred1)
preds <- pred_object$model_summaries
setDT(preds)
# drop predictions with more than 1 indicator variable is marked 1
preds <- preds[X_etio_pneumo == Z_etio_pneumo 
               & X_etio_hib == Z_etio_hib 
               & X_etio_meningo == Z_etio_meningo]
preds[, sum := X_etio_pneumo + X_etio_hib + X_etio_meningo]
preds <- preds[sum <= 1]
# convert one hot encoding to categorical case name field 
preds[                   , case_name := "meningitis_other"]
preds[X_etio_pneumo  == 1, case_name := "meningitis_pneumo"]
preds[X_etio_hib     == 1, case_name := "meningitis_hib"]
preds[X_etio_meningo == 1, case_name := "meningitis_meningo"]
# convert one hot encoding to formatted name field, for plotting
preds[                   , etiology := "Other meningitis"]
preds[X_etio_pneumo  == 1, etiology := "Pneumococcal meningitis"]
preds[X_etio_hib     == 1, etiology := "HiB meningitis"]
preds[X_etio_meningo == 1, etiology := "Meningococcal meningitis"]
# pull in training data for plotting and mark outliers with weights < 0.5
mod_data <- model_fit$train_data
mod_data$outlier  <- round(abs(mod_data$w - 1))
# inverse logit back to linear space
preds[, `:=` (Y_mean_lin    = inv.logit(Y_mean), 
              Y_mean_lo_lin = inv.logit(Y_mean_lo), 
              Y_mean_hi_lin = inv.logit(Y_mean_hi))]

# CREATE PLOTS ------------------------------------------------------------
pdf(paste0(plot_dir, model_prefix, ".pdf"))
myplot_lin <- ggplot(data = preds, aes(x = X_haqi, y = Y_mean_lin)) + 
  geom_line(color = "blue", size = 1.5, alpha = 0.8) +
  geom_ribbon(aes(ymin = Y_mean_lo_lin, ymax = Y_mean_hi_lin), alpha = 0.3) +
  geom_point(data = mod_data, aes(x = haqi, y = mean, size = 1/standard_error^2,
                                  col = factor(outlier)), alpha = 0.6) +
  labs(x = "HAQ", y = "CFR", col = "Trimmed") + theme_minimal() +
  guides(size = F)
print(myplot_lin + facet_wrap(~ etiology))

# plot etiologies together
myplot <- ggplot(data = preds, 
                 aes(x = X_haqi,
                     y = Y_mean_lin,
                     color = etiology, 
                     fill  = etiology)) + 
  geom_line(size = 1.5, alpha = 0.8) +
  geom_ribbon(aes(ymin = Y_mean_lo_lin, 
                  ymax = Y_mean_hi_lin), 
              alpha = 0.075, colour = NA, show.legend = F) +
  labs(x = "HAQ", y = "CFR", col = "Etiology") + theme_minimal()
print(myplot)
dev.off()
