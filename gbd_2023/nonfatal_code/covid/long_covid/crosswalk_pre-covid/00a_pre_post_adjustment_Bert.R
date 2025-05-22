## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 00a_crosswalk_proportions.R
## Description: Crosswalking long covid symptom cluster proportion data.
##    Reference definition: Proportion due to covid-19, accounting for pre-covid
##                          health status
##    Alternative definition: Proportion w/ each symptom cluster regardless of
##                          pre-covid health status
## Inputs: Long covid symptom cluster data, w/ and w/o pre-covid health status
## Outputs: Long covid symptom cluster data crosswalked to reference def,
##          betas
## Contributors: NAME, NAME, NAME
## Date 3/8/2021
## --------------------------------------------------------------------- ----
## stamp: adding germany data and UW data (3/22/2021)
## stamp: adding the full long term cohort data (4/8/2021)

## Environment Prep ---------------------------------------------------- ----
rm(list = ls(all.names = T))

library(openxlsx)
library(dplyr)
library(data.table)
library(reticulate)

Sys.setenv("RETICULATE_PYTHON" = "FILEPATH")
use_python("FILEPATH")
cw <- import("crosswalk")

output_path <- "FILEPATH"

# set.seed(123)
# data for the meta-regression
# -- in a real analysis, you'd get this dataset by
#    creating matched pairs of alternative/reference observations,
#    then using delta_transform() and calculate_diff() to get
#    log(alt)-log(ref) or logit(alt)-logit(ref) as your dependent variable

# Crosswalk functions
# Delta method approximation
# uses the delta method approximation to transform random variables to/from log/logit space
delta_transform <- function(mean, sd, transformation) {

  if (transformation == "linear_to_log") f <- cw$utils$linear_to_log
  if (transformation == "log_to_linear") f <- cw$utils$log_to_linear
  if (transformation == "linear_to_logit") f <- cw$utils$linear_to_logit
  if (transformation == "logit_to_linear") f <- cw$utils$logit_to_linear

  out <- do.call("cbind", f(mean = array(mean), sd = array(sd)))
  colnames(out) <- paste0(c("mean", "sd"), "_", strsplit(transformation, "_")[[1]][3])
  return(out)
}

# Calculate differences between random variables
# calculates means and SDs for differences between random variables,
# ensuring that the alternative defintion/method is in the numerator: log(alt/ref) = log(alt) - log(ref)
calculate_diff <- function(df, alt_mean, alt_sd, ref_mean, ref_sd) {
  df <- as.data.frame(df)
  out <- data.frame(
    diff_mean =  df[, alt_mean] - df[, ref_mean],
    diff_sd = sqrt(df[, alt_sd]^2 + df[, ref_sd]^2)
  )
  return(out)
}

# 4/14/2021 read in new data

# TODO comment this out, uncomment the other block below; this is for the prior source
# now in a archived directory /GBD2021/
# datadate <- "070524"
datadate <- '101424'
user <- 'USERNAME'

meta <- setDT(read.xlsx(paste0("FILEPATH", datadate, "_", user, ".xlsx"), sheet = "extraction"))

alldata <- copy(meta)
# alldata <- alldata[, Group2 := fcase(
#   (study_id %in% c("HAARVI", "Faroe", "pa-COVID", "Sweden PronMed")) |
#     (study_id == "Zurich CC" & sample_characteristics == "retrospective sample" & follow_up_value == 6) |
#     (title == "Russia ISARIC cohort" & sample_age_group == "pediatric"),
#   "measurement_wo_pre", # value when TRUE
#   default="reference"  # value when FALSE
# )]

# Remove GBS from symptom clusters
alldata$symptom_cluster <- ifelse(is.na(alldata$symptom_cluster), alldata$outcome_name, alldata$symptom_cluster)
alldata <- alldata[symptom_cluster != "GBS"]

table(alldata$Group)
table(meta$Group)
table(alldata$symptom_cluster, useNA = "always")
table(meta$symptom_cluster, useNA = "always")

alldata[, condition := fcase(
  symptom_cluster == "any symptom main cluster", "long_term",
  symptom_cluster == "any symptom new cluster", "long_term_extended",
  symptom_cluster == "any symptom", "long_term_all",

  symptom_cluster == "post-acute fatigue syndrome", "post_acute",
  symptom_cluster == "post-acute fatigue", "post_acute",
  symptom_cluster == "cognitive", "cognitive",
  symptom_cluster == "mild cognitive among cognitive", "cog_mild",
  symptom_cluster == "moderate cognitive among cognitive", "cog_moderate",
  symptom_cluster == "respiratory", "res_combine",
  symptom_cluster == "mild respiratory among respiratory", "res_mild",
  symptom_cluster == "moderate respiratory among respiratory", "res_moderate",
  symptom_cluster == "severe respiratory among respiratory", "res_severe",
  symptom_cluster == "post-acute fatigue and respiratory", "Res_Fat",
  symptom_cluster == "post-acute fatigue and cognitive", "Cog_Fat",
  symptom_cluster == "respiratory and cognitive", "Cog_Res",
  symptom_cluster == "post-acute fatigue and respiratory and cognitive", "Cog_Res_Fat",

  # Group all residual symptom cluster into 1 group
  rep(TRUE, nrow(alldata)), "residuals" # NA_character_
)]

table(alldata$condition, useNA = "always")
table(alldata$Group, useNA = "always")

# separates reference and those needs adjustment
reference <- alldata[alldata$Group %in% "reference", ]
table(reference$study_id)

data <- setDT(alldata[alldata$Group %in% "measurement_wo_pre", ])
table(data$study_id)
table(data$location_name)

# calculate SE pre-adjustments
data[, cases := as.numeric(cases)]
data[, prev := mean]
data[, prev_se := sqrt((prev * prev) / sample_size)]

# check unique condition
sort(unique(reference$condition))
sort(unique(data$condition))
# table(data$condition, useNA = "always")

# Old proportions --------------------------------------------------------------
proportion <- setDT(read.xlsx("FILEPATH/proportion_v1.1.xlsx"))
proportion[, 1] <- NULL

# Take out zurich with >=6 months followup
proportion <- proportion[!is.na(proportion$measurement) &
  !proportion$measurement %in% 0 &
  !proportion$follow_up_time %in% c("6-months")]

table(proportion$source)
table(proportion$follow_up_time)
table(proportion$Group)

# Extract wo_pre figures that are valid
proportion_valid <- proportion[valid==1 & source != "Russia"]
table(proportion_valid$Group)
setnames(proportion_valid, "Sex", "sex")

# proportion_v1 <- read.xlsx("FILEPATH/proportion_v1.1.xlsx")
# write.xlsx(proportion_v1, "FILEPATH/proportion_v1.1.xlsx")
# file.remove("FILEPATH/proportion_v0.csv")

# Extract measurement_pre proportions ------------------------------------------
main_cluster <- c("post_acute", "cognitive", "cog_mild", "cog_moderate", "res_combine", "res_mild", "res_moderate", "res_severe", "Cog_Fat", "Res_Fat", "Cog_Res", "Cog_Res_Fat", "long_term")

all_cluster <- c(main_cluster, "long_term_extended", "long_term_all") # exclude "residuals" for now

proportion_v2 <- alldata[study_id %in% c("Sechenov StopCOVID", "Colombia CC") &
                         condition %in% all_cluster &
                         lit_nonlit == "non_lit" &
                         # Group == "reference" &
                         age_start %in% c(0, 18, 20) &
                         age_end %in% c(19, 99, 115)]

proportion_v2$follow_up_time <- paste0(proportion_v2$follow_up_value,"-",proportion_v2$follow_up_units)
unique(proportion_v2$follow_up_time)

proportion_v2 <- proportion_v2[, .(title, study_id, sample_size, sex, condition, Group, mean,
                                   follow_up_time, sample_population, sample_age_group,
                                   age_start, age_end, variant, lit_nonlit)]

# Take out zurich with >=6 months followup
# proportion_v2 <- proportion_v2[!is.na(proportion_v2$mean) & !proportion_v2$mean %in% 0]
# proportion_v2 <- proportion_v2[!(proportion_v2$study_id %in% c("Zurich CC") &
#                                    proportion_v2$follow_up_time %in% c("6-months", "9-months", "12-months"))]

# Remove mean = NA or 0
proportion_v2 <- proportion_v2[!is.na(proportion_v2$mean) & !proportion_v2$mean %in% 0]
proportion_v2$Group <- ifelse(proportion_v2$Group == "reference", "measurement_pre", "measurement_wo_pre")
table(proportion_v2$Group)

unique(proportion_v2$study_id)
unique(proportion_v2$sample_age_group)

proportion_v2$source<- ifelse(proportion_v2$study_id == "Italy ISARIC" &
                                proportion_v2$sample_age_group == "adults", "Italy Adult",
                       ifelse(proportion_v2$study_id == "Italy ISARIC" &
                                proportion_v2$sample_age_group == "pediatric", "Italy Pediatric",
                       ifelse(proportion_v2$study_id == "Sechenov StopCOVID", "Russia Adult",
                       ifelse(proportion_v2$study_id == "Sechenov StopCOVID peds", "Russia Pediatric",
                       ifelse(proportion_v2$study_id == "Iran ICC", "Iran",
                       ifelse(proportion_v2$study_id == "Colombia CC", "Colombia", proportion_v2$study_id))))))

setnames(proportion_v2, "mean", "measurement")
setnames(proportion_v2, "sample_size", "N")
table(proportion_v2$source)

# Merge with current Zurich figures
proportion_v2_short <- proportion_v2[, .(N, sex, condition, Group, measurement, source, follow_up_time, sample_population)]
proportion_valid$valid <- NULL
proportion_valid$valid_mod <- NULL

proportion_merged <- rbind(proportion_v2_short, proportion_valid)
table(proportion_merged$Group)
table(proportion_merged$source)
write.csv(proportion_merged, "FILEPATH/proportion_v2.csv")

# ------------------------------------------------------------------------------

# ------------------------------------------------------------------------------
data_orig <- copy(data)
data_adj <- data.table()

old_cluster <- c("cognitive", "long_term", "post_acute", "res_combine")
agg_cluster <- c("post_acute", "cognitive", "res_combine", "long_term", "long_term_extended", "long_term_all")

for (variable in agg_cluster) {
  df_adjusted <- copy(data[condition %in% variable])
  df_original <- copy(proportion_merged[(condition %in% variable &
    (sex %in% "Both") &
    source %in% unique(proportion_merged$source)), ])
    # source %in% c("Iran", "Russia", "Sechenov", "Zurich")), ])

  # #calculate the standard deviation around a single data point using
  # #ADDRESS
  # # the sample distribution of the sample proportion
  df_original[, prev_se := sqrt((measurement^2) / N)]
  df_original[, prev := measurement]

  method_var <- "Group"
  gold_def <- "measurement_pre"
  keep_vars <- c(
    "condition", "Group", "prev", "prev_se", "source",
    "follow_up_time", "sample_population"
  )

  # Original
  df_matched <- do.call("rbind", lapply(unique(df_original$condition), function(i) {

    dat_i <- filter(df_original, condition == i) %>%
      mutate(dorm = get(method_var))

    keep_vars <- c("dorm", keep_vars)
    row_ids <- expand.grid(idx1 = 1:nrow(dat_i), idx2 = 1:nrow(dat_i))

    do.call("rbind", lapply(1:nrow(row_ids), function(j) {
      dat_j <- row_ids[j, ]
      dat_j[, paste0(keep_vars, "_alt")] <- dat_i[dat_j$idx1, ..keep_vars]
      dat_j[, paste0(keep_vars, "_ref")] <- dat_i[dat_j$idx2, ..keep_vars]
      filter(dat_j, dorm_alt != gold_def & dorm_alt != dorm_ref)
    })) %>%
      mutate(id = i) %>%
      select(-idx1, -idx2)
  }))

  setDT(df_matched)

  # df_matched <- df_matched[source_alt == source_ref &
  #   follow_up_time_alt == follow_up_time_ref &
  #   sample_population_alt == sample_population_ref]

  df_matched <- df_matched[source_alt == source_ref & prev_alt > prev_ref &
                             ((follow_up_time_alt == follow_up_time_ref) | (is.na(follow_up_time_alt) & is.na(follow_up_time_ref))) &
                             ((sample_population_alt == sample_population_ref) | (is.na(sample_population_alt) & is.na(sample_population_ref)))]

  df_matched[, source := source_alt]

  # dat_diff <- as.data.frame(cbind(
  #   delta_transform(
  #     mean = df_matched$prev_alt,
  #     sd = df_matched$prev_se_alt,
  #     transformation = "linear_to_logit"
  #   ),
  #   delta_transform(
  #     mean = df_matched$prev_ref,
  #     sd = df_matched$prev_se_ref,
  #     transformation = "linear_to_logit"
  #   )
  # ))

  # delta_transform() deprecated, switch to using cw$utils$linear_to_logit instead
  # values remain the same but formatting now different, have to construct data.frame
  # ourselves
  dat_diff <- data.frame(
    mean_alt = cw$utils$linear_to_logit(
      mean = array(df_matched$prev_alt),
      sd = array(df_matched$prev_se_alt)
    )[[1]],
    mean_se_alt = cw$utils$linear_to_logit(
      mean = array(df_matched$prev_alt),
      sd = array(df_matched$prev_se_alt)
    )[[2]],
    mean_ref = cw$utils$linear_to_logit(
      mean = array(df_matched$prev_ref),
      sd = array(df_matched$prev_se_ref)
    )[[1]],
    mean_se_ref = cw$utils$linear_to_logit(
      mean = array(df_matched$prev_ref),
      sd = array(df_matched$prev_se_ref)
    )[[2]]
  )

  df_matched[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
    df = dat_diff,
    alt_mean = "mean_alt", alt_sd = "mean_se_alt",
    ref_mean = "mean_ref", ref_sd = "mean_se_ref"
  )

  write.csv(df_matched, file.path(output_path, paste0("FILEPATH", variable, "_", datadate, ".csv")))

  # format data for meta-regression; pass in data.frame and variable names
  dat1 <- cw$CWData(
    df = df_matched,
    obs = "logit_diff", # matched differences in logit space
    obs_se = "logit_diff_se", # SE of matched differences in logit space
    alt_dorms = "dorm_alt", # var for the alternative def/method
    ref_dorms = "dorm_ref", # var for the reference def/method
    covs = list("condition_ref"), # list of (potential) covariate columns
    study_id = "source" # var for random intercepts; i.e. (1|study_id)
  )

  fit1 <- cw$CWModel(
    cwdata = dat1, # result of CWData() function call
    obs_type = "diff_logit", # must be "diff_logit" or "diff_log"
    cov_models = list( # specify covariate details
      cw$CovModel("intercept")
    ),
    gold_dorm = "measurement_pre" # level of 'ref_dorms' that's the gold standard
  )
  
  # Additional step with reticulate method, fit() must be called after
  # setting up CWModel
  fit1$fit()

  names(df_adjusted)[names(df_adjusted) == "condition"] <- "condition_ref"
  message(variable)
  message(fit1$beta)
  message(fit1$beta_sd)

  # Save out fit1 beta and beta_sd by condition
  if (!exists("betas")) {
    betas <- data.frame(
      "condition" = variable,
      "beta" = fit1$beta,
      "beta_sd" = fit1$beta_sd
    )
  } else {
    betas <- rbind(
      betas,
      data.frame(
        "condition" = variable,
        "beta" = fit1$beta,
        "beta_sd" = fit1$beta_sd
      )
    )
  }

  df_subset <- df_adjusted[df_adjusted$prev > 0 &
    !is.na(df_adjusted$mean) &
    df_adjusted$prev < 1 &
    df_adjusted$Group != "reference", ]

  dropped_rows <- anti_join(df_adjusted, df_subset)


  df_subset[, c("mean_adjusted", "se_adjusted", "diff", "diff_se", "data_id")] <- fit1$adjust_orig_vals(
    df = df_subset,
    orig_dorms = "Group",
    orig_vals_mean = "prev",
    orig_vals_se = "prev_se"
  )

  if (nrow(dropped_rows) > 0) {
    dropped_rows[, c("se_adjusted", "diff", "diff_se", "data_id") := NA]
  }

  data_adj <- rbind(data_adj, df_subset)

  if (nrow(dropped_rows) > 0) {
    data_adj <- rbind(data_adj, dropped_rows)
  }
}

# No adjustments applied for severity levels
special <- data[condition %in%
  c(
    "cog_mild", "cog_moderate", "res_mild", "res_moderate",
    "res_severe", "Cog_Fat", "Res_Fat", "Cog_Res", "Cog_Res_Fat"
  )]

special[, c("se_adjusted", "diff", "diff_se", "data_id") := NA]
reference[, c("prev", "prev_se", "se_adjusted", "diff", "diff_se", "data_id") := NA]

names(data_adj)[names(data_adj) == "condition_ref"] <- "condition"
# data_adj[, condition := NULL]
# special[, condition := NULL]

final_all <- rbind(data_adj, special, reference, fill=T)
final_all[, adjusted_ratio := mean_adjusted / mean]
final_all[, follow_up_value := as.numeric(follow_up_value)]
sum(is.na(final_all$follow_up_value))
str(final_all)

final_all[, mean_orig := mean]
final_all[!is.na(mean_adjusted), mean := mean_adjusted]

final_all[, standard_error_orig := standard_error]
final_all[!is.na(se_adjusted), standard_error := se_adjusted]

final_all <- final_all[, !c(
  "prev", "prev_se", "Group", "se_adjusted", "diff",
  "diff_se", "data_id", "adjusted_ratio"
)]

# CROSS WALK FOR RESPONSE RATE
data_o12mo <- copy(final_all)
data_o12mo <- data_o12mo[
  ((follow_up_value > 330 & follow_up_units == "days") |
    (follow_up_value > 50 & follow_up_units == "weeks") |
    (follow_up_value > 11 & follow_up_units == "months") |
    (follow_up_value >= 1 & follow_up_units == "year")) &
    !is.na(response_rate) &
    mean > 0 &
    !symptom_cluster %in% c(
      "mild cognitive among cognitive", "mild respiratory among respiratory",
      "moderate cognitive among cognitive", "moderate respiratory among respiratory",
      "severe respiratory among respiratory"
    )
]

the_rest_of_the_data <- final_all[
  !(((follow_up_value > 330 & follow_up_units == "days") |
     (follow_up_value > 50 & follow_up_units == "weeks") |
     (follow_up_value > 11 & follow_up_units == "months") |
     (follow_up_value >= 1 & follow_up_units == "year")) &
    !is.na(response_rate) &
    mean > 0 &
    !symptom_cluster %in% c(
      "mild cognitive among cognitive", "mild respiratory among respiratory",
      "moderate cognitive among cognitive", "moderate respiratory among respiratory",
      "severe respiratory among respiratory"
    ))
]

table(data_o12mo$study_id)
# ....
# Read csv 
# Rename columns 
# imputed c/d = prev_ref prev_se_ref
# e/f = prev_alt / prev_se_alt

# TODO make this more robust
df_matched <- fread(
  file.path(
    'extraction_response_rate_bias_Zurich.csv')
)

df_matched[, source := 'zurich']
df_matched[, loss_to_fu := 1 - response_rate]

df_matched[, dorm_alt := 'available']
df_matched[, dorm_ref := 'imputed']

setnames(
  df_matched,
  old = c('imputed_data', 'imputed_data_se', 'available_data', 'available_data_se'),
  new = c('prev_ref', 'prev_se_ref', 'prev_alt', 'prev_se_alt')
)

dat_diff <- data.frame(
  mean_alt = cw$utils$linear_to_logit(
    mean = array(df_matched$prev_alt),
    sd = array(df_matched$prev_se_alt)
  )[[1]],
  mean_se_alt = cw$utils$linear_to_logit(
    mean = array(df_matched$prev_alt),
    sd = array(df_matched$prev_se_alt)
  )[[2]],
  mean_ref = cw$utils$linear_to_logit(
    mean = array(df_matched$prev_ref),
    sd = array(df_matched$prev_se_ref)
  )[[1]],
  mean_se_ref = cw$utils$linear_to_logit(
    mean = array(df_matched$prev_ref),
    sd = array(df_matched$prev_se_ref)
  )[[2]]
)

df_matched[, c("logit_diff", "logit_diff_se")] <- calculate_diff(
  df = dat_diff,
  alt_mean = "mean_alt", alt_sd = "mean_se_alt",
  ref_mean = "mean_ref", ref_sd = "mean_se_ref"
)

# format data for meta-regression; pass in data.frame and variable names
dat1 <- cw$CWData(
  df = df_matched,
  obs = "logit_diff", # matched differences in logit space
  obs_se = "logit_diff_se", # SE of matched differences in logit space
  alt_dorms = "dorm_alt", # var for the alternative def/method
  ref_dorms = "dorm_ref", # var for the reference def/method
  covs = list("loss_to_fu"), # list of (potential) covariate columns
  study_id = "source", # var for random intercepts; i.e. (1|study_id),
  add_intercept = FALSE # adds a column called "intercept" that may be used in CWModel()
)

fit1 <- cw$CWModel(
  cwdata = dat1, # result of CWData() function call
  obs_type = "diff_logit", # must be "diff_logit" or "diff_log"
  cov_models = list(
    cw$CovModel(cov_name = "loss_to_fu")
  ),
  gold_dorm = "imputed" # level of 'ref_dorms' that's the gold standard
)

# Additional step with reticulate method, fit() must be called after
# setting up CWModel
fit1$fit()

message(fit1$beta)
message(fit1$beta_sd)

dropped_rows <- anti_join(final_all, data_o12mo)

data_o12mo[, loss_to_fu := 1 - response_rate]

data_o12mo[, prev_se := sqrt((mean * mean) / sample_size)]

# for data mean:
#   1. take logit(mean) using lineartologit function
#   2. logit(adjusted) = logit(mean) + beta * losstofu
#   3. convert adjusted to normal space by function logittolinear
data_o12mo[mean == 1, mean := 0.999]


data_o12mo$logit_mean <- cw$utils$linear_to_logit(
  mean = array(data_o12mo$mean),
  sd = array(data_o12mo$prev_se)
)[[1]]

data_o12mo[, logit_adjusted := logit_mean - fit1$beta[1] * loss_to_fu]

# Calc se first

# for SE:
# 1.  calc SE for data points
# 2. convert SE to logit space using lineartologit like
# 3. add variances in logit space and square root to get combined SE like sqrt(beta_sd^2 + data_se^2)
# 4. convert combined SE to linear space with logit to linear function
data_o12mo$logit_prev_se <- cw$utils$linear_to_logit(
  mean = array(data_o12mo$mean),
  sd = array(data_o12mo$prev_se)
)[[2]]

data_o12mo[, combined_se := sqrt(fit1$beta_sd[1]^2 + logit_prev_se^2)]

data_o12mo$mean_adjusted_o12mo <- cw$utils$logit_to_linear(
  mean = array(data_o12mo$logit_adjusted),
  sd = array(data_o12mo$combined_se)
)[[1]]

data_o12mo$mean_adjusted_012mo_se <- cw$utils$logit_to_linear(
  mean = array(data_o12mo$logit_adjusted),
  sd = array(data_o12mo$combined_se)
)[[2]]

data_o12mo[, mean_orig := mean]
data_o12mo[!is.na(mean_adjusted_o12mo), mean := mean_adjusted_o12mo]

data_o12mo[, standard_error_orig := standard_error]
data_o12mo[!is.na(mean_adjusted_012mo_se), standard_error := mean_adjusted_012mo_se]

data_o12mo <- data_o12mo[, !c(
  "prev", "prev_se", "Group", "se_adjusted", "diff",
  "diff_se", "data_id", "adjusted_ratio"
)]


final_all <- rbind(the_rest_of_the_data, data_o12mo, fill=T)


# Write out files
write.csv(final_all, file.path(output_path, paste0("GBD2023/adjusted_long_covid_data_", datadate, ".csv")), row.names = F)
write.csv(betas, file.path(output_path, paste0("GBD2023/long_covid_crosswalk_betas_", datadate, ".csv")))



