## ******************************************************************************
##
# Age-sex split and crosswalk
##
## ******************************************************************************

rm(list=ls())

args <- commandArgs(trailingOnly = TRUE)
BUN_ID <- args[1]
MEASURE_NAME <- args[2]

library('openxlsx')
library('tidyr')
library('dplyr')
library('data.table')
library('matrixStats')
library('testit', lib.loc="FILEPATH")

# shared functions
central <- "FILEPATH"
for (func in paste0(central, list.files(central))) source(paste0(func))

#source custom functions
source(paste0("FILEPATH", "functions_agesex_split.R"))
source(paste0("FILEPATH", "divide_data.R"))

# global objects
DATE <- gsub("-", "_", Sys.Date())
SAVE_DIR <- "FILEPATH"
# read in most recent settings file
MAP <- get_recent(folder = "FILEPATH")
ME_ID <- MAP[bundle == BUN_ID, me_id]
VERSION <- MAP[bundle == BUN_ID, version]
AGE_MAP <- fread("FILEPATH")


# ------------------------------------------------------------------------------ DEFINE FUNCTIONS -----------------------------------------------------------------------------------------------------

age_sex_split <- function(bundle, version){
  
  # read in data, drop outliers, fill in blank mean, cases, ss
  bun_data <- as.data.table(read.xlsx("FILEPATH"))
  # some bundles don't have a group review column, add one and include everything
  if (!("group_review" %in% names(bun_data))){
    bun_data[, group_review := 1]
  }
  # exclude these rows in pull_bundle_data
  bun_data[is.na(group_review), group_review:=1]
  group_review <- bun_data[group_review==0,]
  df <- pull_bundle_data(measure_name = MEASURE_NAME, bun_id = bundle, bun_data = bun_data)
  
  #-----------------------------------------------------------------------------------
  #subset data into an aggregate dataset and a fully-specified dataset
  #-----------------------------------------------------------------------------------
  data <- divide_data(input_data = df)
  good_data <- data[need_split == 0]
  # rows in all data that aren't in good data
  aggregate <- fsetdiff(data, good_data, all = TRUE)
  # just a check
  aggregate <- aggregate[need_split == 1,]
  
  if (nrow(aggregate)==0) { 
    stop(paste("Bundle",bundle,"does not contain any",MEASURE_NAME,"data that needs to be split"))
  }
  
  #-----------------------------------------------------------------------------------
  #expand the aggregate dataset into its constituent age and sexes
  #-----------------------------------------------------------------------------------
  expanded <- expand_test_data(agg.test = aggregate, bun_id = bundle)
  
  #-----------------------------------------------------------------------------------
  #merge populations and age group ids onto the expanded dataset
  #-----------------------------------------------------------------------------------
  if ("age_group_id" %in% names(expanded) == TRUE) {
    expanded$age_group_id <- NULL
  }
  
  expanded <- add_pops(expanded, bun_id = bundle, age_map = AGE_MAP)
  print_log_message("Loaded populations")
  
  #-----------------------------------------------------------------------------------
  # Pull model results from DisMod to use as age/sex weights
  #-----------------------------------------------------------------------------------
  #label each row with the closest prior dismod estimation year for matching to dismod model results
  #round down
  expanded[year_id%%5 < 3, est_year_id := year_id - year_id%%5]
  #round up
  expanded[year_id%%5 >= 3, est_year_id := year_id - year_id%%5 + 5]
  expanded[est_year_id < 1990, est_year_id := 1990]
  expanded[year_id == 2017, est_year_id := 2017]
  
  #' Pull draw data for each age-sex-yr for every location in the current aggregated test data 
  #' needed to be split.
  weight_draws <- pull_model_weights(model_id = ME_ID, measure_name = MEASURE_NAME, expanded = expanded)
  print_log_message("Pulled DisMod results")
  
  #' Append draws to the aggregated dataset
  draws <- merge(expanded, weight_draws, by = c("age_group_id", "sex_id", "location_id", "est_year_id"), 
                 all.x=TRUE)
  
  #' Take all the columns labeled "draw" and melt into one column, row from expanded now has 1000 
  #' rows with same data with a unique draw. Draw ID for the equation
  draws <- melt.data.table(draws, id.vars = names(draws)[!grepl("draw", names(draws))], measure.vars = patterns("draw"),
                           variable.name = "draw.id", value.name = "model.result")
  
  ####################################################################################
  
  #' This is the numerator of the equation, the total number of cases for a specific age-sex-loc-yr
  #' based on the modeled prevalence
  draws[, numerator := model.result * population]
  
  #' This is the denominator, the sum of all the numerators by both draw and split ID. The number of cases in the aggregated age/sex 
  #' group.
  #' The number of terms to be summed should be equal to the number of age/sex groups present in the original aggregated data point
  draws[, denominator := sum(numerator), by = .(split.id, draw.id)]
  
  #' Calculate the actual estimate of the split point from the input data (mean) and a unique draw from the modelled 
  #' prevalence (model.result)
  draws[, estimate := (mean * model.result / denominator) * pop.sum]
  draws[, sample_size_new := sample_size * population / pop.sum]
  
  # If the numerator and denominator is zero, set the estimate to zero
  draws[numerator == 0 & denominator == 0, estimate := 0]
  
  # Collapsing the draws by the expansion ID, by taking the mean, SD, and quantiles of the 1000 draws of each calculated split point
  #' Each expand ID has 1000 draws associated with it, so just take summary statistics by expand ID
  final <- draws[, .(mean.est = mean(estimate),
                     sd.est = sd(estimate),
                     upr.est = quantile(estimate, .975),
                     lwr.est = quantile(estimate, .025),
                     sample_size_new = unique(sample_size_new),
                     cases.est = mean(numerator),
                     agg.cases = mean(denominator)), by = expand.id] %>% merge(expanded, by = "expand.id")
  
  #if the mean is zero, calculate the standard error using Wilson's formula instead of the standard deviation of the mean
  z <- qnorm(0.975)
  final[sd.est == 0 & mean.est == 0 & measure == "prevalence", 
        sd.est := (1/(1+z^2/sample_size_new)) * sqrt(mean.est*(1-mean.est)/sample_size_new + z^2/(4*sample_size_new^2))]
  final[, se.est := sd.est]
  final[, agg.sample.size := sample_size]
  final[, sample_size := sample_size_new]
  final[,sample_size_new:=NULL]
  
  #' Set all proper/granular age/sex groups derived from an aggregated group of 0 to also 0
  final[mean==0, mean.est := 0]
  final[, case_weight := cases.est / agg.cases]
  final$agg.cases <- NULL
  final[, crosswalk_parent_seq := seq]
  final[, seq := NA]
  
  #-----------------------------------------------------------------------------------
  #' Save off just the split data for troubleshooting/diagnostics
  #-----------------------------------------------------------------------------------
  split_data <- final[, c('nid','crosswalk_parent_seq','age_start','age_end','sex_id','mean',
                          'standard_error', 'cases', 'case_weight','sample_size',
                          'agg_age_start','agg_age_end','agg_sex_id',
                          'agg.sample.size', 'mean.est', 'se.est',
                          'population','pop.sum',
                          'age_group_id','age_demographer','n.age','n.sex',
                          'location_id','year_start','year_end','est_year_id')]
  
  setnames(split_data, c("mean", "standard_error", "cases"), c("agg.mean", "agg.std.error", "agg.cases"))
  setnames(split_data, c("mean.est", "se.est"), c("mean", "standard_error"))
  split_data <- split_data[order(nid)]
  
  dir.create(paste0("FILEPATH"), recursive = TRUE, showWarnings = FALSE)
  
  write.csv(split_data, 
            file = "FILEPATH",
            row.names = FALSE)
  
  
  #-----------------------------------------------------------------------------------
  #' Append split data back onto fully-specified data and save the fully split bundle
  #-----------------------------------------------------------------------------------
  # data that didnt need to be split
  good_data[, crosswalk_parent_seq := numeric()]
  # aggregate data that was split
  aggregate[, crosswalk_parent_seq := numeric()]
  
  full_bundle <- rbind(good_data, aggregate)
  
  full_bundle[, c("age_start", "age_end") := NULL]
  setnames(full_bundle, c("orig_age_start", "orig_age_end"), c("age_start", "age_end"))
  
  # drop agg mean, cases, ss
  final[, c("mean", "standard_error", "cases") := NULL]
  setnames(final, c("mean.est", "se.est"), c("mean", "standard_error"))
  
  final[, sex := ifelse(sex_id == 1, "Male", "Female")]
  final[, `:=` (lower = lwr.est, upper = upr.est,
                cases = NA, effective_sample_size = NA)]
  
  final <- final[,names(full_bundle),with = FALSE]
  final[, need_split := 0]
  full_bundle <- rbind(full_bundle, final)
  
  # group reviewed data that was excluded from the beginning
  group_review[, crosswalk_parent_seq := numeric()]
  
  full_bundle[need_split==1, group_review := 0]
  
  full_bundle[, c("sex_id", "need_split") := NULL]
  full_bundle <- rbind(full_bundle, group_review)
  
  bun_data[, crosswalk_parent_seq := numeric()]
  bun_data <- bun_data[,names(full_bundle),with = FALSE]
  
  df[,crosswalk_parent_seq := numeric()]
  df <- df[, names(full_bundle), with=FALSE]
  
  # excluding ss, cases, and group review because filled in for original data points
  dropped <- fsetdiff(df[, !names(df) %in% c("cases", "sample_size", "group_review"), with=FALSE], full_bundle[, !names(full_bundle) %in% c("cases", "sample_size", "group_review"), with=FALSE])
  assert("some data was dropped", nrow(dropped) == 0)
  
  full_bundle[is.na(full_bundle)] <- " "
  full_bundle <- full_bundle[order(seq)]
  
  print_log_message("writing all the data!")
  write.csv(full_bundle, 
            file = "FILEPATH",
            row.names = FALSE)
}

# ------------------------------------------------------------------------------ RUN AGE-SEX SPLITTING -----------------------------------------------------------------------------------------------------
age_sex_split(bundle = BUN_ID, version = VERSION)

## ---------------------------------------------------------------------------- MR BRT CROSSWALK ------------------------------------------------------------------------------------------------------------
prep_data <- function(df, se_trim){
  # getting the standard errors of the ref and alternate def
  df <- df %>%
    mutate(
      ref_se = (ref_UL - ref_LL) / 3.92,
      alt_se = (alt_UL - alt_LL) / 3.92
    )
  
  # transformations in logit space
  df2 <- df %>%
    mutate(
      prev_logit_alt = logit(mean_alt),
      prev_logit_ref = logit(mean_ref)
    )
  
  df2$se_prev_logit_alt <- sapply(1:nrow(df2), function(i) {
    alt_i <- df2[i, "mean_alt"]
    alt_se_i <- df2[i, "alt_se"]
    deltamethod(~log(x1/(1-x1)), alt_i, alt_se_i^2)
  })
  
  df2$se_prev_logit_ref <- sapply(1:nrow(df2), function(i) {
    ref_i <- df2[i, "mean_ref"]
    ref_se_i <- df2[i, "ref_se"]
    deltamethod(~log(x1/(1-x1)), ref_i, ref_se_i^2)
  })
  
  df2 <- df2 %>%
    mutate(
      diff_logit = prev_logit_alt - prev_logit_ref,
      se_diff_logit = sqrt(se_prev_logit_alt^2 + se_prev_logit_ref^2)
    )
  
  refs <- as.character(unique(df2$ref_case_def))
  alts <- as.character(unique(df2$alt_case_def))
  
  more_alts <- setdiff(refs, alts)
  alts <- c(grep("literature", more_alts, value=TRUE, invert=TRUE), alts)
  
  if (length(unique(df2$ref_case_def))==1){
    # some causes don't have matches for alt to alt, don't do network analysis here
    for (i in alts) df2[, i] <- ifelse(df2$alt_case_def == i, 1, ifelse(df2$ref_case_def == i, -1, 0))
    print_log_message("not doing a network analysis")
    
  } else {
    print_log_message("doing a network analysis")
    for (i in alts) df2[, i] <- 0
    for (i in alts) df2[, i] <- df2[, i] - sapply(i, grepl, df2$ref_case_def)
    for (i in alts) df2[, i] <- df2[, i] + sapply(i, grepl, df2$alt_case_def)
  }
  
  df2 <- data.table(df2, key="nid_x,nid_y")
  df2[, study_id:=.GRP, by=key(df2)]
  
  check_newvars <- lapply(alts, function(x) table(df2[, ..x]) )
  names(check_newvars) <- alts
  check_newvars
  
  cols <- c("diff_logit", "se_diff_logit", "ref_case_def", "alt_case_def","study_id", alts)
  
  df2 <- df2[, ..cols]
  df2$intercept <- 1
  
  return(list(df2 = df2, alts = alts))
}


get_original_data <- function(b, alts){
  # get original data and logit transform
  df <- read_in_data(bundle=b, alts=alts, date = "<insert_date_here>")
  df[,`:=` (mean_logit = numeric(), se_logit = numeric())]
  
  # separate out mean = 0, gold standard cols
  z_df <- df[mean == 0]
  ref_df <- df[gold_standard == 1]
  df <- df[mean !=0 & gold_standard != 1]
  
  df[, `:=` (standard_error = as.numeric(standard_error), mean = as.numeric(mean))]
  
  # need mean and se in logit space
  df[, mean_logit := logit(mean)]
  df$se_logit <- sapply(1:nrow(df), function(i) {
    mean_i <- df[i, mean]
    mean_se_i <- df[i, standard_error]
    deltamethod(~exp(x1)/(1+exp(x1)), mean_i, mean_se_i^2)
  })
  
  df <- rbind(df, z_df, ref_df)
  return(df)
  
}


############################################################################################
# mr brt

run_model <- function(df, alts){
  cov_list <- lapply(alts, function(x) cov_info(x, "X"))
  
  model <- run_mr_brt(
    output_dir = out_dir,
    model_label = MODEL_LABEL,
    data = df,
    mean_var = "diff_logit",
    se_var = "se_diff_logit",
    covs = cov_list,
    remove_x_intercept = TRUE,
    method = "trim_maxL",
    trim_pct = 0.1,
    study_id = "study_id",
    overwrite_previous = TRUE
  )
  return(model)
  
}





###############################################################################
# run and apply the crosswalk


run_and_apply_crosswalk <- function(bundle){
  
  df <- read.csv("FILEPATH")
  
  objs <- prep_data(df, se_trim)
  data <- objs$df2
  alts <- objs$alts
  print_log_message("started running model")
  model <- run_model(data, alts)
  print_log_message("finished running model")
  
  # plot the fit
  print_log_message("making funnel plots")
  plot <- plot_mr_brt_custom(model)
  
  # apply them
  
  check_for_outputs(model)
  pred_frame <- as.data.frame(diag(length(alts)))
  names(pred_frame) <- alts
  pred1 <- predict_mr_brt(model, newdata = pred_frame)
  check_for_preds(pred1)
  preds <- as.data.table(pred1$model_summaries)
  
  
  print_log_message("making forest plots")
  tmp_orig <- get_original_data(bundle, alts)
  
  print_log_message("applying crosswalk")
  dfs <- list()
  for (alt in alts){
    
    # subset to only rows with alternate def
    tmp_orig2 <- tmp_orig[get(alt)==1,]
    preds2 <- preds[get(paste0("X_", alt))==1,]
    
    adj_logit <- preds2$Y_mean
    
    # note: compared to the metafor version, this interval already
    #       incorporates between-study heterogeneity
    se_adj_logit <- (preds2$Y_mean_hi - preds2$Y_mean_lo) / 3.92
    
    
    # new variance of the adjusted data point is just the sum of variances
    # because the adjustment is a sum rather than a product in logit space
    tmp_orig3 <- tmp_orig2 %>%
      mutate(
        mean_logit_tmp = mean_logit - adj_logit, # adjust the mean estimate. We subtract because the MR-BRT prediction is interpreted as logit(prev_alt) minus logit(prev_ref)
        var_logit_tmp = se_logit^2 + se_adj_logit^2, # adjust the variance
        se_logit_tmp = sqrt(var_logit_tmp)
      )
    
    
    # use adjusted mean
    tmp_orig4 <- tmp_orig3 %>%
      mutate(
        mean_logit_adjusted = mean_logit_tmp,
        se_logit_adjusted = se_logit_tmp,
        lo_logit_adjusted = mean_logit_adjusted - 1.96 * se_logit_adjusted,
        hi_logit_adjusted = mean_logit_adjusted + 1.96 * se_logit_adjusted,
        mean_adjusted = inv.logit(mean_logit_adjusted),
        lo_adjusted = inv.logit(lo_logit_adjusted),
        hi_adjusted = inv.logit(hi_logit_adjusted) )
    
    tmp_orig4$se_adjusted <- sapply(1:nrow(tmp_orig4), function(i) {
      mean_i <- tmp_orig4[i, "mean_logit_adjusted"]
      mean_se_i <- tmp_orig4[i, "se_logit_adjusted"]
      deltamethod(~exp(x1)/(1+exp(x1)), mean_i, mean_se_i^2)
    })
    
    tmp_orig4 <- as.data.table(tmp_orig4)
    
    dfs[[alt]] <- tmp_orig4
  }
  
  dfs <- rbindlist(dfs)
  
  # final df has reference and alternates
  dfs <- rbind.fill(dfs, tmp_orig[tmp_orig$gold_standard==1,])
  dfs <- as.data.table(dfs)
  
  # save a copy to vet
  write.csv(dfs, file = "FILEPATH", row.names=FALSE)
  
  # use adjusted mean for alt data points
  dfs[, standard_error := as.numeric(standard_error)]
  dfs[mean_adjusted != 0 & !(is.na(mean_adjusted)), `:=` (mean = mean_adjusted,
                                                          lower = lo_adjusted, upper = hi_adjusted,
                                                          standard_error = se_adjusted)]
  dfs[mean == 0, standard_error := sqrt(standard_error^2 + se_adjusted^2)]
  dfs[,c("gold_standard", "mean_logit", "se_logit","mean_logit_tmp",
         "var_logit_tmp",	"se_logit_tmp","mean_logit_adjusted",	"se_logit_adjusted",
         "lo_logit_adjusted", "hi_logit_adjusted",	"mean_adjusted",	"lo_adjusted",
         "hi_adjusted","se_adjusted") := NULL]
  
  
  dfs[is.na(dfs)] <- ""
  
  print_log_message("Writing adjusted data to df!!!")
  write.xlsx(dfs, file = "FILEPATH", row.names=FALSE, sheetName = "extraction")
  
}



###################################################################################
# actually run it 

bundles <- c("<insert_bundles_here>")

for (bundle in bundles){
  
  print_log_message(paste0("working on ", bundle))
  out_dir <- "FILEPATH"
  dir.create(out_dir, showWarnings = F, recursive=T)
  
  tryCatch(run_and_apply_crosswalk(bundle = bundle), error = function(e)
  {print_log_message(paste0(bundle, " crosswalk failed"))})
  
}

