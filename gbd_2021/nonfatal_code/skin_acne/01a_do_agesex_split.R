rm(list=ls())

args <- commandArgs(trailingOnly = TRUE)
BUN_ID <- args[1]
DESCRIPTION <- args[2]

print(args)

os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  k <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- paste0("FILEPATH")
  k <- "FILEPATH"
}

library('openxlsx')
library('tidyr')
library('dplyr')
library('data.table')
library('matrixStats')
library('testit', lib.loc=paste0(h, "R/3.5/"))

source(paste0("FILEPATH"))
source(paste0("FILEPATH"))
source(paste0("FILEPATH"))
source(paste0("FILEPATH"))
source(paste0("FILEPATH"))
source(paste0("FILEPATH"))
source(paste0("FILEPATH"))
source(paste0("FILEPATH")
source(paste0("FILEPATH"))

source(paste0("FILEPATH"))
source(paste0("FILEPATH"))

SKIN_DIR <- paste0("FILEPATH")
SAVE_DIR <- paste0("FILEPATH")

MAP <- as.data.table(read.xlsx(paste0("FILEPATH"))
ME_ID <- MAP[bundle == BUN_ID, me_id]
AGE_MAP <- fread("FILEPATH")

age_sex_split <- function(bundle, measure_name){

  bundle_df <- fread(paste0("FILEPATH"))

  bun_data <- get_bundle_version(bundle_version_id=bundle_df[,bundle_version], fetch="all", export=TRUE)
  
  if (bundle==245){
    bun_data[, age_demographer := NA]
  }
  if (!("group_review" %in% names(bun_data))){
    bun_data[, group_review := 1]
  }
  bun_data[is.na(group_review), group_review:=1]
  group_review <- bun_data[group_review==0,]
  df <- pull_bundle_data(measure_name = measure_name, bun_id = bundle, bun_data = bun_data)

  data <- divide_data(input_data = df)
  good_data <- data[need_split == 0]
  aggregate <- fsetdiff(data, good_data, all = TRUE)
  aggregate <- aggregate[need_split == 1,]
  
  if (nrow(aggregate)==0) { 
    print_log_message(paste("Bundle",bundle,"does not contain any", measure_name,"data that needs to be split"))
  } else {
    print_log_message(paste("Running age/sex splitting!"))
  
  expanded <- expand_test_data(agg.test = aggregate, bun_id = bundle)
  
  if ("age_group_id" %in% names(expanded) == TRUE) {
    expanded$age_group_id <- NULL
  }
  
  expanded <- add_pops(expanded, bun_id = bundle, age_map = AGE_MAP)
  print_log_message("Loaded populations")
  
  expanded[year_id%%5 < 3, est_year_id := year_id - year_id%%5]
  expanded[year_id%%5 >= 3, est_year_id := year_id - year_id%%5 + 5]
  expanded[est_year_id < 1990, est_year_id := 1990]
  expanded[year_id == 2020, est_year_id := 2019]
  expanded[nid==431674 & year_id==2018, est_year_id := 2019]
  
  weight_draws <- pull_model_weights(model_id = ME_ID, measure_name = measure_name, expanded = expanded)
  print_log_message("Pulled DisMod results")
  
  draws <- merge(expanded, weight_draws, by = c("age_group_id", "sex_id", "location_id", "est_year_id"), 
                 all.x=TRUE)
  
  draws <- melt.data.table(draws, id.vars = names(draws)[!grepl("draw", names(draws))], measure.vars = patterns("draw"),
                           variable.name = "draw.id", value.name = "model.result")
  
  draws[, numerator := model.result * population]
  draws[, denominator := sum(numerator), by = .(split.id, draw.id)]
  
  draws[, estimate := (mean * model.result / denominator) * pop.sum]
  draws[, sample_size_new := sample_size * population / pop.sum]
  
  draws[numerator == 0 & denominator == 0, estimate := 0]
  
  final <- draws[, .(mean.est = mean(estimate),
                     sd.est = sd(estimate),
                     upr.est = quantile(estimate, .975),
                     lwr.est = quantile(estimate, .025),
                     sample_size_new = unique(sample_size_new),
                     cases.est = mean(numerator),
                     agg.cases = mean(denominator)), by = expand.id] %>% merge(expanded, by = "expand.id")
  
  z <- qnorm(0.975)
  final[sd.est == 0 & mean.est == 0 & measure == "prevalence", 
        sd.est := (1/(1+z^2/sample_size_new)) * sqrt(mean.est*(1-mean.est)/sample_size_new + z^2/(4*sample_size_new^2))]
  final[, se.est := sd.est]
  final[, agg.sample.size := sample_size]
  final[, sample_size := sample_size_new]
  final[,sample_size_new:=NULL]
  
  final[mean==0, mean.est := 0]
  final[, case_weight := cases.est / agg.cases]
  final$agg.cases <- NULL
  final[, crosswalk_parent_seq := seq]
  final[, seq := NA]

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
            file = paste0("FILEPATH"),
            row.names = FALSE)
  
  good_data[, crosswalk_parent_seq := numeric()]
  aggregate[, crosswalk_parent_seq := numeric()]
  
  full_bundle <- rbind(good_data, aggregate)
  
  full_bundle[, c("age_start", "age_end") := NULL]
  setnames(full_bundle, c("orig_age_start", "orig_age_end"), c("age_start", "age_end"))
  
  final[, c("mean", "standard_error", "cases") := NULL]
  setnames(final, c("mean.est", "se.est"), c("mean", "standard_error"))
  
  final[, sex := ifelse(sex_id == 1, "Male", "Female")]
  final[, `:=` (lower = lwr.est, upper = upr.est,
                cases = NA, effective_sample_size = NA)]
  
  final <- final[,names(full_bundle),with = FALSE]
  final[, need_split := 0]
  full_bundle <- rbind(full_bundle, final)
  
  group_review[, crosswalk_parent_seq := numeric()]
  
  full_bundle[need_split==1, group_review := 0]
  
  full_bundle[, c("sex_id", "need_split") := NULL]
  full_bundle <- rbind(full_bundle, group_review)
  
  bun_data[, crosswalk_parent_seq := numeric()]
  bun_data <- bun_data[,names(full_bundle),with = FALSE]
  
  df[,crosswalk_parent_seq := numeric()]
  df <- df[, names(full_bundle), with=FALSE]
  
  dropped <- fsetdiff(df[, !names(df) %in% c("cases", "sample_size", "group_review"), with=FALSE], full_bundle[, !names(full_bundle) %in% c("cases", "sample_size", "group_review"), with=FALSE])
  assert("some data was dropped", nrow(dropped) == 0)
  
  full_bundle[is.na(full_bundle)] <- " "
  full_bundle <- full_bundle[order(seq)]
  bad_nids <- unique(full_bundle[mean>1, nid])
  if (bundle==240){
    bad_nids <- bad_nids[!(bad_nids %in% 331156)]
    full_bundle <- full_bundle[!(nid==331156 & age_start == 15)]
  }
  full_bundle <- full_bundle[!(nid %in% bad_nids)]
  dir.create(paste0("FILEPATH"), recursive = TRUE, showWarnings = FALSE)
  
  print_log_message(paste0("FILEPATH"))  
  print_log_message("writing all the data!")
  write.csv(full_bundle, 
            file = paste0("FILEPATH"),
            row.names = FALSE)
  }
}

for (measure_name in c("prevalence", "incidence", "mtexcess")){
  print_log_message(paste("working on", measure_name))
  age_sex_split(bundle=BUN_ID, measure_name=measure_name)
}
