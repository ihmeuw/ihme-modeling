rm(list = ls())

library(data.table)
library(magrittr)
library(parallel)
library(stringr)

args <- commandArgs(trailingOnly = TRUE)
param_map_filepath <- args[1]

task_id <- ifelse(is.na(as.integer(Sys.getenv("SGE_TASK_ID"))), 1, as.integer(Sys.getenv("SGE_TASK_ID")))
param_map <- fread(param_map_filepath)
  
location_id <- param_map[task_id, location_id]
v <- param_map[task_id, version]

if(v == "airpol_shifted"){
  copula_dir = paste0("FILEPATH")
} else{
  copula_dir = paste0("FILEPATH")
}

print(paste("Loc:", location_id, "version", v))

source("FILEPATH/find_non_draw_cols.R")

get_other_cols <- function(data, toMatch = c("ga", "bw")){
  microdata_ids <- names(data)[!grepl(paste(toMatch,collapse="|"), names(data))]  
  return(microdata_ids)
}

get_data <- function(loc){ 

  input_file <- paste0(copula_dir, loc, ".rds")
  ga_bw_microdata <- readRDS(input_file)
  ga_bw_microdata[, draw := as.integer(gsub(x = draw, pattern = "draw_", replacement = ""))]
  ga_bw_microdata[, location_id := loc]

  return(ga_bw_microdata)
  
}


collapse_microdata_to_prevalence <- function(microdata){
  
  prevalence <- microdata[, .(ga_cat = cut(ga, breaks = ga_break_points, labels = ga_labels, include.lowest = T, right = F),
                              bw_cat = cut(bw, breaks = bw_break_points, labels = bw_labels, include.lowest = T, right = F)), 
                              by = microdata_ids][, .(total_rows = .N), by = c("ga_cat", "bw_cat", microdata_ids)][,
                                                    .(baseline_exposure = total_rows / sum(total_rows), ga_cat, bw_cat), by = c(microdata_ids)]
  
  return(prevalence)
}

merge_on_me_map <- function(data){
  
  data.list <- split(data, by = c("location_id", "sex_id", "age_group_id", "year_id", "draw"))
  data.list <- lapply(data.list, merge, me_map, by = c("ga_cat", "bw_cat"), all = T)
  data.list <- lapply(data.list, fill_missing_id_values)
  data <- rbindlist(data.list, use.names = T, fill = T)
  data[is.na(baseline_exposure), baseline_exposure := 0]
  data[is.na(modelable_entity_id), modelable_entity_id := 0]
  
  return(data)
  
}

fill_missing_id_values <- function(data){
  
  ids <- get_other_cols(data, toMatch = c("ga", "bw", "baseline_exposure", "parameter", "modelable_"))
  id.vals <- data[!is.na(draw), ids, with = F] %>% unique
  data <- cbind(id.vals, data[, -ids, with = F])
  
  return(data)
  
}

merge_on_age_specific_information <- function(data, age_group_id){
  
  data <- merge_on_mortality_information(data, mort, age_group_id = age_group_id)
  data <- merge_on_rr_information(data, age_group_id = age_group_id)
  
  return(data)
  
}


move_draw_num_to_end <- function(x, age){
  
  no_age_id <- gsub(x, pattern = paste0("_", age), replacement = "")
  final_underscore_position <- max(unlist(gregexpr(no_age_id, pattern = "_")))
  prefix <- strtrim(no_age_id, width = final_underscore_position)  
  draw_num <- gsub(no_age_id, pattern = prefix, replacement = "")  
  replacement_name <- paste0(prefix, paste0(age, "_"), draw_num)
  
  return(replacement_name)
  
}


get_mortality_information <- function(loc_id){

  mort <- fread("FILEPATH") 
  mort <- mort[location_id == loc_id, ]
  mort[, location_id := loc_id]
  id_cols <- find_non_draw_cols(mort, "_enn|_lnn")
  
  cleaned_names <- lapply(names(mort), function(x){
    
    if(x %in% id_cols){
      return(x)
    } else {
      age <- str_sub(x, -3)
      cleaned <- move_draw_num_to_end(x, age)
      return(cleaned)
    }
    
  }) %>% unlist
  
  names(mort) <- cleaned_names
  mort <- melt(mort, id.vars = c("location_id", "year_id", "sex_id"), variable.name = "draw", variable.factor = F,
               value.name = c("births", "pop_surv_enn", "pop_surv_lnn", "qx_enn", "qx_lnn", "ly_enn", "ly_lnn", "mort_rate_enn", "mort_rate_lnn"),
               measure = patterns("births_enn_", "pop_survive_enn_", "pop_survive_lnn_", "qx_enn_", "qx_lnn_", "life_years_enn_", "life_years_lnn_", "mort_rate_enn_", "mort_rate_lnn_"))
  
  
  mort[, pop_deaths_enn := births - pop_surv_enn]
  mort[, pop_deaths_lnn := pop_surv_enn - pop_surv_lnn]
  mort <- melt(mort, id.vars = c("location_id", "year_id", "sex_id", "draw", "births"), variable.name = "age_group_id", variable.factor = F,
               value.name = c("pop_surv", "pop_deaths", "qx", "ly", "mort_rate"),
               measure = patterns("pop_surv_", "deaths_", "qx_", "ly_", "mort_rate_"))
  
  setnames(mort, c("births", "qx", "ly", "mort_rate"), c("pop_baseline", "pop_qx", "pop_ly", "pop_mort_rate"))
  
  
  mort[, draw := as.integer(draw)][, age_group_id := as.integer(age_group_id)]
  mort[draw == 1000, draw := 0]
  mort[age_group_id == 2, age_group_id := 3]
  mort[age_group_id == 1, age_group_id := 2]
  mort[age_group_id == 3, pop_baseline := mort[age_group_id == 2, pop_surv]]
  
  return(mort)
  

}

merge_on_mortality_information <- function(data, mort, age_group_id){
  
  mort <- mort[age_group_id == age_group_id, ]
  data <- merge(data, mort, by = c("location_id", "year_id", "sex_id", "age_group_id", "draw"), all.x = T)
  
  return(data)
  
}

merge_on_rr_information <- function(data, age_group_id = NULL){
  
  rrs_filepaths <- list.files("FILEPATH", full.names = T, include.dirs = F)
  rrs_filepaths <- rrs_filepaths[grepl(pattern = ".csv", x = rrs_filepaths)]

  rrs <- lapply(rrs_filepaths, function(x) {
    
    dt <- fread(x)
    return(dt)
    
  }) %>% rbindlist(fill = T, use.names = T)
  
  rrs <- rrs[cause_id == 686]
  rr_id_vars <- find_non_draw_cols(rrs, "draw") 
  rrs <- melt(rrs, id.vars = rr_id_vars, variable.name = "draw", value.name = "rr")
  rrs[, draw := gsub(pattern = "draw_", x = draw, replacement = "")]
  rrs[, draw := as.integer(draw)]
  
  data <- merge(data, rrs[, list(age_group_id, year_id, parameter, draw, sex_id, rr)], by = c("age_group_id", "parameter", "draw", "sex_id", "year_id"), all.x = T)
  data[is.na(rr), rr := 0]
  
  return(data)
  
  
}

distribute_impossible_exposure_to_bins <- function(data){
  
  ids_and_ga <- get_other_cols(data, toMatch = c("bw", "baseline_exposure", "parameter", "modelable_", "rr"))
  add_exp_key <- data[rr == 0, list(additional_exp = sum(baseline_exposure)), by = ids_and_ga]
  add_exp_key <- merge(data[rr != 0, list(bw = max(bw)), by = ids_and_ga], add_exp_key, by = ids_and_ga, all = T) 
  add_exp_key[is.na(bw), bw := 4500]

  data <- merge(data, add_exp_key, by = c(ids_and_ga, "bw") , all = T)
  data[!is.na(additional_exp), baseline_exposure := baseline_exposure + additional_exp]
  data[is.na(additional_exp) & rr == 0, baseline_exposure := 0]
  data <- data[, -c("additional_exp")]
  
  return(data)
  
  
}


age_cohort <- function(data, age_group_id = NULL){
  
  
  if(age_group_id == 2){
    data[, ax := ((0.6 + 0.4*3.5) / 365)]
    data[, years_in_age_period := 7/365]
    
  } else if(age_group_id == 3){
    
    data[, ax := (7) / 365]
    data[, years_in_age_period := (28 - 7) / 365]
    
  } else{
    stop("No age group specified.")
  }
  
  calculate_baseline_per_category(data)
  
  calculate_mort_rate_per_category(data)
  
  calculate_qx_per_category(data)
  
  calculate_life_years_per_category(data)
  
  calculate_deaths_per_category(data)
  
  data <- scale_deaths_per_category(data)
  
  calculate_survivors_per_category(data) 
  
  calculate_final_exposure_per_category(data)
  
  calculate_period_prevalence(data)
  
  calculate_new_mort_rate_per_category(data)
  
  return(data)
  
  
  
}

calculate_baseline_per_category <- function(data) {
  
  data[, categ_baseline := pop_baseline * baseline_exposure]
  
}

calculate_mort_rate_per_category <- function(data) {
  
  data[,  sum_prod_rr_exp := sum(rr * baseline_exposure), by = microdata_ids]
  data[,  tmrel_mort_rate := pop_mort_rate/sum_prod_rr_exp, by = microdata_ids]
  data[, categ_mort_rate := rr * pop_mort_rate / sum_prod_rr_exp, by = microdata_ids]
  
}

calculate_qx_per_category <- function(data){
  
  data[, categ_qx := rr * pop_qx / sum_prod_rr_exp, by = microdata_ids]
}

calculate_life_years_per_category <- function(data) {
  
  data[, categ_life_years := categ_baseline * ( ax * categ_qx ) + categ_baseline * (years_in_age_period) * (1 - categ_qx)]
  data[categ_life_years < 0, categ_life_years := 0]
  
}

calculate_deaths_per_category <- function(data) {
  
  data[, categ_deaths := categ_mort_rate * categ_life_years]
  data[categ_deaths < 0 | (categ_life_years == 0 & categ_qx != 0), categ_deaths := categ_baseline] ## life years where all people die in category is 0. this causes an issue in the "scaling of deaths" so need to edit that function too
  
  
  
}

scale_deaths_per_category <- function(data) {
  
  data[, categ_deaths_unscaled := categ_deaths ]
  
  pop_scaling_key <- data[(categ_deaths == categ_baseline), ]
  pop_scaling_key <- pop_scaling_key[, list(pop_minus_non_scaling_deaths = pop_deaths - sum(categ_deaths)) , by = microdata_ids] %>% unique
  
  
  scaling_factor_key <- data[!(categ_deaths == categ_baseline), ]
  scaling_factor_key <- scaling_factor_key[, list(scaling_factor = categ_deaths / sum(categ_deaths), ga_bw), by = microdata_ids] 
  
  data <- merge(scaling_factor_key, data, by = c(microdata_ids, "ga_bw"), all = T)
  data <- merge(pop_scaling_key, data, by = c(microdata_ids), all = T)
  
  data <- data[!is.na(scaling_factor), categ_deaths := pop_minus_non_scaling_deaths * scaling_factor , by = microdata_ids]
  
  data <- data[, -c("pop_minus_non_scaling_deaths", "scaling_factor")]
  
  return(data)
  
}

calculate_survivors_per_category <- function(data) {
  
  data[, categ_surv := categ_baseline - categ_deaths]
  
  if(v == "airpol_shifted"){
    
    data[categ_surv < 0 , categ_surv := 0]

  } else{
  
    print(paste("age_group_id:", data$age_group_id %>% unique, "categories with negative # of survivors", data[categ_surv < 0, .N]))
      
  }
  
  
  
}

calculate_final_exposure_per_category <- function(data) {
  
  data[, final_exposure_compare := categ_surv / pop_surv , by = microdata_ids]
  data[, final_exposure := categ_surv / sum(categ_surv) , by = microdata_ids]
   
}

calculate_period_prevalence <- function(data) {
  
  data[, period_prevalence := categ_life_years / pop_ly, by = microdata_ids]
  
}


change_to_new_baselines <- function(data){
  
  data[, baseline_exposure := final_exposure]
  
}

calculate_new_mort_rate_per_category <- function(data) {
  
  data[, categ_mort_rate_post_scale := (categ_deaths / categ_life_years) ]  
  data[, rr_post_scale := categ_mort_rate_post_scale / min(categ_mort_rate_post_scale, na.rm = T) , by = microdata_ids]
  
}


vet_age_cohort <- function(data){
  
  print( paste( "Number of bins with less than 0 survivors",
                data[categ_surv < 0, .N] ))
  
  data <- data[, .(pop_surv = unique(pop_surv), categ_surv_summed = sum(categ_surv), 
                   pop_deaths = unique(pop_deaths), categ_deaths_summed = sum(categ_deaths),
                   pop_ly = unique(pop_ly), categ_ly_summed = sum(categ_life_years)), by = microdata_ids]
  
  data[, surv_check := round(pop_surv / categ_surv_summed, 2) ][, deaths_check := round(pop_deaths / categ_deaths_summed, 2)][, ly_check := round(pop_ly / categ_ly_summed, 2)]
  
  data[surv_check == 1.01, surv_check := 1][deaths_check == 1.01, deaths_check := 1][ly_check == 1.01, ly_check := 1]
  
  print( paste("Number of inconsistencies between population and summed bins =",
               data[(surv_check != 1) | (deaths_check != 1) | (ly_check != 1), .N]))
  
  num_inconsistencies <- data[(surv_check != 1) | (deaths_check != 1) | (ly_check != 1), .N]
  
  if (num_inconsistencies != 0){
    vet_file <- data.table(message = paste("Number of inconsistencies between population and summed bins =",
                                           data[(surv_check != 1) | (deaths_check != 1) | (ly_check != 1), .N]),
                           range_surv_check = range(data$surv_check),
                           range_deaths_check = range(data$deaths_check),
                           range_ly_check = range(data$ly_check))
    
    write.csv(vet_file, "FILEPATH", row.names = F, na = "")
    
  }
  
}

vet_dimensions <- function(dt){
  
  num_ages = dt[, age_group_id] %>% unique %>% length # 2
  num_draws = dt[, draw] %>% unique %>% length # 1000
  num_sexes = dt[, sex_id] %>% unique %>% length # 1
  num_locs = dt[, location_id] %>% unique %>% length # 1
  num_years = dt[, year_id] %>% unique %>% length # 3
  num_ga_cats = dt[, ga_cat] %>% unique %>% length # 12
  num_bw_cats = dt[, bw_cat] %>% unique %>% length # 11
  num_me_ids = dt[, modelable_entity_id] %>% unique %>% length # 11
  
  if(num_ages != 2 | 
     num_draws != 100 | 
     num_sexes != 2 |
     num_locs != 1 |
     num_years != 8 |
     num_ga_cats != 12 |
     num_bw_cats != 11 | 
     num_me_ids != 59){
        
    vet_file <- data.table(n2_ages = num_ages, n100_draws = num_draws, n2_sexes = num_sexes, n1_locs = num_locs, 
                           n8_years = num_years, n12_ga_cats = num_ga_cats, n11_bw_cats = num_bw_cats, n59_me_ids = num_me_ids)
    
    write.csv(vet_file, "FILEPATH", row.names = F, na = "")
    
  }
  
}









# --------------------------
# LOAD ME_MAP & MORT INFORMATION

me_map <- fread("FILEPATH")

me_map <- me_map[, list(modelable_entity_name, ga_cat, bw_cat, ga, bw, parameter, modelable_entity_id, ga_bw)]

ga_labels <- me_map$ga_cat %>% unique ## 12 labels
ga_labels <- sort(ga_labels)

bw_labels <- me_map$bw_cat %>% unique ## 11 labels 
bw_labels <- sort(bw_labels)

ga_break_points <- c("-Inf", "24", "26", "28", "30", "32", "34", "36", "37", "38", "40", "42", "Inf")

bw_break_points <- c("-Inf", "500", "1000", "1500", "2000", "2500", "3000", "3500", "4000", "4500", "5000", "Inf")


mort <- get_mortality_information(location_id)


print("loaded mort")

# --------------------------
# LOAD COPULA MICRODATA

ga_bw_microdata <- get_data(loc = location_id)


print("loaded copula")


# --------------------------
# COLLAPSE MICRODATA INTO PREVALENCE


microdata_ids <- get_other_cols(ga_bw_microdata, toMatch = c("ga", "bw"))


ga_bw_prevalence <- collapse_microdata_to_prevalence(ga_bw_microdata)

print("collapsed to prev")


# --------------------------
# COMBINE PREVALENCE DATA WITH OTHER INPUTS (mortality + RRs)

ga_bw_prevalence[, age_group_id := 2]

ga_bw_prevalence <- merge_on_me_map(ga_bw_prevalence)

ga_bw_prevalence <- merge_on_age_specific_information(ga_bw_prevalence, age_group_id = 2)

ga_bw_prevalence <- distribute_impossible_exposure_to_bins(ga_bw_prevalence)

print("dataprep done")


# --------------------------
# ANALYSIS: AGE THROUGH EARLY & LATE NEONATAL PERIOD (AGE_GROUP_ID = 2)

ga_bw_prevalence <- age_cohort(ga_bw_prevalence, age_group_id = 2)

store_age_group_2 <- copy(ga_bw_prevalence)

print("aged early neonatal")

ga_bw_prevalence[, age_group_id := 3]
ga_bw_prevalence <- ga_bw_prevalence[, list(location_id, sex_id, age_group_id, year_id, draw, ga_bw, ga_cat, ga, bw_cat, bw, modelable_entity_name, modelable_entity_id, parameter, final_exposure)]
ga_bw_prevalence <- merge_on_age_specific_information(ga_bw_prevalence, age_group_id = 3)
change_to_new_baselines(ga_bw_prevalence)

print("prepared for next age group")

ga_bw_prevalence <- age_cohort(ga_bw_prevalence, age_group_id = 3)

print("aged late neonatal")

aged_cohort <- rbindlist(list(store_age_group_2, ga_bw_prevalence), use.names = T, fill = T)

print("bound early and late neonatal")

setkeyv(aged_cohort, microdata_ids)

print(vet_age_cohort(aged_cohort))

print("aged_cohort complete")

vet_dimensions(aged_cohort)

saveRDS(aged_cohort, paste0("FILEPATH", location_id, ".rds"))

print("saved and done!")

print(str(aged_cohort))