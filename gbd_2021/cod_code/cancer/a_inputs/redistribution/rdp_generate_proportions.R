##################################################################################
# Name of Script: rdp_generate_proportions.r
# Description: Generate rdp proportions for rdp pckages that redistribute onto
#              multiple causes
# More info found on HUB page: 
# FILEPATH
# Arguments: N/A
# Output: rdp proportions for inc by super region for each age, sex, cause
#         rdp proportions for mor globally for each age, sex, cause
# Contributors: USERNAME
###################################################################################

rm(list=ls())

# set working directories by operating system
user <- Sys.info()[["user"]]
if (Sys.info()["sysname"]=="Linux") {
  j_root <- "/home/j" 
  h_root <- file.path("/homes", user)
} else if (Sys.info()["sysname"]=="Darwin") {
  j_root <- "~/J"
  h_root <- "~/H"
} else { 
  j_root <- "J:"
  h_root <- "H:"
}

# load in necessary tables and filepaths
library(dplyr); library(data.table); library(openxlsx); library(feather) 
library(assertthat); library(tidyr); library(readstata13)

source("FILEPATH")

locs <- get_location_metadata(gbd_round_id = 7, location_set_id = 35)
registry <- fread("FILEPATH")
dataset <- fread("FILEPATH")
iccc_datasets <- dataset[coding_system_id %in% c(3, 5, 11)]
iccc_datasets <- iccc_datasets$dataset_id %>% unique

get_dt_by_metric <- function(metric_type){
  # get active data
  if(metric_type == "inc"){
    # gold standard datasets
    dts <- c(39, 40, 41, 244, 260, 390, 391, 221, 124, 261, 103, 666)
  } else if(metric_type == "mor"){
    # our active datasets
    dts <- c(20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 36, 39, 40, 41, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 
             56, 57, 58, 59, 60, 65, 66, 67, 68, 69, 70, 71, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 
             93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 110, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 
             122, 123, 124, 125, 126, 127, 128, 130, 136, 140, 149, 151, 152, 157, 160, 165, 168, 171, 172, 173, 175, 179, 184, 185, 186, 
             187, 188, 189, 192, 193, 195, 196, 198, 200, 201, 204, 205, 206, 208, 209, 210, 211, 212, 213, 215, 217, 218, 219, 221, 223, 224, 
             225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 238, 240, 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 251, 
             252, 253, 254, 255, 256, 257, 258, 259, 260, 261, 262, 263, 265, 266, 267, 268, 269, 271, 272, 274, 275, 276, 277, 278, 279, 280, 
             281, 282, 283, 284, 285, 286, 287, 288, 289, 290, 291, 292, 293, 294, 295, 296, 298, 304, 305, 306, 308, 309, 310, 311, 314, 315, 
             316, 317, 318, 319, 320, 321, 322, 323, 324, 325, 326, 327, 328, 329, 330, 331, 332, 333, 334, 335, 336, 337, 338, 339, 340, 341, 
             342, 343, 344, 345, 346, 347, 348, 349, 352, 353, 354, 355, 356, 357, 358, 359, 360, 361, 362, 363, 364, 365, 366, 367, 368, 369, 
             370, 371, 372, 373, 374, 375, 376, 377, 378, 379, 380, 381, 382, 383, 384, 385, 386, 387, 388, 389, 390, 391, 392, 393, 394, 395, 
             396, 398, 399, 400, 401, 402, 403, 404, 405, 407, 408, 409, 410, 411, 412, 413, 414, 416, 417, 418, 419, 424, 425, 426, 427, 428, 
             429, 430, 431, 432, 433, 434, 435, 436, 437, 438, 439, 440, 441, 442, 443, 445, 446, 447, 449, 451, 452, 453, 454, 455, 456, 458, 
             460, 464, 468, 469, 473, 475, 476, 477, 478, 479, 482, 483, 485, 486, 488, 489, 490, 494, 495, 496, 498, 499, 500, 502, 503, 504, 
             505, 506, 509, 513, 515, 516, 517, 518, 519, 525, 527, 544, 547, 562, 665, 666, 667, 802, 803, 804, 805)
  }
  return(dts)
}

clean_input_data <- function(metric_type){
  prep5 <- fread(paste0("FILEPATH"))
  prep5 <- prep5[dataset_id %in% get_dt_by_metric(metric_type)]
  
  if(metric_type == "inc"){
    # handling for special garbage codes
    all_gc = c('C91.9', 'C91.7', 'C91.8', 'C91.5', 'C91.4')
    aml_gc = c('C92.7', 'C93.9', 'C92.9', 'C93.5', 'C92.8', 'C93.2', 'C93.7')
    cll_gc = c('C91.1')
    
    prep5[acause == "C91.1" & age > 9, acause := "neo_leukemia_ll_chronic"]
    prep5[acause == "C91.1" & age <= 9, acause := "neo_leukemia_ll_acute"]
    
    prep5[acause %in% aml_gc & age > 9, acause := "neo_leukemia_other"]
    prep5[acause %in% aml_gc & age <= 9, acause := "neo_leukemia_ml_acute"]
    
    prep5[acause %in% all_gc & age > 9, acause := "neo_leukemia_other"]
    prep5[acause %in% all_gc & age <= 9, acause := "neo_leukemia_ll_acute"]
    
    prep5[acause == "neo_ben_brain", acause := "neo_ben_other"]
    prep5[acause == "C69", acause := "neo_eye"]
    
  } else if (metric_type == "mor"){
    prep5[acause == "C91.1", acause := "neo_leukemia_ll_chronic"]
  }
  
  parent_causes <- c("neo_lymphoma", "neo_eye", "neo_liver", "neo_leukemia")
  
  # aggregate leukemia subtypes + parent to get new parent, eye parent, NHL parent, bcc/scc
  aggregate_subtypes <- function(parent_cause, input_df, uid_cols, metric){
    i <- 1
    totals <- list()
    for(system in c("ICD9_detail", "ICD10")){
      subset_df <- input_df[coding_system == system]
      cur_dt_id <- input_df$dataset_id %>% unique
      if(parent_cause == "neo_leukemia" & system == "ICD10" & 1==2){
        subtypes <-subset_df[grepl(parent_cause, acause)]
      } else if(parent_cause == "neo_liver"){
        subtypes <- subset_df[dataset_id %in% iccc_datasets & grepl(parent_cause, acause)]
      } else{
        subtypes <- subset_df[grepl(parent_cause, acause)]
      }
      agg_df <- subtypes[, lapply(.SD, FUN = sum), .SDcols = c(metric), by = c(uid_cols)]
      agg_df$acause <- parent_cause
      subtypes <- subtypes[acause != parent_cause]
      liver_orig <- subset_df[(dataset_id %in% iccc_datasets & acause != parent_cause) | (!(dataset_id %in% iccc_datasets) & grepl(parent_cause, acause))]
      if(parent_cause == "neo_liver") total_df <- rbindlist(list(liver_orig, agg_df), use.names=TRUE)
      else total_df <- rbindlist(list(subtypes, agg_df), use.names=TRUE)
      totals[[i]] <- total_df
      i <- i + 1
    }
    return(rbindlist(totals, use.names=TRUE))
  }
  
  non_subtypes <- prep5[!grepl(paste0(parent_causes, collapse = "|"), acause)]
  has_subtypes <- prep5[grepl(paste0(parent_causes, collapse = "|"), acause)]
  
  # aggregate children to get parent
  aggregated <- lapply(parent_causes, FUN = function(x){
    aggregate_subtypes(parent_cause = x, input_df = has_subtypes,
                       uid_cols = c("registry_index", "year_start", 
                                    "year_end", "sex_id", "age", 
                                    "coding_system", "dataset_id"), 
                       metric = "cases")
  })
  aggregated <- rbindlist(aggregated)
  aggregated <- rbindlist(list(aggregated, non_subtypes), use.names=TRUE)
  
  prop_new <- inner_join(aggregated, (registry[, list(registry_index, location_id)] %>% unique), by = c("registry_index"), all.x = T)
  setDT(prop_new)
  prop_new <- inner_join(prop_new, locs[, list(location_id, super_region_id, super_region_name)], by = c("location_id"), all.x = T)
  setDT(prop_new)
  prop_new <- prop_new[!(is.na(super_region_id))]
  prop_new <- prop_new[grepl("neo_", acause)]
  # some renaming of causes
  prop_new[acause == "neo_neuroblastoma", acause := "neo_neuro"]
  prop_new[acause == "neo_tissues_sarcoma", acause := "neo_tissue_sarcoma"]
  return(prop_new)
}

##################################################################
#### GENERATE GLOBAL IICC Proportions for mortality proportions  #
##################################################################
library(haven)
library(data.table)
library(reshape2)
library(dplyr)

create_global_iicc_prop <- function(){
  # Creates global BL and other NHL proportions to splice in
  # from IICC data at the formatting step
  causes <- c("Iib", "Iic", "Iid", "Iie")
  Q405 <- read_dta("FILEPATH")%>% 
    filter(cause %in% causes) %>% 
    select(registry_index, year_start, year_end, sex_id, cause, grep('case*', colnames(.))) %>% as.data.table() 
  Q406 <- read_dta("FILEPATH")%>% 
    filter(cause %in% causes) %>% 
    select(registry_index, year_start, year_end, sex_id, cause, grep('case*', colnames(.))) %>% as.data.table() 
  
  ## reshape
  Q405 <- melt(Q405, id.vars = c("registry_index","year_start", "year_end", "sex_id", "cause"), value.name = 'value')
  Q406 <- melt(Q406, id.vars = c("registry_index","year_start", "year_end", "sex_id", "cause"), value.name = 'value')
  setnames(Q405, c('variable'), c('age_group'))
  setnames(Q406, c('variable'), c('age_group'))
  Q405 <- Q405[, age_group := gsub("cases*", "",age_group)]
  Q406 <- Q406[, age_group := gsub("cases*", "",age_group)]
  Q405 <- reshape(Q405, idvar = c("registry_index","year_start", "year_end", "sex_id", "age_group"), timevar = 'cause', direction = 'wide')
  Q406 <- reshape(Q406, idvar = c("registry_index","year_start", "year_end", "sex_id", "age_group"), timevar = 'cause', direction = 'wide')
  data <- rbind(Q405,Q406) %>% select(-c(year_start, year_end))
  names(data) <- gsub('value.', '', names(data))
  
  ## assign locaiton names and GBD 
  data <- data %>% select(sex_id, age_group, Iib, Iic, Iid, Iie) 
  data <- data %>% filter(sex_id %in% c(1,2)) %>% as.data.table()
  data <- data[age_group == 2, age_group := 94]
  age_0_4 <- data[age_group %in% c(91, 94), ]
  age_0_4 <- age_0_4[, age_group := 2]
  
  set1_1 <- data[, `:=`(BL = sum(Iic)/sum(Iib+Iic+Iid), NHL_parent_other = sum(Iib+Iid)/sum(Iib+Iic+Iid)), by = c("sex_id", "age_group")] %>%
    select(age_group, sex_id, BL, NHL_parent_other) %>%  unique()
  set1_3 <- age_0_4[, `:=`(BL = sum(Iic)/sum(Iib+Iic+Iid), NHL_parent_other = sum(Iib+Iid)/sum(Iib+Iic+Iid)),by = c("sex_id")] %>%
    select(age_group, sex_id, BL, NHL_parent_other) %>%  unique() 
  
  set1 <- rbind(set1_1, set1_3)
  set1$location_name <- "Global"
  nhl_all <- melt(set1, id.vars = c("age_group", "sex_id"), 
                  measure.vars = c("BL", "NHL_parent_other"), 
                  value.name = "prop", variable.name = "acause")
  setnames(nhl_all, c("age_group"), c("age"))
  nhl_all[acause == "BL", acause := "neo_lymphoma_burkitt"]
  nhl_all[acause == "NHL_parent_other", acause := "neo_lymphoma_other"]
  return(nhl_all)
}

#######################################################################
# GENERATE GLOBAL Leukemia specific prop for mortality proportions    #
#           USING VR data                                             #
#######################################################################
create_global_leuk_prop <- function(){
  # Creates global leukemia subtype proportions to splice in
  # Used get_claude_data for leukemia for the input data for this function
  leuk <- fread("FILEPATH")
  leuk_total <- leuk[acause != "neo_leukemia", .(deaths_total = sum(deaths)), by = list(age, sex_id)]
  leuk_sub_total <- leuk[acause != "neo_leukemia", .(deaths_sub_total = sum(deaths)), by = list(acause, age, sex_id)]
  leuk_all <- merge(leuk_sub_total, leuk_total, by = c("age", "sex_id"))
  leuk_all[, prop := deaths_sub_total/deaths_total]
  leuk_all[sex_id == 1, sex_name := "Males"]
  leuk_all[sex_id == 2, sex_name := "Females"]
  leuk_all <- merge(leuk_all, age_id, by = c("age"))
  return(leuk_all)
}

#######################################################################
#                 CREATE THE GENERAL PROPORTIONS                      #
#######################################################################
create_proportions <- function(prop_new, metric_name){
  # Function for creating proportions based on metric_name: ['inc', 'mor']
  #
  if(metric_name == "inc"){
    prop_total <- prop_new[, .(prop_t= sum(cases)), by = c('super_region_id', 'super_region_name', 'age', 'sex_id')]
    prop_cause_t <- prop_new[, .(prop_cause = sum(cases)), by = c('acause', 'super_region_id', 'super_region_name', 'age', 'sex_id')]
    
    prop_total_new <- merge(prop_total, prop_cause_t, by =  c('super_region_id', 'super_region_name', 'age', 'sex_id'))
    prop_total_new[, prop := prop_cause/prop_t]
    prop_total_new[, sex_name := "Both Sexes"]
    prop_total_new[sex_id == 2, sex_name := "Females"]
    prop_total_new[sex_id == 1, sex_name := "Males"]
    age_id <- data.table(age = c(2, 7:25), new_age_id = c(1:20))
    prop_total_new <- merge(prop_total_new, age_id, by = c("age"), all.x = T)
    
    # convert to five year age bins
    five_year <- data.table(age = c(2, 7:25), five_year = seq(0, 95, 5))
    prop_total_new <- merge(prop_total_new, five_year, by = c("age"), all.x = T)
    setnames(prop_total_new, old = c("age", "five_year"), new = c("cancer_age_id", "age"))
    
    ################################################################
    ## impute for proportions that are 0 in age restriction range ##
    ################################################################
    # get average across 20+ for all causes and get min across 20+ 
    average_prop <- prop_total_new[age >= 20 & prop != 0, .(avg_prop = mean(prop), min_prop = min(prop)), by = c("sex_id", "super_region_name", "acause")]
    average_prop <- prop_total_new[age >= 20 & prop != 0, .(avg_prop = mean(prop), min_prop = min(prop)), by = c("sex_id", "super_region_name", "acause")]
    
    # global minimum (meaning the lowest proportion for that age/sex/cause across the other super regions), 
    # another with the super region average
    average_prop <- prop_total_new[prop != 0, .(avg_prop = mean(prop)), by = c("sex_id", "super_region_name", "acause")]
    min_prop <- prop_total_new[prop != 0, .(global_min_prop = min(prop)), by = c("sex_id", "acause", "age")]
    
    min_prop_inter <- prop_total_new[prop != 0, .(min_prop_super_region = min(prop)), by = c("sex_id", "super_region_name", "acause")]
    
    zeros <- prop_total_new[prop == 0]
    registry_input <- fread("FILEPATH")
    zeros <- merge(zeros, (registry_input[decomp_step == 3 & refresh == 2, list(yld_age_end, yld_age_start, acause)] %>% unique), by = "acause")
    zeros <- zeros[age >= yld_age_start & age <= yld_age_end]
    zeros <- zeros[!grepl("neo_ovarian|testicular|uterine|prostate", acause)]
    zeros <- zeros[, list(acause, super_region_name, sex_id, prop, age, yld_age_start, yld_age_end)] %>% unique
    zeros <- merge(zeros, average_prop, by = c("sex_id", "super_region_name", "acause"))
    zeros <- merge(zeros, min_prop, by = c("sex_id", "acause", "age"), all.x = T)
    zeros <- merge(zeros, min_prop_inter, by = c("sex_id", "super_region_name", "acause"), all.x = T)
    
    zeros_adj <- zeros[, prop := min_prop_super_region]
    setnames(zeros_adj, "prop", "prop_adj")
    zeros_adj <- zeros_adj[, list(sex_id, super_region_name, acause, age, prop_adj)]
    prop_total_new <- merge(prop_total_new, zeros_adj, by = c("sex_id", "super_region_name", "age", "acause"), all.x = T)
    prop_total_new[!is.na(prop_adj), prop := prop_adj]
    prop_total_new$prop_adj <- NULL
    
    fwrite(prop_total_new, "FILEPATH")
  }
  if(metric_name == "mor"){
    prop_new <- handle_liver_proportions(prop_new)
    
    # create global proportions
    global_total <- prop_new[, .(prop_t= sum(deaths)), by = c('age', 'sex_id')]
    global_cause_t <- prop_new[, .(prop_cause = sum(deaths)), by = c('acause', 'age', 'sex_id')]
    global <- merge(global_total, global_cause_t, by =  c('age', 'sex_id'))
    global[, prop := prop_cause/prop_t]
    # splice in nhl and leukemia VR proportions (multiply by parent proportion)
    leuk_old_prop <- global[acause == "neo_leukemia"]
    setnames(leuk_old_prop, "prop", "parent_prop")
    leuk_all <- create_global_leuk_prop()
    setnames(leuk_all, "prop", "relative_prop")
    leuk_all <- merge(leuk_all, leuk_old_prop[, list(age, sex_id, parent_prop)], all.x = T, by = c('age', 'sex_id'))
    global <- merge(global, leuk_all[, list(age, sex_id, acause, relative_prop, parent_prop)], all.x = T, by = c('acause', 'age', 'sex_id'))
    global[!is.na(relative_prop) & !is.na(parent_prop), prop := relative_prop * parent_prop]
    global$relative_prop <- NULL
    global$parent_prop <- NULL
    
    lymph_old_prop <- global[acause == "neo_lymphoma"]
    setnames(lymph_old_prop, "prop", "parent_prop")
    
    nhl_all <- create_global_iicc_prop()
    setnames(nhl_all, "prop", "relative_prop")
    nhl_all[, age := as.numeric(age)]
    nhl_all <- merge(nhl_all, lymph_old_prop[, list(age, sex_id, parent_prop)], all.x = T, by = c('age', 'sex_id'))
    global <- merge(global, nhl_all[, list(age, sex_id, acause, relative_prop, parent_prop)], all.x = T, by = c('acause', 'age', 'sex_id'))
    global[!is.na(relative_prop) & !is.na(parent_prop), prop := relative_prop * parent_prop]
    
    # convert to five year age bins
    five_year <- data.table(age = c(2, 7:25), five_year = seq(0, 95, 5))
    global <- merge(global, five_year, by = c("age"), all.x = T)
    setnames(global, old = c("age", "five_year"), new = c("cancer_age_id", "age"))
    fwrite(global, "FILEPATH")
  }
}

handle_liver_proportions <- function(prop_new){
  # handling liver by setting liver values = liver hbl values when <10 yo
  # get count by group if we have both liver and liver_hbl
  livers <- prop_new[grepl("neo_liver", acause)]
  nonlivers <- prop_new[!grepl("neo_liver", acause)]
  livers[, has_hbl := ifelse(acause == "neo_liver_hbl", 1, 0)]
  livers[, has_parent := ifelse(acause == "neo_liver", 1, 0)]
  test_liv <- livers %>% copy
  test_liv$acause <- NULL
  test_liv <- test_liv[, list(age,sex_id,registry_index,year_start,year_end,age,dataset_id, has_hbl, has_parent)] %>% unique
  test_liv <- test_liv[, lapply(.SD, sum), .SDcols = c("has_hbl", "has_parent"), by = c("age", "sex_id", "registry_index", "year_start", 
                                                                                        "year_end", "dataset_id")]
  
  test_liv[, hasBoth := "1"[has_hbl == 1 & has_parent == 1], by = c("age", "sex_id", "registry_index", "year_start", "year_end", "age", "dataset_id")]
  liver <- merge(livers, test_liv, all.x = T, by = c("age", "sex_id", "registry_index", "year_start", "year_end", "dataset_id") )
  liver[is.na(hasBoth), hasBoth := 0]
  liver_noagg <- liver[hasBoth == 0]
  liver_agg <- liver[hasBoth == 1]
  liver_total <- liver[hasBoth == 1 & acause == "neo_liver"]
  liver_sub <- liver[hasBoth == 1 & acause == "neo_liver_hbl"]
  setnames(liver_total, "deaths", "sub_deaths")
  liver_all <- merge(liver_sub, liver_total)
  liver_all[sub_deaths < deaths & age < 8, deaths := deaths + sub_deaths]
  liver_all[sub_deaths > deaths & age < 8, deaths := sub_deaths]
  setnames(liver_all, "deaths", "new_deaths")
  liver_agg <- merge(liver_agg, liver_all[, list(age,sex_id,registry_index,year_start,year_end,dataset_id, new_deaths)], all.x = T)
  liver_agg[age < 8, deaths := new_deaths]
  liver_agg$new_deaths <- NULL
  
  # for where there is liver and no liver hbl for age < 10
  liver_hbl_only <- liver_noagg[acause == "neo_liver" & age < 8]
  liver_hbl_only[acause == "neo_liver", acause := "neo_liver_hbl"]
  
  # for where there is liver_hbl and no liver parent for age < 10
  liver_only <- liver_noagg[acause == "neo_liver_hbl" & age < 8]
  liver_only[acause == "neo_liver_hbl", acause := "neo_liver"]
  
  prop_new <- rbindlist(list(nonlivers, liver_agg, liver_noagg, liver_hbl_only, liver_only), fill = T)
  return(prop_new)
}

run <- function(){
  metric_type <- "inc"
  # main run function
  prop_new <- clean_input_data(metric_type)
  create_proportions(prop_new, metric_type)
}

