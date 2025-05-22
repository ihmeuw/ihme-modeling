###########################################################
### Author: USER
### Date: 12/12/17
### Project: Data Procesing Functions
### Purpose: GBD 2017 Nonfatal Estimation
###########################################################

## SET-UP
pacman::p_load(data.table, ggplot2, boot)
library(openxlsx, lib.loc = paste0("FILEPATH", "FILEPATH"))

## SET OBJECTS
step <- "step1"

## FUNCTIONS
read_data <- function(id){
  print(id)
  dt <- get_bundle_data(bundle_id = id, step)
  return(dt)
}

col_order <- function(data.table){
  dt <- copy(data.table)
  epi_order <- fread(paste0("FILEPATH", "upload_order.csv"), header = F)
  epi_order <- tolower(as.character(epi_order[, V1]))
  epi_order <- epi_order[!epi_order == ""]
  names_dt <- tolower(names(dt))
  setnames(dt, names(dt), names_dt)
  for (name in epi_order){
    if (name %in% names(dt) == F){
      dt[, c(name) := ""]
    }
  }
  extra_cols <- setdiff(names(dt), tolower(epi_order))
  new_epiorder <- c(epi_order, extra_cols)
  setcolorder(dt, new_epiorder)
  return(dt)
}

return_ethiopia <- function(name){ # removes old ethiopia data from each dataframe in all_data and appends age-sex split ethiopia data for that specific bundle
  dt <- get(name)
  dt <- dt[!nid == 276968]
  ethiopia <- total[type == map[bundle_name == name, type] & cause == map[bundle_name == name, cause]]
  withe <- rbindlist(list(dt, ethiopia), use.names = T, fill = T)
  return(withe)
}

age_sex_split <- function(name){
  
  ## GET DATA
  step1 <- paste0(name, "_1")
  dt <- get(step1)  
  df_split <- copy(dt)
  
  ## SUBSET DATA
  df_split[is.na(sample_size), note_modeler := paste0(note_modeler, " | sample size back calculated to age sex split")]
  df_split[measure == "prevalence" & is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
  df_split[measure == "incidence" & is.na(sample_size), sample_size := mean/standard_error^2]
  df_split[is.na(cases), cases := sample_size * mean]
  df_split <- df_split[!is.na(cases),]
  df_split[, split := length(specificity[specificity == "age,sex"]), by = list(nid, group, measure, year_start, year_end)]
  df_split <- df_split[specificity %in% c("age", "sex") & split == 0,]
  
  ## CALC CASES MALE/FEMALE
  df_split[, cases_total:= sum(cases), by = list(nid, group, specificity, measure)]
  df_split[, prop_cases := round(cases / cases_total, digits = 3)]
  
  ## CALC PROP MALE/FEMALE
  df_split[, ss_total:= sum(sample_size), by = list(nid, group, specificity, measure)]
  df_split[, prop_ss := round(sample_size / ss_total, digits = 3)]
  
  ## CALC SE
  df_split[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
  df_split[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]
  
  ## RATIO 
  df_split[, ratio := round(prop_cases / prop_ss, digits = 3)]
  df_split[, se_ratio:= round(sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) ), digits = 3)]
  df_ratio <- df_split[specificity == "sex", list(nid, group, sex, measure, ratio, se_ratio, prop_cases, prop_ss, year_start, year_end)]
  
  ## CREATE NEW OBSERVATIONS
  print("creating new observations")
  age.sex <- copy(df_split[specificity == "age"])
  age.sex[,specificity := "age,sex"]
  age.sex[,seq := ""]
  age.sex[,c("ratio", "se_ratio", "split",  "cases_total",  "prop_cases",  "ss_total",	"prop_ss",	"se_cases",	"se_ss") := NULL]
  male <- copy(age.sex[, sex := "Male"])
  female <- copy(age.sex[, sex := "Female"])
  
  age.sex <- rbind(male, female)   
  age.sex <- merge(age.sex, df_ratio, by = c("nid", "group", "sex", "measure", "year_start", "year_end"))
  
  ## CALC MEANS
  age.sex[, standard_error := round(sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2), digits = 4)]
  age.sex[, mean := mean * ratio]
  age.sex[, cases := round(cases * prop_cases, digits = 0)]
  age.sex[, sample_size := round(sample_size * prop_ss, digits = 0)]
  age.sex[,note_modeler := paste(note_modeler, "| age,sex split using sex ratio", round(ratio, digits = 2))]
  age.sex[,c("ratio", "se_ratio", "prop_cases", "prop_ss", "version") := NULL]
  age.sex[, c("lower", "upper", "uncertainty_type_value") := ""]
  
  ## CREATE UNIQUE GROUP TO GET PARENTS
  age.sex.m <- age.sex[,c("nid","group", "measure"), with=F]    
  age.sex.m <- unique(age.sex.m, by=c("nid","group", "measure"))
  
  ## GET PARENTS
  parent <- merge(age.sex.m, dt, by= c("nid", "group", "measure"))
  parent[specificity == "age" | specificity == "sex", group_review:=0]
  parent[, note_modeler := paste0(note_modeler, " | parent data, has been age-sex split")]
  
  ## FINAL DATA
  original <- dt[!seq %in% parent$seq]
  total <- rbind(parent, age.sex, original)
  total <- col_order(total)
  
  ## MESSAGE AND RETURN
  if (nrow(parent) == 0) print(paste0("nothing to age-sex split in bundle for ",name))
  else print(paste0("split ", length(parent[, unique(nid)]), " nids for ", name))
  return(total)
}

sex_split <- function(name){
  step2 <- paste0(name, "_2")
  dt <- get(step2)
  cause <- map[bundle_name == name, cause]
  names <- paste0(map[adjust == "unadjusted", bundle_name], "_2")
  names <- names[!names == step2]
  names <- names[grep(cause, names)]
  other_dt <- rbindlist(lapply(names, function(x) get(x)), fill = T)
  dt[, version := "small"]
  other_dt[, version := "big"]
  all_dt <- rbind(dt, other_dt, fill = T)
  
  ## SUBSET DATA
  df_split <- copy(all_dt)
  df_split[is.na(sample_size), note_modeler := paste0(note_modeler, " | sample size back calculated to sex split from same source")]
  df_split[measure == "prevalence" & is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
  df_split[measure == "incidence" & is.na(sample_size), sample_size := mean/standard_error^2]
  df_split[is.na(cases), cases := sample_size * mean]
  df_split <- df_split[!is.na(cases),]
  df_split[group_review == 1, split := length(specificity[specificity == "sex"]), by = list(nid, group, location_id, measure, year_start, year_end, version, type)]
  df_tosplit <- copy(df_split[specificity == "total" & split == 0 & version == "small"])
  df_split <- df_split[specificity == "sex" & split == 2 & version == "big"]
  
  ## CALC CASES MALE/FEMALE
  df_split[, cases_total:= sum(cases), by = list(nid, group, specificity, measure, version, type, location_id)]
  df_split[, prop_cases := round(cases / cases_total, digits = 3)]
  
  ## CALC PROP MALE/FEMALE
  df_split[, ss_total:= sum(sample_size), by = list(nid, group, specificity, measure, version, type, location_id)]
  df_split[, prop_ss := round(sample_size / ss_total, digits = 3)]
  
  ## CALC SE
  df_split[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
  df_split[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]
  
  ## RATIO 
  df_split[, ratio := round(prop_cases / prop_ss, digits = 3)]
  df_split[, se_ratio:= sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) )]
  df_ratio <- df_split[specificity == "sex", list(nid, group, type, sex, location_id, measure, ratio, se_ratio, prop_cases, prop_ss, year_start, year_end, cases)]
  
  ## MAKE SURE NO DUPLICATED PATTERNS
  df_ratio[, length := .N, by = list(nid, group, location_id, measure, year_start, year_end)]
  df_ratio[length > 2, keep := sum(cases), by = list(type, nid, group, location_id, measure, year_start, year_end)]
  df_ratio[length>2, mean := mean(keep)]
  df_ratio <- df_ratio[is.na(mean) | keep>mean, .(nid, group, sex, location_id, measure, ratio, se_ratio, prop_cases, prop_ss, year_start, year_end, type)]
  setnames(df_ratio, "type", "oldtype")
  
  ## CREATE NEW OBSERVATIONS
  print("creating new observations")
  age.sex <- copy(df_tosplit)
  age.sex[,specificity := "sex"]
  age.sex[,seq := ""]
  male <- copy(age.sex[, sex := "Male"])
  female <- copy(age.sex[, sex := "Female"])
  
  age.sex <- rbind(male, female)   
  age.sex <- merge(age.sex, df_ratio, by = c("nid", "group", "sex", "measure", "year_start", "year_end", "location_id"))
  
  ## CALC MEANS
  age.sex[, standard_error := round(sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2), digits = 4)]
  age.sex[, mean := mean * ratio]
  age.sex[, cases := round(cases * prop_cases, digits = 0)]
  age.sex[, sample_size := round(sample_size * prop_ss, digits = 0)]
  age.sex[,note_modeler := paste(note_modeler, "| sex split using ratio", round(ratio, digits = 2), " from ", oldtype, " headache")]
  age.sex[,c("ratio", "se_ratio", "split", "prop_cases", "prop_ss", "oldtype", "version") := NULL]
  age.sex[, c("lower", "upper", "uncertainty_type_value") := ""]
  
  ## CREATE UNIQUE GROUP TO GET PARENTS
  age.sex.m <- age.sex[,c("nid","group", "measure", "location_id"), with=F]    
  age.sex.m <- unique(age.sex.m, by=c("nid","group", "measure", "location_id"))
  
  ## GET PARENTS
  parent <- merge(age.sex.m, dt, by= c("nid", "group", "measure", "location_id"))
  parent[specificity == "total", group_review:=0]
  parent[, note_modeler := paste0(note_modeler, " | parent data, has been sex split using same source")]
  
  ## FINAL DATA
  original <- dt[!seq %in% parent$seq]
  total <- rbindlist(list(parent, age.sex, original), fill = T)
  total <- col_order(total)
  total[, version := NULL]
  
  ## MESSAGE AND RETURN
  if (nrow(parent) == 0) print(paste0("nothing to sex split in bundle for ", name))
  else print(paste0("split ", length(parent[, unique(nid)]), " nids for ", name))
  return(total)
  
}

age_split_fromsource <- function(name){
  step3 <- paste0(name, "_3")
  dt <- get(step3)
  cause <- map[bundle_name == name, cause]
  names <- paste0(map[adjust == "unadjusted", bundle_name], "_3")
  names <- names[!names == step3]
  names <- names[grep(cause, names)]
  other_dt <- rbindlist(lapply(names, function(x) get(x)), fill = T)
  dt[, version := "small"]
  other_dt[, version := "big"]
  all_dt <- rbind(dt, other_dt, fill = T)
  
  ## SUBSET DATA #1
  df_split <- copy(all_dt)
  df_split[is.na(sample_size), note_modeler := paste0(note_modeler, " | sample size back calculated to age split from same source")]
  df_split[measure == "prevalence" & is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
  df_split[measure == "incidence" & is.na(sample_size), sample_size := mean/standard_error^2]
  df_split[is.na(cases), cases := sample_size * mean]
  df_split <- df_split[!is.na(cases),]
  df_split[group_review == 1, split := length(specificity[specificity == "age"]), by = list(nid, group, location_id, measure, year_start, year_end, version, type)]  
  df_tosplit <- copy(df_split[specificity == "total" & split == 0 & version == "small"])
  df_split <- df_split[specificity == "age" & split > 0 & version == "big"]
  
  ## CALC CASES MALE/FEMALE
  df_split[, cases_total:= sum(cases), by = list(nid, group, specificity, measure, version, type, location_id)]
  df_split[, prop_cases := cases / cases_total]
  
  ## CALC PROP MALE/FEMALE
  df_split[, ss_total:= sum(sample_size), by = list(nid, group, specificity, measure, version, type, location_id)]
  df_split[, prop_ss := sample_size / ss_total]
  
  ## CALC SE
  df_split[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
  df_split[, se_ss:= prop_ss*(1-prop_ss) / ss_total]
  
  ## RATIO 
  df_split[, ratio := prop_cases / prop_ss]
  df_split[, se_ratio:= sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) )]
  df_ratio <- df_split[specificity == "age", list(nid, group, sex, location_id, age_start, age_end, measure, ratio, se_ratio, prop_cases, prop_ss, year_start, year_end, type, cases)]
  
  ## CHECK FOR DUPLICATES
  df_ratio[, length_total := .N, by = list(nid, group, sex, measure, year_start, year_end)]
  df_ratio[, length_type := .N, by = list(nid, group, sex, measure, year_start, year_end, type)]
  df_ratio[length_total>length_type, keep := sum(cases), by = list(nid, type, group, sex, measure, year_start, year_end)]
  df_ratio[length_total>length_type, keep_mean := mean(keep), by = list(nid, group, sex, measure, year_start, year_end)]
  df_ratio <- df_ratio[is.na(keep) | keep>keep_mean, .(nid, group, sex, location_id, age_start, age_end, measure, ratio, se_ratio, prop_cases, prop_ss, year_start, year_end, type)]
  setnames(df_ratio, "type", "oldtype")
  
  ## CREATE NEW OBSERVATIONS
  print("creating new observations")
  age.sex <- copy(df_tosplit)
  age.sex[,specificity := "age"]
  age.sex[,seq := ""]
  age.sex[, c("age_start", "age_end") := NULL]
  age.sex <- merge(age.sex, df_ratio, by = c("nid", "group", "location_id", "sex", "measure", "year_start", "year_end"))
  
  ## CALC MEANS
  age.sex[, standard_error := sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2)]
  age.sex[, mean := mean * ratio]
  age.sex[, cases := round(cases * prop_cases, digits = 0)]
  age.sex[, sample_size := round(sample_size * prop_ss, digits = 0)]
  age.sex[,note_modeler := paste(note_modeler, "| sex split using ratio ", round(ratio, digits = 2), " from ", oldtype, " headache")]
  age.sex[,c("ratio", "se_ratio", "split", "prop_cases", "prop_ss", "oldtype", "version") := NULL]
  age.sex[, c("lower", "upper", "uncertainty_type_value") := ""]
  
  ## CREATE UNIQUE GROUP TO GET PARENTS
  age.sex.m <- age.sex[,c("nid","group", "measure", "location_id"), with=F]    
  age.sex.m <- unique(age.sex.m, by=c("nid","group", "measure", "location_id"))
  
  ## GET PARENTS AND SAVE
  parent1 <- merge(age.sex.m, dt, by= c("nid", "group", "measure", "location_id"))
  parent1[specificity == "total", group_review:=0]
  parent1[, note_modeler := paste0(note_modeler, " | parent data, has been age split using same source")]
  parent1[, version := NULL]
  round1 <- rbind(parent1, age.sex)
  
  ## SUBSET DATA #2
  df_split <- copy(all_dt)
  df_split[is.na(sample_size), note_modeler := paste0(note_modeler, " | sample size back calculated to age split from same source")]
  df_split[measure == "prevalence" & is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
  df_split[measure == "incidence" & is.na(sample_size), sample_size := mean/standard_error^2]
  df_split[is.na(cases), cases := sample_size * mean]
  df_split <- df_split[!is.na(cases),]
  df_split[group_review == 1, split := length(specificity[specificity == "age,sex"]), by = list(nid, group, location_id, measure, year_start, year_end, version, type)]
  df_tosplit <- copy(df_split[specificity == "sex" & split == 0 & version == "small"])
  df_split <- df_split[specificity == "age,sex" & split > 0 & version == "big"]
  
  ## CALC CASES MALE/FEMALE
  df_split[, cases_total:= sum(cases), by = list(nid, group, specificity, measure, version, type, location_id)]
  df_split[, prop_cases := cases / cases_total]
  
  ## CALC PROP MALE/FEMALE
  df_split[, ss_total:= sum(sample_size), by = list(nid, group, specificity, measure, version, type, location_id)]
  df_split[, prop_ss := sample_size / ss_total]
  
  ## CALC SE
  df_split[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
  df_split[, se_ss:= prop_ss*(1-prop_ss) / ss_total]
  
  ## RATIO 
  df_split[, ratio := prop_cases / prop_ss]
  df_split[, se_ratio:= sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) )]
  df_ratio <- df_split[specificity == "age", list(nid, group, sex, location_id, age_start, age_end, measure, ratio, se_ratio, prop_cases, prop_ss, year_start, year_end, type, cases)]
  
  ## CHECK FOR DUPLICATES
  df_ratio[, length_total := .N, by = list(nid, group, sex, measure, year_start, year_end)]
  df_ratio[, length_type := .N, by = list(nid, group, sex, measure, year_start, year_end, type)]
  df_ratio[length_total>length_type, keep := sum(cases), by = list(nid, type, group, sex, measure, year_start, year_end)]
  df_ratio[length_total>length_type, keep_mean := mean(keep), by = list(nid, group, sex, measure, year_start, year_end)]
  df_ratio <- df_ratio[is.na(keep) | keep>keep_mean, .(nid, group, sex, location_id, age_start, age_end, measure, ratio, se_ratio, prop_cases, prop_ss, year_start, year_end, type)]
  setnames(df_ratio, "type", "oldtype")
  
  ## CREATE NEW OBSERVATIONS
  print("creating new observations")
  age.sex <- copy(df_tosplit)
  age.sex[,specificity := "age,sex"]
  age.sex[,seq := ""]
  age.sex[, c("age_start", "age_end") := NULL]
  age.sex <- merge(age.sex, df_ratio, by = c("nid", "group", "location_id", "sex", "measure", "year_start", "year_end"))
  
  ## CALC MEANS
  age.sex[, standard_error := round(sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2), digits = 4)]
  age.sex[, mean := mean * ratio]
  age.sex[, cases := round(cases * prop_cases, digits = 0)]
  age.sex[, sample_size := round(sample_size * prop_ss, digits = 0)]
  age.sex[,note_modeler := paste(note_modeler, "| sex split using ratio", round(ratio, digits = 2), " from ", oldtype, " headache")]
  age.sex[,c("ratio", "se_ratio", "split", "prop_cases", "prop_ss", "oldtype", "version") := NULL]
  age.sex[, c("lower", "upper", "uncertainty_type_value") := ""]
  
  ## CREATE UNIQUE GROUP TO GET PARENTS
  age.sex.m <- age.sex[,c("nid","group", "measure", "location_id"), with=F]    
  age.sex.m <- unique(age.sex.m, by=c("nid","group", "measure", "location_id"))
  
  ## GET PARENTS AND SAVE
  parent2 <- merge(age.sex.m, dt, by= c("nid", "group", "measure", "location_id"))
  parent2[specificity == "sex", group_review:=0]
  parent2[, note_modeler := paste0(note_modeler, " | parent data, has been age split using same source")]
  parent2[, version := NULL]
  round2 <- rbind(parent2, age.sex)
  
  ## GET ORIGINAL DATA AND RETURN
  all_parents <- rbindlist(list(parent1, parent2), use.names = T, fill = T)
  originals <- dt[!seq %in% c(parent1$seq, parent2$seq)]
  total <- rbindlist(list(round1, round2, originals), fill = T)
  total[, version := NULL]
  if (nrow(all_parents) == 0) print(paste0("nothing to age split in bundle for ", name))
  else print(paste0("split ", length(all_parents[, unique(nid)]), " nids for ", name))
  return(total)
}

age_sex_split_type <- function(name){
  step4 <- paste0(name, "_4")
  dt <- get(step4)
  cause <- map[bundle_name == name, cause]
  names <- paste0(map[adjust == "unadjusted", bundle_name], "_4")
  names <- names[!names == step4]
  names <- names[grep(cause, names)]
  other_dt <- rbindlist(lapply(names, function(x) get(x)), fill = T)
  dt[, version := "small"]
  other_dt[, version := "big"]
  all_dt <- rbind(dt, other_dt, fill = T)
  
  ## SUBSET DATA
  df_split <- copy(all_dt)
  df_split[is.na(sample_size), note_modeler := paste0(note_modeler, " | sample size back calculated to age-sex split from same source")]
  df_split[measure == "prevalence" & is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
  df_split[measure == "incidence" & is.na(sample_size), sample_size := mean/standard_error^2]
  df_split[is.na(cases), cases := sample_size * mean]
  df_split <- df_split[!is.na(cases),]
  df_split[group_review == 1, split := length(specificity[specificity == "age,sex"]), by = list(nid, group, location_id, measure, year_start, year_end, version, type)]
  df_tosplit <- copy(df_split[specificity == "total" & split == 0 & version == "small"])
  df_split <- df_split[specificity == "age,sex" & split > 0 & version == "big"]
  
  ## CALC CASES MALE/FEMALE
  df_split[, cases_total:= sum(cases), by = list(nid, group, specificity, measure, version, type, location_id)]
  df_split[, prop_cases := round(cases / cases_total, digits = 3)]
  
  ## CALC PROP MALE/FEMALE
  df_split[, ss_total:= sum(sample_size), by = list(nid, group, specificity, measure, version, type, location_id)]
  df_split[, prop_ss := round(sample_size / ss_total, digits = 3)]
  
  ## CALC SE
  df_split[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
  df_split[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]
  
  ## RATIO 
  df_split[, ratio := round(prop_cases / prop_ss, digits = 3)]
  df_split[, se_ratio:= sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) )]
  df_ratio <- df_split[specificity == "age,sex", list(nid, group, type, sex, age_start, age_end, location_id, measure, ratio, se_ratio, prop_cases, prop_ss, year_start, year_end, cases)]
  
  ## MAKE SURE NO DUPLICATED PATTERNS
  df_ratio[, length_total := .N, by = list(nid, group, sex, measure, year_start, year_end)]
  df_ratio[, length_type := .N, by = list(nid, group, sex, measure, year_start, year_end, type)]
  df_ratio[length_total>length_type, keep := sum(cases), by = list(nid, type, group, sex, measure, year_start, year_end)]
  df_ratio[length_total>length_type, keep_mean := mean(keep), by = list(nid, group, sex, measure, year_start, year_end)]
  df_ratio <- df_ratio[is.na(keep) | keep>keep_mean, .(nid, group, sex, location_id, age_start, age_end, measure, ratio, se_ratio, prop_cases, prop_ss, year_start, year_end, type)]
  setnames(df_ratio, "type", "oldtype")
  
  ## CREATE NEW OBSERVATIONS
  print("creating new observations")
  age.sex <- copy(df_tosplit)
  age.sex[,specificity := "age,sex"]
  age.sex[,seq := ""]
  age.sex[, c("sex", "age_start", "age_end") := NULL]
  
  age.sex <- merge(age.sex, df_ratio, by = c("nid", "group", "measure", "year_start", "year_end", "location_id"))
  
  ## CALC MEANS
  age.sex[, standard_error := round(sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2), digits = 4)]
  age.sex[, mean := mean * ratio]
  age.sex[, cases := round(cases * prop_cases, digits = 0)]
  age.sex[, sample_size := round(sample_size * prop_ss, digits = 0)]
  age.sex[,note_modeler := paste(note_modeler, "| age-sex split using ratio", round(ratio, digits = 2), " from ", oldtype, " headache")]
  age.sex[,c("ratio", "se_ratio", "split", "prop_cases", "prop_ss", "oldtype", "version") := NULL]
  age.sex[, c("lower", "upper", "uncertainty_type_value") := ""]
  
  ## CREATE UNIQUE GROUP TO GET PARENTS
  age.sex.m <- age.sex[,c("nid","group", "measure", "location_id"), with=F]    
  age.sex.m <- unique(age.sex.m, by=c("nid","group", "measure", "location_id"))
  
  ## GET PARENTS
  parent <- merge(age.sex.m, dt, by= c("nid", "group", "measure", "location_id"))
  parent[specificity == "total", group_review:=0]
  parent[, note_modeler := paste0(note_modeler, " | parent data, has been age-sex split using same source")]
  
  ## FINAL DATA
  original <- dt[!seq %in% parent$seq]
  total <- rbindlist(list(parent, age.sex, original), fill = T)
  total <- col_order(total)
  total[, version := NULL]
  
  ## MESSAGE AND RETURN
  if (nrow(parent) == 0) print(paste0("nothing to age-sex split in bundle for ", name))
  else print(paste0("split ", length(parent[, unique(nid)]), " nids for ", name))
  return(total)
  
}

add_sub <- function(name){
  ## GET DATA
  print(paste0("calculating for ", name))
  step5 <- paste0(name, "_5")
  dt <- get(step5)
  cause <- map[bundle_name == name, cause]
  names <- paste0(map[adjust == "unadjusted", bundle_name], "_5")
  names <- names[!names == step5]
  names <- names[grep(cause, names)]
  other_data <- lapply(names, function(x) get(x))
  other1 <- as.data.table(other_data[1])
  other2 <- as.data.table(other_data[2])

  ## MAKE SURE CASES/SAMPLE SIZE ARE THERE
  other1[is.na(sample_size), note_modeler := paste0(note_modeler, " | sample size back calculated to calculate third type")]
  other1[measure == "prevalence" & is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
  other1[measure == "incidence" & is.na(sample_size), sample_size := mean/standard_error^2]
  other1[is.na(cases), cases := sample_size * mean]
  other1 <- other1[!is.na(cases),]
  other2[is.na(sample_size), note_modeler := paste0(note_modeler, " | sample size back calculated to calculate third type")]
  other2[measure == "prevalence" & is.na(sample_size), sample_size := mean*(1-mean)/standard_error^2]
  other2[measure == "incidence" & is.na(sample_size), sample_size := mean/standard_error^2]
  other2[is.na(cases), cases := sample_size * mean]
  other2 <- other2[!is.na(cases),]

  ## MERGE
  other1 <- other1[, .(nid, type, location_id, age_start, age_end, sex, year_start, year_end, measure, group, cases, sample_size)]
  setnames(other1, c("cases", "sample_size", "type"), c("other_cases", "other_sample_size", "other_type"))
  duplicates <- merge(other2, other1, by = c("nid", "location_id", "age_start", "age_end", "sex", "year_start", "year_end", "measure", "group"))
  duplicates[, dif := sample_size - other_sample_size]
  #duplicates <- duplicates[!dif>5 & !dif< -5] get rid if sample size discrepency greater than five
  duplicates[, sample_size := (sample_size + other_sample_size) / 2]
  
  ## CALCULATE
  if (duplicates$type[1] == "definite" & duplicates$other_type[1] == "probable"){
    duplicates[, `:=` (cases = cases + other_cases,
                       type = "both")]
  } else if (duplicates$type[1] == "probable" & duplicates$other_type[1] == "both"){
    duplicates[, `:=` (cases = other_cases - cases,
                       type = "definite")]
  } else if (duplicates$type[1] == "definite" & duplicates$other_type[1] == "both"){
    duplicates[, `:=` (cases = other_cases - cases,
                       type = "probable")]
  }
  duplicates[, c("other_type", "other_cases", "other_sample_size") := NULL]
  duplicates[, c("mean", "lower", "upper", "standard_error", "uncertainty_type_value") := ""]
  duplicates[, note_modeler := paste0(note_modeler, " | calculated from other two headache types")]
  
  ## APPEND - MAKE SURE DOESN"T ALREADY EXIST
  duplicates[, dup := 1]
  final <- rbindlist(list(dt, duplicates), use.names = T, fill = T)
  final[, count := .N, by = list(nid, type, location_id, age_start, age_end, sex, year_start, year_end, measure, group)]
  final <- final[count == 1 | (count == 2 & is.na(dup))]
  final[, c("dif", "dup", "count") := NULL]
  return(final)
}

add_outliers <- function(name){
  print(paste0("adding outliers for ", name))
  step6 <- paste0(name, "_6")
  dt <- get(step6)
  c <- map[bundle_name == name, cause]
  outliers <- as.data.table(fread(paste0(repo_dir, "headache_outliers.csv")))
  outliers <- outliers[cause == c]
  outliers[, cause := NULL]
  outliers[, flag := 1]
  dt <- merge(dt, outliers, by = "nid", all.x = T)
  dt[flag == 1, `:=` (is_outlier = 1,
                      note_modeler = paste0(note_modeler, " | ", note))]
  dt[, c("flag", "note") := NULL]
  return(dt)
}

delete_previous_data <- function(name){
  print(paste0("clearing adj bundle for ", name))
  id <- map[bundle_name == name, bundle_id]
  acause <- map[bundle_name == name, acause]
  dt <- get_bundle_data(bundle_id = id, step)
  dt <- dt[, .(seq)]
  write.xlsx(dt, "FILEPATH"), sheetName = "extraction")
  upload_bundle_data(bundle_id = id, filepath = "FILEPATH", decomp_step = step)
}


upload_data <- function(name, file_name){
  print(paste0("uploading ", name, " adjusted"))
  step7 <- paste0(name, "_7")
  dt <- get(step7)
  dt[, seq := ""]
  acause <- map[bundle_name == name, acause]
  bundle_toupload <- paste0(name, "_adj")
  id <- map[bundle_name == bundle_toupload, bundle_id]
  write.xlsx(dt, "FILEPATH", sheetName = "extraction")
  upload_bundle_data(bundle_id = id, filepath = "FILEPATH", decomp_step = step)
}

