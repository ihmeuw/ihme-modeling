###########################################################
### Author: 
### Date: 12/12/17
### Project: Split Ethiopia Study (Prob/Def)
### Purpose: GBD 2017 Nonfatal Estimation
###########################################################

## SET-UP
if (Sys.info()[1] == "Linux"){
  j_root <- "/home/j/"
  h_root <- "~/"
} else if (Sys.info()[1] == "Windows"){
  j_root <- "J:/"
  h_root <- "H:/"
}

pacman::p_load(data.table, ggplot2, readr)
library("openxlsx", lib.loc = paste0(j_root, FILEPATH))

## SET OBJECTS
temp_dir <- paste0(j_root, FILEPATH)
functions_dir <- paste0(j_root, FILEPATH)
upload_dir <- paste0(j_root, FILEPATH)
date <- Sys.Date()
date <- gsub("-", "_", date)
split_locs <- c(44861, 44855, 44854, 44858)

## SOURCE FUNCTIONS
source(paste0(functions_dir, "upload_epi_data.R"))
source(paste0(functions_dir, "get_epi_data.R"))
source(paste0(functions_dir, "get_ids.R"))
source(paste0(functions_dir, "get_population.R"))

## USER FUNCTIONS
col_order <- function(dt){
  epi_order <- fread(paste0(temp_dir, FILEPATH), header = F)
  epi_order <- tolower(as.character(epi_order[, V1]))
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

age_sex_ethiopia <- function(dt){
  ## SUBSET DATA #1
  df_split <- copy(dt)
  df_tosplit <- copy(df_split[type %in% c("probable", "definite")])
  df_split <- df_split[group_review == 0]
  
  ## CALC CASES MALE/FEMALE
  df_split[, cases_total:= sum(cases), by = list(nid, group, specificity, measure, cause, type)]
  df_split[, prop_cases := cases / cases_total]
  
  ## CALC PROP MALE/FEMALE
  df_split[, ss_total:= sum(sample_size), by = list(nid, group, specificity, measure, type, cause)]
  df_split[, prop_ss := sample_size / ss_total]
  
  ## CALC SE
  df_split[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
  df_split[, se_ss:= sqrt(prop_ss*(1-prop_ss) / ss_total)]
  
  ## RATIO 
  df_split[, ratio := prop_cases / prop_ss]
  df_split[, se_ratio:= sqrt( (prop_cases^2 / prop_ss^2)  * (se_cases^2/prop_cases^2 + se_ss^2/prop_ss^2) )]
  df_ratio <- df_split[specificity == "age,sex", list(nid, group, cause, sex, age_start, age_end, measure, ratio, se_ratio, prop_cases, prop_ss, year_start, year_end)]
  
  ## CREATE NEW OBSERVATIONS
  print("creating new observations")
  age.sex <- copy(df_tosplit)
  age.sex[,specificity := "age,sex"]
  age.sex[,seq := ""]
  age.sex[, c("age_start", "age_end", "sex") := NULL]
  age.sex <- merge(age.sex, df_ratio, by = c("nid", "group", "measure", "year_start", "year_end", "cause"), allow.cartesian = T)
  
  ## CALC MEANS
  age.sex[, standard_error := round(sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * ratio^2 + se_ratio^2 * mean^2), digits = 4)]
  age.sex[, mean := mean * ratio]
  age.sex[, cases := round(cases * prop_cases, digits = 0)]
  age.sex[, sample_size := round(sample_size * prop_ss, digits = 0)]
  age.sex[,note_modeler := paste(note_modeler, "| age-sex split from source using ratio", round(ratio, digits = 2))]
  age.sex[,c("ratio", "se_ratio", "split", "prop_cases", "prop_ss") := NULL]
  age.sex[, c("lower", "upper", "uncertainty_type_value") := ""]
  
  ## GET PARENTS AND ATTACH AND WELL AS OTHER DATA POINTS
  parents <- copy(df_tosplit)
  parents[, `:=` (note_modeler = "parent data, has been age-sex split using other type pattern", 
                  group_review = 0)]
  original <- copy(dt[!seq %in% parents$seq])
  total <- rbindlist(list(parents, age.sex, original), use.names = T)
  return(total)
}

split_data <- function(dt, weight_dt, loc_id){
  weight <- as.numeric(weight_dt[location_id == loc_id, weight])
  name <- as.character(weight_dt[location_id == loc_id, location_name])
  new_data <- copy(dt)
  new_data[, c("seq", "input_type", "upper", "lower", "cases", "uncertainty_type_value", "uncertainty_type") := ""]
  new_data[, group_review := 1]
  new_data[, `:=` (sample_size = sample_size * weight,
                  note_modeler = paste0(note_modeler, " | split from Ethiopia data (by sex pops) using weight = ", weight),
                  location_name = name,
                  location_id = loc_id)]
  return(new_data)
}

## GET DATA
dt_total <- rbindlist(all_data, fill = T)
ethiopia <- copy(dt_total[nid == 276968])
other_data <- copy(dt_total[!nid == 276968])

## AGE SEX SPLIT DATA
print("age-sex splitting")
ethiopia <- age_sex_ethiopia(ethiopia)

## GET POPS
print("getting pops")
loc_info <- get_ids(table = "location")
pops <- get_population(location_id = split_locs, year_id = 2014, 
                       sex_id = c(1, 2), gbd_round_id = 5)
pops[, total_pop := sum(population), by = "sex_id"]
pops[, weight := population / total_pop]
pops <- merge(pops, loc_info, by = "location_id")
weights <- pops[, .(sex_id, location_id, location_name, weight)]

## SPLIT BY POPULATION
print("splitting by pops")
f_weights <- weights[sex_id == 2]
f_tosplit <- ethiopia[sex == "Female" & !grepl("(by sex pops)", note_modeler) & group_review == 1]
f_split <- rbindlist(lapply(1:length(split_locs),
                             function(x) split_data(dt = f_tosplit, weight_dt = f_weights,
                                                    loc_id = split_locs[x])))
f_parents <- copy(f_tosplit)
f_parents[, `:=` (note_modeler = paste0(note_modeler, " | parent data, split by Ethiopia (sex) pops"),
                  group_review = 0)]

m_weights <- weights[sex_id == 2]
m_tosplit <- ethiopia[sex == "Male" & !grepl("(by sex pops)", note_modeler) & group_review == 1]
m_split <- rbindlist(lapply(1:length(split_locs),
                            function(x) split_data(dt = m_tosplit, weight_dt = m_weights,
                                                   loc_id = split_locs[x])))
m_parents <- copy(m_tosplit)
m_parents[, `:=` (note_modeler = paste0(note_modeler, " | parent data, split by Ethiopia (sex) pops"),
                  group_review = 0)]

## PUT TOGETHER
print("finishing up")
children <- rbind(f_split, m_split)
parents <- rbind(f_parents, m_parents)
others <- ethiopia[!seq %in% parents$seq]
total <- rbindlist(list(parents, children, others), use.names = T)
total <- col_order(total)

