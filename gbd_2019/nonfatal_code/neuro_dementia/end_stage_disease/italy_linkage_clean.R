##########################################################################
### Project: GBD Nonfatal Estimation
### Purpose: Italy Linked Data Analysis
##########################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
  functions_dir <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
  functions_dir <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2)
date <- gsub("-", "_", Sys.Date())

# SET UP OBJECTS ----------------------------------------------------------

italy_folder <- paste0("FILEPATH")
taiwan_folder <- paste0("FILEPATH")
france_folder <- paste0("FILEPATH")
graphs_dir <- paste0("FILEPATH")
results_dir <- paste0("FILEPATH")
repo_dir <- paste0("FILEPATH")
functions_dir <- paste0("FILEPATH")

name_dt <- data.table(cond = c("anycond", "bedridden", "bronchitis", "dehydration", "dysphagia", "fallfrombed", "fracture",
                               "malnut", "muscular_wasting", "pneum", "senility", "sepsis", "sodium", "ulcer", "uti"),
                      name = c("Any Condition", "Bedridden", "Bronchitis", "Dehydration", "Dysphagia", "Fall From Bed",
                               "Fracture", "Malnutrition", "Muscular Wasting", "Pneumonia", "Senility", "Sepsis", "Sodium",
                               "Ulcer", "Urinary Tract Infection"))

# SOURCE FUNCTIONS --------------------------------------------------------

source(paste0(functions_dir, "get_ids.R"))
source(paste0(functions_dir, "get_age_metadata.R"))
source(paste0(functions_dir, "get_population.R"))

get_age_groups <- function(change_dt){
  dt <- copy(change_dt)
  start_years <- age_dt[, age_group_years_start]
  dt[, age_group_years_start := start_years[findInterval(age, vec = start_years)]]
  dt <- merge(dt, age_dt[, .(age_group_years_start, age_group_name)], by = "age_group_years_start", sort = F)
  dt[, age_group_years_start := NULL]
  return(dt)
}

# GET AGE DATA ------------------------------------------------------------

age_metadata <- get_age_metadata(12)
age_names <- get_ids(table = "age_group")
full_age_dt <- merge(age_metadata, age_names, by = "age_group_id")
full_age_dt <- full_age_dt[age_group_id >= 13]
full_age_dt[age_group_id == 235, age_group_years_end := 100]
full_age_dt[, risk_age := (age_group_years_start + age_group_years_end) / 2]
age_dt <- copy(full_age_dt[age_group_years_start >= 65])
age_dt[age_group_id == 18, `:=` (age_group_years_start = 40, age_group_name = "40 to 69")]
sex_names <- get_ids(table = "sex")


# DATA PREP CODE ----------------------------------------------------------

source(paste0(repo_dir, "italy_linkage_prep.R"))

# CODES FOR END STAGE CONDITIONS ------------------------------------------

get_endstage_hosp <- function(hosp_dt){
  dt <- copy(hosp_dt)
  diagcols <- paste0("diag", 1:6)
  dt[, ulcer := apply(.SD, 1, function(x) sum(grepl("^7070", x) | grepl("^7072", x) | grepl("^7078", x) | grepl("^7079", x))), .SDcols = diagcols]
  dt[, malnut := apply(.SD, 1, function(x) sum(grepl("^262", x) | grepl("^263", x) | grepl("^261", x))), .SDcols = diagcols]
  dt[, pneum := apply(.SD, 1, function(x) sum(grepl("^482", x) | grepl("^483", x) | grepl("^486", x) | grepl("^5070", x) | grepl("^514", x) | grepl("^485$", x))), .SDcols = diagcols]
  dt[, dysphagia := apply(.SD, 1, function(x) sum(grepl("^7872", x))), .SDcols = diagcols]
  dt[, bronchitis := apply(.SD, 1, function(x) sum(grepl("^466", x))), .SDcols = diagcols]
  dt[, senility := apply(.SD, 1, function(x) sum(grepl("^797", x))), .SDcols = diagcols]
  dt[, fracture := apply(.SD, 1, function(x) sum(grepl("^820", x))), .SDcols = diagcols]
  dt[, dehydration := apply(.SD, 1, function(x) sum(grepl("^2765", x))), .SDcols = diagcols]
  dt[, sodium := apply(.SD, 1, function(x) sum(grepl("^2761", x) | grepl("^2760", x) | grepl("^2768", x) | grepl("^2769", x))), .SDcols = diagcols]
  dt[, septicemia := apply(.SD, 1, function(x) sum(grepl("^389$", x))), .SDcols = diagcols]
  dt[, muscular_wasting := apply(.SD, 1, function(x) sum(grepl("^7282", x))), .SDcols = diagcols]
  dt[, sepsis := apply(.SD, 1, function(x) sum(grepl("^038", x))), .SDcols = diagcols]
  dt[, fallbed := apply(.SD, 1, function(x) sum(grepl("E884.4", x))), .SDcols = diagcols]
  dt[, uti := apply(.SD, 1, function(x) sum(grepl("^5990", x) | grepl("^5901", x) | grepl("^595$", x) | grepl("^5950", x))), .SDcols = diagcols]
  dt[, bedridden := apply(.SD, 1, function(x) sum(grepl("^V4984", x))), .SDcols = diagcols]
  dt <- dt[, .(sid, hid, start_date, end_date, ulcer, malnut, pneum, dehydration,
               sepsis, sodium, fallbed, uti, senility, bedridden, muscular_wasting,
               bronchitis, dysphagia, fracture)]
}

get_anycond_hosp <- function(condition_dt){
  dt <- copy(condition_dt)
  endstagecols <- c("ulcer", "malnut", "sepsis", "fallbed", "uti", "pneum",
                    "senility", "dehydration", "sodium", "bedridden", "muscular_wasting", "bronchitis", "dysphagia",
                    "fracture")
  dt[, anycond := apply(.SD, 1, function(x) sum(x > 0)), .SDcols = endstagecols]
  return(dt)
}

conditions_dt <- get_endstage_hosp(hosp)
cond_dt <- get_anycond_hosp(conditions_dt)

# TAG ON MORTALITY DATE ---------------------------------------------------

get_mort_date <- function(mort_dt){
  dt <- copy(mort_dt)
  dt <- dt[, .(sid, date_death, age, sex)]
  return(dt)
}

mort_dates <- get_mort_date(mort)

# CALCULATE COMPLICATIONS IN LAST DAYS ------------------------------------

calc_comp <- function(cond_dt, comp_col, num_months){
  num_days <- 30.44 * num_months
  dt <- copy(cond_dt)
  dt <- merge(dt, mort_dates, by = "sid")
  dt[, date_diff := difftime(date_death, end_date, units = "days")]
  dt[, in_range := (date_diff <= num_days)]
  dt[in_range == T, paste0(comp_col, "_range") := get(comp_col)][in_range == F, paste0(comp_col, "_range") := 0]
  dt[, paste0(comp_col, "_range") := sum(get(paste0(comp_col, "_range")), na.rm = T), by = "sid"]
  dt <- unique(dt, by = "sid")
  dt <- dt[, c("sid", paste0(comp_col, "_range")), with = F]
  return(dt)
}

months <- c(1, 3, 6, 12)
conditions <- c("anycond", "ulcer", "malnut", "dehydration", "sodium",
                "sepsis", "pneum", "fallbed", "uti", "senility", "bedridden", "muscular_wasting",
                "bronchitis", "dysphagia", "fracture")
for (cond in conditions){
  print(cond)
  cond_list <- lapply(1:4, function(x) calc_comp(cond_dt = cond_dt, comp_col = cond, num_months = months[x]))
  assign(cond, cond_list)
}

# COMPARE WITH AND WITHOUT DEMENTIA ---------------------------------------

prop_yes <- function(range_dt, dem_dt, comp_col, num_months){
  dt <- copy(range_dt)
  dt <- merge(dt, dem_dt, by = "sid")
  dt <- merge(dt, mort_dates, by = "sid")
  dt <- get_age_groups(dt)
  dt[, mean := mean(get(paste0(comp_col, "_range")) > 0), by = c("dem", "age_group_name")]
  dt <- unique(dt, by = c("dem", "age_group_name"))
  dt <- dt[, .(dem, age_group_name, mean, cond = comp_col, months = num_months)]
  return(dt)
}

prop_yes_sex <- function(range_dt, dem_dt, comp_col, num_months){
  dt <- copy(range_dt)
  dt <- merge(dt, dem_dt, by = "sid")
  dt <- merge(dt, mort_dates, by = "sid")
  dt <- get_age_groups(dt)
  dt[, N := .N, by = c("dem", "sex", "age_group_name")]
  dt[, n := sum(get(paste0(comp_col, "_range")) > 0), by = c("dem", "sex", "age_group_name")]
  dt[, mean := n/N]
  dt <- unique(dt, by = c("dem", "sex", "age_group_name"))
  dt <- dt[, .(dem, age_group_name, sex, n, N, mean, cond = comp_col, months = num_months)]
  return(dt)
}

prop_yes_noage <- function(range_dt, dem_dt, comp_col, num_months){
  dt <- copy(range_dt)
  dt <- merge(dt, dem_dt, by = "sid")
  dt <- merge(dt, mort_dates, by = "sid")
  dt[, mean := mean(get(paste0(comp_col, "_range")) > 0), by = c("dem")]
  dt <- unique(dt, by = c("dem"))
  dt <- dt[, .(dem, mean, cond = comp_col, months = num_months)]
  return(dt)
}

for (cond in conditions){
  print(cond)
  cond_means <- rbindlist(lapply(1:4, function(x) prop_yes_noage(range_dt = get(cond)[[x]], dem_dt = dem_status, comp_col = cond, num_months = months[x])))
  assign(paste0(cond, "_means"), cond_means)
}

# GET ALL IN ONE DATATABLE ------------------------------------------------

mean_list <- list(anycond_means, ulcer_means, malnut_means, pneum_means, sepsis_means, bedridden_means,
                  uti_means, sodium_means, dehydration_means, senility_means, muscular_wasting_means,
                  bronchitis_means, dysphagia_means, fracture_means)

combine_all <- function(dt){
  combine_dt <- copy(dt)
  combine_dt[, `:=` (dataset = "Italy")]
  return(combine_dt)
}

all_means <- rbindlist(lapply(mean_list, combine_all))

calc_dif <- function(dt){
  dif_dt <- copy(dt)
  dif_dt <- dcast(dif_dt, cond + months ~ dem, value.var = "mean")
  setnames(dif_dt, c("0", "1"), c("no_dem", "dem"))
  dif_dt[, dif := dem - no_dem]
  return(dif_dt)
}

pre_dif <- calc_dif(all_means)
pre_dif[, rules := "oldrules"]


# GET AGE SEX DIFFERENCES - DON'T ALLOW NEGATIVE (SET TO ZERO) ------------

get_difference <- function(dt){
  diff_dt <- copy(dt)
  diff_dt[, totalN := sum(N), by = c("cond", "age_group_name", "sex")]
  diff_dt <- dcast(diff_dt, age_group_name + sex + cond + dataset + totalN ~ dem, value.var = "mean")
  setnames(diff_dt, c("0", "1"), c("nodem", "dem"))
  diff_dt[, diff := dem - nodem]
  diff_dt <- diff_dt[diff < 0, diff := 0]
  diff_dt[, n := diff * totalN]
  diff_dt[, `:=` (n = sum(n), N = sum(totalN)), by = "cond"]
  diff_dt <- unique(diff_dt, by = "cond")
  diff_dt[, mean := n/N]
  diff_dt <- diff_dt[, .(cond, dataset, n, N, mean)]
  return(diff_dt)
}

italy_diff <- get_difference(all_means[months == 12])




# GRAPH MEANS -------------------------------------------------------------

graph_means <- function(graph_dt){
  dt <- copy(graph_dt)
  gg <- ggplot(dt, aes(x = as.factor(months), y = mean, fill = as.factor(dem))) +
    geom_bar(position = "dodge", stat = "identity") +
    labs(y = "Percent", x = "Months Before Death") +
    facet_wrap(~as.factor(age_group_name)) +
    scale_fill_manual(name = "Dementia Status", values = c("midnightblue", "mediumpurple1"), labels = c("No Dementia", "Dementia")) +
    ggtitle(paste0("Percent of Individuals who Died with ", dt[, unique(cond)])) +
    theme_classic()
  return(gg)
}

graph_means_sex <- function(graph_dt){
  dt <- copy(graph_dt)
  gg <- ggplot(dt, aes(x = as.factor(months), y = mean, fill = as.factor(dem))) +
    geom_bar(position = "dodge", stat = "identity") +
    labs(y = "Percent", x = "Months Before Death") +
    facet_wrap(~as.factor(sex)) +
    scale_fill_manual(name = "Dementia Status", values = c("midnightblue", "mediumpurple1"), labels = c("No Dementia", "Dementia")) +
    ggtitle(paste0("Percent of Individuals who Died with ", dt[, unique(cond)])) +
    theme_classic()
  return(gg)
}

graph_means_noage <- function(graph_dt){
  dt <- copy(graph_dt)
  gg <- ggplot(dt, aes(x = as.factor(months), y = mean, fill = as.factor(dem))) +
    geom_bar(position = "dodge", stat = "identity") +
    labs(y = "Percent", x = "Months Before Death") +
    ylim(0, 0.27) +
    scale_fill_manual(name = "Dementia Status", values = c("midnightblue", "mediumpurple1"), labels = c("No Dementia", "Dementia")) +
    ggtitle(paste0("Percent of Individuals who Died with ", dt[, unique(cond)])) +
    theme_classic() +
    theme(text = element_text(size = 20, color = "black"))
  return(gg)
}

mean_list <- list(anycond_means, ulcer_means, malnut_means, pneum_means, sepsis_means, bedridden_means,
                  uti_means, sodium_means, dehydration_means, senility_means, muscular_wasting_means,
                  bronchitis_means, dysphagia_means, fracture_means)
graphs <- lapply(mean_list, graph_means_noage)


