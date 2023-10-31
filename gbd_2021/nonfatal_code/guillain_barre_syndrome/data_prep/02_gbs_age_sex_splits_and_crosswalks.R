##################################################################################################
## Purpose: GBS Step 2 Applying crosswalks from 2019 step 4 best 
## Creation Date: 04/15/20
## Created by: USERNAME
##################################################################################################
#Only Sex splitting and uploading as there is no new data which requires age split or age-sex split. 

rm(list=ls())
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
}


library(mortdb, lib = "FILEPATH")
library(Hmisc)
library(data.table)
library(msm)
library(plyr)
##################################################################################################
#Set objects
date <- gsub("-", "_", Sys.Date())
bundle_id <- 278
decomp_step <- 'step2'
gbd_round_id <- 7
bundle_version_id <- 22562
draws <- paste0("draw_", 0:999)
cause_name <- cause_path <- "278_gbs_impairment"


path_to_data <- paste0("FILEPATH")
description <- "2020 step2 sex split data, no age split as no data needs, no xwalk as GBS has none"

#File/Dir paths
mrbrt_helper_dir <- paste0("FILEPATH")
dem_dir <- paste0("FILEPATH")

##################################################################################################
source("FILEPATH/get_bundle_data.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/save_crosswalk_version.R")


functs <- c("run_mr_brt_function", "cov_info_function", "predict_mr_brt_function",
            "check_for_outputs_function", "check_for_preds_function", "load_mr_brt_outputs_function",
            "load_mr_brt_preds_function")
for (funct in functs){
  source(paste0(mrbrt_helper_dir, funct, ".R"))
}


##################################################################################################
#Function definitions:

get_row <- function(n, dt){
  row <- copy(dt)[n]
  pops_sub <- copy(pops[location_id == row[, location_id] & year_id == row[, midyear] &
                          age_group_years_start >= row[, age_start] & age_group_years_end <= row[, age_end]])
  agg <- pops_sub[, .(pop_sum = sum(population)), by = c("sex")]
  row[, `:=` (male_N = agg[sex == "Male", pop_sum], female_N = agg[sex == "Female", pop_sum],
              both_N = agg[sex == "Both", pop_sum])]
  return(row)
}


graph_predictions <- function(dt){
  #Set measure to incidence, prevalence, or proportion depending on model
  graph_dt <- copy(dt[measure == "prevalence", .(age_start, age_end, mean, male_mean, male_standard_error, female_mean, female_standard_error)])
  graph_dt_means <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_mean", "female_mean"))
  graph_dt_means[variable == "female_mean", variable := "Female"][variable == "male_mean", variable := "Male"]
  graph_dt_error <- melt(graph_dt, id.vars = c("age_start", "age_end", "mean"), measure.vars = c("male_standard_error", "female_standard_error"))
  graph_dt_error[variable == "female_standard_error", variable := "Female"][variable == "male_standard_error", variable := "Male"]
  setnames(graph_dt_error, "value", "error")
  graph_dt <- merge(graph_dt_means, graph_dt_error, by = c("age_start", "age_end", "mean", "variable"), allow.cartesian = TRUE)
  graph_dt[, N := (mean*(1-mean)/error^2)]
  graph_dt <- subset(graph_dt, value > 0)
  wilson <- as.data.table(binconf(graph_dt$value*graph_dt$N, graph_dt$N, method = "wilson"))
  graph_dt[, `:=` (lower = wilson$Lower, upper = wilson$Upper)]
  graph_dt[, midage := (age_end + age_start)/2]
  ages <- c(60, 70, 80, 90)
  graph_dt[, age_group := cut2(midage, ages)]
  gg_sex <- ggplot(graph_dt, aes(x = mean, y = value, color = variable)) +
    geom_point() +
    geom_errorbar(aes(ymin = lower, ymax = upper)) +
    facet_wrap(~midage) +
    labs(x = "Both Sex Mean", y = " Sex Split Means") +
    geom_abline(slope = 1, intercept = 0) +
    ggtitle("Sex Split Means Compared to Both Sex Mean") +
    scale_color_manual(name = "Sex", values = c("Male" = "midnightblue", "Female" = "purple")) +
    theme_classic()
  return(gg_sex)
}

## CALCULATE STD ERROR BASED ON UPLOADER FORMULAS
get_se <- function(raw_dt){
  dt <- copy(raw_dt)
  dt[is.na(standard_error) & !is.na(lower) & !is.na(upper), standard_error := (upper-lower)/3.92]
  z <- qnorm(0.975)
  dt[is.na(standard_error) & measure == "proportion", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "prevalence", standard_error := sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]
  dt[is.na(standard_error) & measure == "incidence" & cases < 5, standard_error := ((5-mean*sample_size)/sample_size+mean*sample_size*sqrt(5/sample_size^2))/5]
  dt[is.na(standard_error) & measure == "incidence" & cases >= 5, standard_error := sqrt(mean/sample_size)]
  return(dt)
}
##################################################################################################

bundle_278_version <- get_bundle_version(bundle_version_id = bundle_version_id,
                                         fetch = "new")
dt <- copy(bundle_278_version)
dt <- dt[group_review==1 | is.na(group_review), ]
dt[age_start == 99 & age_end == 124, age_end := 99] #Fixing age ranges 

tosplit_dt <- as.data.table(copy(dt))
nosplit_dt <- tosplit_dt[sex %in% c("Male", "Female")] 
tosplit_dt <- tosplit_dt[sex == "Both"] 
tosplit_dt[, midyear := floor((year_start + year_end)/2)] #Setting mid year
pred_draws <- as.data.table(read.csv("FILEPATH"))
pred_draws[, c("X_intercept", "Z_intercept") := NULL]
pred_draws[, (draws) := lapply(.SD, exp), .SDcols = draws]
ratio_mean <- round(pred_draws[, rowMeans(.SD), .SDcols = draws], 2)
ratio_se <- round(pred_draws[, apply(.SD, 1, sd), .SDcols = draws], 2)

pops <- get_mort_outputs(model_name = "population single year", model_type = "estimate", demographic_metadata = T,
                         year_ids = tosplit_dt[, unique(midyear)], location_ids = tosplit_dt[, unique(location_id)])

pops[age_group_years_end == 125, age_group_years_end := 99]

tosplit_dt <- rbindlist(parallel::mclapply(1:nrow(tosplit_dt), function(x) get_row(n = x, dt = tosplit_dt), mc.cores = 9))
tosplit_dt <- tosplit_dt[!is.na(both_N)]
tosplit_dt[, merge := 1]
pred_draws[, merge := 1]
split_dt <- merge(tosplit_dt, pred_draws, by = "merge", allow.cartesian = T)
split_dt[, paste0("male_", 0:999) := lapply(0:999, function(x) mean * (both_N/(male_N + (get(paste0("draw_", x)) * female_N))))]
split_dt[, paste0("female_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * get(paste0("male_", x)))]
split_dt[, male_mean := rowMeans(.SD), .SDcols = paste0("male_", 0:999)]
split_dt[, female_mean := rowMeans(.SD), .SDcols = paste0("female_", 0:999)]
split_dt[, male_standard_error := apply(.SD, 1, sd), .SDcols = paste0("male_", 0:999)]
split_dt[, female_standard_error := apply(.SD, 1, sd), .SDcols = paste0("female_", 0:999)]
split_dt[, c(draws, paste0("male_", 0:999), paste0("female_", 0:999)) := NULL]
male_dt <- copy(split_dt)
male_dt[, `:=` (mean = male_mean, standard_error = male_standard_error, upper = NA, lower = NA,
                cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Male",
                note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                      ratio_se, ")"))]
male_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
male_dt <- dplyr::select(male_dt, names(dt))
female_dt <- copy(split_dt)
female_dt[, `:=` (mean = female_mean, standard_error = female_standard_error, upper = NA, lower = NA,
                  cases = NA, sample_size = NA, uncertainty_type_value = NA, sex = "Female",
                  note_modeler = paste0(note_modeler, " | sex split with female/male ratio: ", ratio_mean, " (",
                                        ratio_se, ")"))]
female_dt[specificity == "age", specificity := "age,sex"][specificity == "total", specificity := "sex"]
female_dt <- dplyr::select(female_dt, names(dt))
total_dt <- rbindlist(list(nosplit_dt, female_dt, male_dt))

predict_sex <- list(final = total_dt, graph = split_dt)


pdf(paste0(dem_dir, cause_name, "_sex_split_graph.pdf"))
graph_predictions(predict_sex$graph)
dev.off()

write.csv(total_dt, paste0(dem_dir, cause_name, "_sex.csv"), row.names = F)

#End of sex split but as there is no new data that needs to age split or age-sex split and GBS has no xwalks we can just upload the data.

write.xlsx(total_dt, file = path_to_data, sheetName="extraction")

#From here we need to run the MAD outlier code as GBS has MAD outliering.