###########################################################
### Author: USER
### Date: 3/12/2025
### Project: GBD Nonfatal Estimation
### Purpose: Epilepsy Splits over time diagnostics
### Lasted edited: USER 10 June 2024
###########################################################

rm(list=ls())
user <- Sys.info()[['user']]
date <- gsub("-", "_", Sys.Date())

# ---LOAD PACKAGES--------------------------------------------------------------

library(data.table)
library(ggplot2)
library(lme4)
library(arm)
library(boot)
library(haven)
library(readr)
library(merTools)
library(RColorBrewer)
library(openxlsx)

# ---SET FILE PATHS-------------------------------------------------------------

j_root <- "FILEPATH"
share_path <- "FILEPATH"
functions <- "FILEPATH"
all_output <- "FILEPATH"
output <- paste0(all_output, date, "_GBD_2023/")
csv_input <- "FILEPATH"
splits_bundle_path <- "FILEPATH"

# ---SET OBJECTS----------------------------------------------------------------

set.seed(98736)
draws <- paste0("draw_", 0:999)
# splits_bundle <- 1346
release_id <- 16

# ---CENTRAL FUNCTIONS----------------------------------------------------------

source(paste0(functions, "get_covariate_estimates.R"))
source(paste0(functions, "get_location_metadata.R"))
source(paste0(functions, "get_outputs.R"))

# ---DEFINE FUNCTIONS-----------------------------------------------------------

format_data <- function(dt){
  dt[, year_id := floor((year_start + year_end)/2)]
  dt <- dt[group_review == 1 | is.na(group_review)] 
  dt[, cases := sum(cases), by = c("nid", "year_id", "location_id")]
  dt[, sample_size := sum(sample_size), 
     by = c("nid", "year_id", "location_id")]
  dt[, mean := cases/sample_size]
  dt <- unique(dt, by = c("nid", "year_id", "location_id")) 
  dt <- merge(dt, covs, by= c("location_id", "year_id"))
  return(dt)
}

graph_data <- function(input, preds){
  input <- input[, .(year_id, location_id, super_region_name, region_name, mean)]
  input[, type := "input"]
  preds[, mean := rowMeans(.SD), .SDcols = draws]
  preds[, (draws) := NULL]
  preds[, type := "prediction"]
  preds <- merge(preds, super_region_map, by = "location_id")
  graph_data <- rbind(preds, input, fill = TRUE)
  return(graph_data)
}

graph <- function(dt, split){
  scale_color <- colorRampPalette(
    brewer.pal(8, "RdYlBu"))((length(unique(dt$region_name))))
  names(scale_color) <- sort(unique(dt$region_name))
  pdf(paste0(output, split, "_overtime.pdf"), width = 15)
  gg <- ggplot(dt, aes(x = year_id, y = mean)) + 
    geom_line(data = dt[type == "prediction"], 
              aes(group = location_id, color = region_name)) +
    scale_color_manual(values = scale_color, guide = FALSE) +
    geom_point(data = dt[type == "input"], 
               aes(fill = region_name), size = 4, pch = 21) +
    scale_fill_manual(name = "Region", values = scale_color) +
    facet_wrap(~super_region_name) +
    theme_bw()
  print(gg)
  dev.off()
}

# ---GET LOCATION METADATA------------------------------------------------------

print("Getting locations")
locations <- get_location_metadata(location_set_id = 35, 
                                   release_id = release_id) 

gglocations <- locations[!location_id==1, .(location_id, 
                                            location_name, 
                                            parent_id, 
                                            super_region_name, 
                                            region_name, 
                                            most_detailed)]
most_detailed <- locations[most_detailed == 1]
super_region_map <- locations[, .(location_id, region_name, super_region_name)]

# ---GET COVARIATES-------------------------------------------------------------

print("Getting covariates")
haqi <- get_covariate_estimates(covariate_id = 1099, release_id = release_id)
setnames(haqi, "mean_value", "haqi")
haqi <- haqi[,.(location_id, year_id, haqi)] 

pigmeat <- get_covariate_estimates(covariate_id = 100, release_id = release_id)
setnames(pigmeat, "mean_value", "pigmeat")
pigmeat[, log_pig := log(pigmeat)]
pigmeat <- pigmeat[,.(location_id, year_id, pigmeat, log_pig)]

sanitation <- get_covariate_estimates(covariate_id = 142, release_id = release_id)
setnames(sanitation, "mean_value", "sanitation")
sanitation <- sanitation[, .(location_id, year_id, sanitation)]

muslim <- get_covariate_estimates(covariate_id = 1218, release_id = release_id)
setnames(muslim, "mean_value", "muslim")
muslim <- muslim[, .(location_id, year_id, muslim)]

# ---GET UNDER FIVE MORTALITY---------------------------------------------------

under_5 <- get_outputs(topic = "cause", 
                       age_group_id = 1, 
                       sex_id = 3, 
                       year_id = 2023,
                       metric_id = 3, 
                       location_id = "all", 
                       location_set_id = 35, 
                       release_id= 16,
                       compare_version_id  = 8055,
                       cause_set_id =2)

setnames(under_5, "val", "mortality")
under_5 <- merge(under_5, locations, by = "location_id", all = TRUE)
for (id in under_5[is.na(mortality), unique(parent_id)]){
  val <- under_5[location_id == id, mortality]
  under_5[is.na(mortality) & parent_id == id, mortality := val]
}
under_5[, log_mortality := log(mortality)]
under_5 <- under_5[, .(location_id, mortality, log_mortality)]

# ---GET EPILEPSY DEATHS--------------------------------------------------------

epilepsy <- get_outputs(topic = "cause", 
                        age_group_id = 27, 
                        sex_id = 3, 
                        year_id = 2023,
                        metric_id = 3, 
                        location_id = "all", 
                        location_set_id = 35, 
                        release_id= 16,
                        compare_version_id  = 8055,
                        cause_set_id = 2,
                        cause_id = 545)

setnames(epilepsy, "val", "epilepsy")
epilepsy <- merge(epilepsy, locations, by = "location_id", all = TRUE)
for (id in epilepsy[is.na(epilepsy), unique(parent_id)]){
  val <- epilepsy[location_id == id, epilepsy]
  epilepsy[is.na(epilepsy) & parent_id == id, epilepsy := val]
}
epilepsy[, log_epilepsy := log(epilepsy)]
epilepsy <- epilepsy[, .(location_id, epilepsy, log_epilepsy)]

merge_dts <- list(haqi, pigmeat, sanitation, muslim)
covs <- Reduce(function(...) merge(..., all = TRUE, by = c("location_id", "year_id")), merge_dts)
merge_dts2 <- list(covs, under_5, epilepsy, locations)
covs <- Reduce(function(...) merge(..., by = c("location_id")), merge_dts2)

# ---GET PROPORTION DATA--------------------------------------------------------

print("Getting data")
splits_data <- as.data.table(openxlsx::read.xlsx(splits_bundle_path, na.strings = ""))
idiopathic_prop <- splits_data[split == "idiopathic"]
severe_prop <- splits_data[split == "severe"]
tg_prop <- splits_data[split == "tg"]
treated_nf_prop <- splits_data[split == "tnf"]

# ---IDIOPATHIC REGRESSION------------------------------------------------------

print("Idiopathic regression")
idiopathic_prop[quality==0, quality:=2]
idiopathic_prop[quality==1, quality:=0]
idiopathic_prop[quality==2, quality:=1]
idiopathic_prop <- idiopathic_prop[!exclude == 1] 
idiopathic_prop <- format_data(idiopathic_prop)

quality_model <- glm(mean ~ quality, data = idiopathic_prop, family = "binomial")
beta <- inv.logit(summary(quality_model)$coefficients[2,1]) 
idiopathic_prop[quality == 1, mean := mean * beta] 

idio_preds <- as.data.table(read_rds(paste0(output, "idio_draws.rds")))
idio_dt <- graph_data(idiopathic_prop, idio_preds)
graph(idio_dt, "idio")

# ---SEVERE REGRESSION----------------------------------------------------------

severe_prop <- format_data(severe_prop)
severe_preds <- as.data.table(read_rds(paste0(output, "severe_draws.rds")))
severe_dt <- graph_data(severe_prop, severe_preds)
graph(severe_dt, "severe")

# ---TREATMENT GAP REGRESSION---------------------------------------------------

tg_prop <- format_data(tg_prop)
tg_preds <- as.data.table(read_rds(paste0(output, "tg_draws.rds")))
tg_dt <- graph_data(tg_prop, tg_preds)
graph(tg_dt, "tg")

# ---TREATMENT NO FITS REGRESSION-----------------------------------------------

tnf_prop <- format_data(treated_nf_prop)
tnf_preds <- as.data.table(read_rds(paste0(output, "tnf_draws.rds")))
tnf_dt <- graph_data(tnf_prop, tnf_preds)
graph(tnf_dt, "tnf")