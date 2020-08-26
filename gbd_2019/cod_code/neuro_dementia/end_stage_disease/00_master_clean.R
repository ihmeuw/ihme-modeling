##########################################################################
### Project: GBD Nonfatal Estimation
### Purpose: Master Script for Calculating Code Differential
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
library(mortcore, lib = "FILEPATH")
date <- gsub("-", "_", Sys.Date())

# SET UP OBJECTS ----------------------------------------------------------

italy_folder <- paste0("FILEPATH")
graphs_dir <- paste0("FILEPATH")
results_dir <- paste0("FILEPATH")
repo_dir <- paste0("FILEPATH")
code_dir <- paste0("FILEPATH")
functions_dir <- paste0("FILEPATH")
save_dir <- paste0("FILEPATH", date)
dir.create(save_dir)
save_dir <- paste0("FILEPATH")

# SOURCE FUNCTIONS --------------------------------------------------------

source(paste0(functions_dir, "get_ids.R"))
source(paste0(functions_dir, "get_age_metadata.R"))

# DATA PREP CODE ----------------------------------------------------------

source(paste0(repo_dir, "italy_linkage_prep.R"))

# GET CODES ---------------------------------------------------------------

code_cols <- paste0("diag", 1:6)
code_dt <- melt(hosp, id.vars = "sid", measure.vars = code_cols)
code_dt <- code_dt[!is.na(value)]
code_dt[, n := .N, by = "value"]
code_dt <- code_dt[n >= 500]
code_dt <- unique(code_dt, by = "value")
params <- data.table(code = code_dt[, value])
params[, task_num := 1:.N]
map_path <- paste0(save_dir, "task_map.csv")
write.csv(params, map_path, row.names = F)

# SUBMIT JOBS -------------------------------------------------------------

array_qsub(jobname = "end_stage",
           shell = "ADDRESS",
           code = paste0(code_dir, "01_calccodes.R"),
           pass = list(map_path, save_dir),
           proj = "proj_custom_models",
           num_tasks = nrow(params),
           slots = 3, log = T, submit = T)

# COMPILE -----------------------------------------------------------------

files <- list.files(save_dir)

compile_files <- function(num){
  dt <- readr::read_rds(paste0(save_dir, files[num]))
  return(dt)
}

results <- rbindlist(parallel::mclapply(1:length(files), compile_files, mc.cores= 9))
results <- merge(results, icd9[, .(value, code_name)], by.x = "icdcode", by.y = "value", all.x = T)

# CALCULATE DIFFERENCE ----------------------------------------------------

dif_dt <- dcast(results, icdcode + code_name + months ~ dem, value.var = "mean")
setnames(dif_dt, c("0", "1"), c("no_dem", "dem"))
dif_dt[, dif := dem - no_dem]
dif_dt <- dif_dt[order(-dif)]

## WIDE DIF DT TO IDENTIFY TOP CODES
back_dt <- dcast(dif_dt, icdcode + code_name ~ months, value.var = "dif")
setnames(back_dt, c("1", "3", "6", "12"), c("month1", "month3", "month6", "month12"))
back_dt <- back_dt[order(-month12)]
write.csv(back_dt, paste0("FILEPATH"), row.names = F)
top_codes <- back_dt[, icdcode][1:100]

# CREATE GRAPHS -----------------------------------------------------------

graph_means_noage <- function(num){
  code <- top_codes[num]
  dt <- copy(results[icdcode == code])
  name <- dt[, unique(code_name)]
  gg <- ggplot(dt, aes(x = as.factor(months), y = mean, fill = as.factor(dem))) +
    geom_bar(position = "dodge", stat = "identity") +
    labs(y = "Percent", x = "Months Before Death") +
    ylim(0, 0.12) +
    scale_fill_manual(name = "Dementia Status", values = c("midnightblue", "mediumpurple1"), labels = c("No Dementia", "Dementia")) +
    ggtitle(stringr::str_wrap(paste0("Percent of Individuals who Died with ", name), width = 60)) +
    theme_classic()
  return(gg)
}

pdf(paste0("FILEPATH"))
lapply(1:length(top_codes), graph_means_noage)
dev.off()
