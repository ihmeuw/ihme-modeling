# STH Crosswalk Matching 

### ----------------------- Set-Up ------------------------------

os <- .Platform$OS.type
if (os=="windows") {
  ADDRESS <-FILEPATH
  ADDRESS <-FILEPATH
} else {
  ADDRESS <-FILEPATH
  ADDRESS <-paste0(FILEPATH, Sys.info()[7], "/")
}

source(FILEPATH)
library(data.table)
library(stringr)
library(metafor, lib.loc = FILEPATH)
library(msm, lib.loc = FILEPATH)

repo_dir <- FILEPATH
source(paste0(repo_dir, FILEPATH))
source(paste0(repo_dir, FILEPATH))
source(paste0(repo_dir, FILEPATH))
source(paste0(repo_dir, FILEPATH))
source(paste0(repo_dir, FILEPATH))
source(paste0(repo_dir, FILEPATH))
source(paste0(repo_dir, FILEPATH))
source(paste0(repo_dir, FILEPATH))

## Set-up run directory
run_file <- fread(paste0(FILEPATH))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, FILEPATH)

params_dir <- FILEPATH

### ----------------------- Match ------------------------------

# matching off sheets
worm_sheets <- list(list("ascar", paste0(params_dir, FILEPATH)), 
                    list("trichur", paste0(params_dir, FILEPATH)), 
                    list("hook", paste0(params_dir, FILEPATH)))

for (element in worm_sheets){
  
  worm  <- element[[1]]
  sheet <- element[[2]]
  
  #' [Direct Matches]
  data <- fread(sheet)
  data <- data[sex != "Both"]
  
  # subset out 0 cases
  data <- data[cases != 0]
  data[, match := str_c(age_start, age_end, location_name, year_start, year_end, site_memo)]
  
  dm <- data[, .N, by = c("age_start", "age_end", "location_name", "year_start", "year_end", "site_memo")][N > 1]
  
  cat(paste0("Matches for ", worm, ": ", nrow(dm)))
  
  matches <- dm[, str_c(age_start, age_end, location_name, year_start, year_end, site_memo)]
  
  # subset to the matches 
  data_ss_subset <- data[match %in% matches]
  
  # remove row where both females
  data_ss_subset <- data_ss_subset[site_memo != "Calbico, Calbuco Commune, Llanquihue Province, Chile Region"]
  
  # sort
  setorder(data_ss_subset, year_start, year_end, age_start, age_end, location_name, site_memo)
  
  # correct names
  data_ss_m <- data_ss_subset[sex == "Male", ]
  data_ss_f <- data_ss_subset[sex == "Female",]
  
  # validate -- if all true then ready to clean and prep for mrbrt
  all(data_ss_m[, .(year_start, year_end, age_start, age_end, location_name, site_memo)] == data_ss_f[, .(year_start, year_end, age_start, age_end, location_name, site_memo)])
  
  # mrbrt columns - NID - Author - year - cases_1 - sample_size_1 = mean_1 - se_1 - dx_1 //2's - ratio ref_to_alt - se_ratio - compar - group_id
  mrbrt_sheet <- data.table(
    NID = paste0(data_ss_m[, nid], "-", data_ss_f[, nid]),
    Author = "",
    year = data_ss_m[, year_start],
    cases_1 = data_ss_m[, cases],
    sample_size_1 = data_ss_m[, sample_size],
    mean_1 = data_ss_m[, mean],
    se_1 = data_ss_m[, standard_error],
    dx_1 = "male",
    cases_2 = data_ss_f[, cases],
    sample_size_2 = data_ss_f[, sample_size],
    mean_2 = data_ss_f[, mean],
    se_2 = data_ss_f[, standard_error],
    dx_2 = "female")
  
  fwrite(mrbrt_sheet, paste0(crosswalks_dir, FILEPATH, worm, ".csv"))
  
  #' [MR-BRT -- save model fit as rds object] 
  
  # Load matched sheet
  sex_cw_sheet <- fread(paste0(crosswalks_dir, FILEPATH, worm, ".csv"))
  
  # Log-Transform
  sex_cw_sheet[, ratio := cases_1 / cases_2]
  sex_cw_sheet[, ratio_se := sqrt(ratio*((se_1^2/cases_1^2)+(se_2^2/cases_2^2)))]
  sex_cw_sheet[, ratio_log := log(ratio)]
  
  sex_cw_sheet$ratio_se_log <- sapply(1:nrow(sex_cw_sheet), function(i) {
    ratio_i    <- sex_cw_sheet[i, "ratio"]
    ratio_se_i <- sex_cw_sheet[i, "ratio_se"]
    deltamethod(~log(x1), ratio_i, ratio_se_i^2)
  })
  
  # Fit Model
   fit1 <- run_mr_brt(
     output_dir  = paste0(crosswalks_dir),
     model_label = paste0(FILEPATH, worm),
     data        = sex_cw_sheet,
     #covs        = covs1,
     mean_var    = "ratio_log",
     se_var      = "ratio_se_log",
     #method      = "trim_maxL",
     study_id    = "NID",
     #trim_pct    = 0.00,
     overwrite_previous = TRUE
   )
  
  check_for_outputs(fit1)
  plot_mr_brt(fit1)
  
  readRDS(paste0(crosswalks_dir, paste0(FILEPATH, worm, ".rds")))
  cat(paste0("Saved fit for ", worm))
  
}