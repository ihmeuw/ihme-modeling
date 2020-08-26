#' [Title: FBT Sex Crosswalk Model
#' [Notes: Create All-Species Sex-Split Fit from between species comparisons

# Sex_Split using matching between specicies
# 1) Match 
# 2) MR-BRT

#############################################################################################
###'                                [General Set-Up]                                      ###
#############################################################################################

# clear area
rm(list = ls())

# create root
root <- "FILEPATH"

#'[ 1) create new run id if needed
source(paste0(root, "FILEPATH"))
#gen_rundir(root = "FILEPATH", acause = "ntd_foodborne", message = "GBD 2019 Final")

# packages
source("FILEPATH")
library(data.table)
library(stringr)
library(metafor, lib.loc = "FILEPATH")
library(msm, lib.loc = "FILEPATH")

# mrbrt
repo_dir <- "FILEPATH"
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

## Set-up run directory
run_file <- fread("FILEPATH")
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- "FILEPATH"
params_dir <- "FILEPATH"

#############################################################################################
###'                                  [Match]                                             ###
#############################################################################################

# compile all case data
bADDRESS1 <- fread("FILEPATH")
bADDRESS1[, species := "clono"]

bADDRESS2 <- fread("FILEPATH")
bADDRESS2[, species := "fascio"]

bADDRESS3 <- fread("FILEPATH")
bADDRESS3[, species := "fluke"]

bADDRESS4 <- fread("FILEPATH")
bADDRESS4[, species := "opistho"]

bADDRESS5 <- fread("FILEPATH")
bADDRESS5[, species := "paragon"]

bADDRESS6 <- fread("FILEPATH")
bADDRESS6[, species := "c_paragon"]

col_names <- Reduce(intersect, list(names(bADDRESS1), names(bADDRESS2), names(bADDRESS3), names(bADDRESS4), names(bADDRESS5), names(bADDRESS6)))
all_data <- rbind(bADDRESS1[, ..col_names], bADDRESS2[, ..col_names], bADDRESS3[, ..col_names], bADDRESS4[, ..col_names], bADDRESS5[, ..col_names], bADDRESS6[, ..col_names])

#' [Direct Matches]
all_data <- all_data[sex != "Both"]

# subset out 0 cases here so pair does not end up in
all_data <- all_data[cases != 0]
all_data[, match := str_c(age_start, age_end, location_name, year_start, year_end, species)]


dm <- all_data[, .N, by = c("age_start", "age_end", "location_name", "year_start", "year_end", "species")][N == 2]
matches <- dm[, str_c(age_start, age_end, location_name, year_start, year_end, species)]

# subset to the matches 
data_ss_subset <- all_data[match %in% matches]

# sort
setorder(data_ss_subset, year_start, year_end, age_start, age_end, location_name, species)

# correct names
data_ss_m <- data_ss_subset[sex == "Male", ]
data_ss_f <- data_ss_subset[sex == "Female",]

# validate -- if all true then ready to clean and prep 
all(data_ss_m[, .(year_start, year_end, age_start, age_end, location_name, species)] == data_ss_f[, .(year_start, year_end, age_start, age_end, location_name, species)])

# mrbrt columns: NID - Author - year - cases_1 - sample_size_1 = mean_1 - se_1 - dx_1 //2's - ratio ref_to_alt - se_ratio - compar - group_id
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

model_name <- "all_species_no_trim"

fwrite(mrbrt_sheet, "FILEPATH"))

#############################################################################################
###'                                   [Fit]                                              ###
#############################################################################################

#' [MR-BRT -- save model fit as rds object] 
# Load matched sheet
sex_cw_sheet <- fread("FILEPATH"))

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
    output_dir  = crosswalks_dir,
    model_label = model_name,
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

# save out object
saveRDS(fit1, "FILEPATH")
