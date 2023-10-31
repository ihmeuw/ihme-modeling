##################################################
## Author: USERNAME
## Project: CVPDs
## Script purpose: Adjust measles codcorrected fatal draws 
## Date: June 16, 2021
##################################################
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2)
username <- Sys.info()[["user"]]
gbd_round_id <- 7
decomp_step <- "step3"

# SOURCE SHARED FUNCTIONS ------------------------------
shared_functions <- "FILEPATH"
functions <- c("get_location_metadata.R", "get_outputs.R", "get_draws.R", "get_age_metadata.R", "get_population.R", "get_model_results.R")
for (func in functions) {
 source(paste0(shared_functions, func))
}

# ARGUMENTS -------------------------------------  
args <- commandArgs(trailingOnly = TRUE)

# arguments
codcorrect_version <- args[[1]] %>% as.integer() # covidfree codcorrect vers
covidfree_custom_version <- args[[2]] # covidfree model version to use with covidfree codcorrect draws to calc implied CFR. should have same setting as current model except for covid adjustment
custom_version <- args[[3]] # the current model version, where the cod scalars should be saved and the version to read in covid inclusive age/sex/loc specific NF draws from to calc covid incl deaths
fatal_decomp_step <- args[[4]]
gbd_round <- args[[5]]
release_id <- args[[6]]

date <- gsub("-", "_", Sys.Date())


home <- file.path("FILEPATH/00_documentation")
j.version.dir <- file.path(home, "models", custom_version)
j.version.dir.logs <- file.path(j.version.dir, "model_logs")
j.version.dir.inputs <- file.path(j.version.dir, "model_inputs")


# gbd central objects
hierarchy <- get_location_metadata(22, gbd_round_id = gbd_round, decomp_step = fatal_decomp_step)
draw_names <- paste0("draw_", 0:999)

# read in age and sex specific covidfree incidence rate for 2020
nfdraw_dir <- (paste0("FILEPATH", covidfree_custom_version, "/draws/"))
covidfree_cases <- lapply(list.files(nfdraw_dir, full.names = T), fread) %>% rbindlist() %>% .[year_id == 2020 & measure_id == 6]
covidfree_cases[, cause_id := 341]

population <- get_population(location_id=unique(covidfree_cases$location_id), year_id= 2020, age_group_id=c(2:3, 388, 389, 238, 34, 6:20, 30:32, 235), 
                             status="best", sex_id=1:3, gbd_round_id=gbd_round, decomp_step=decomp_step)

covidfree_cases <- merge(covidfree_cases, population, by = c("location_id", "sex_id", "age_group_id", "year_id"))
covidfree_cases[, (draw_names) := lapply(.SD, function(x) x*population), .SDcols = draw_names]

cf_cases_long <- melt(covidfree_cases, 
                      id.vars = setdiff(names(covidfree_cases), grep("draw_", names(covidfree_cases), value = T)),
                      value.name = "nocovid_cases")
# pull codcorrected covidfree death counts draws
nocovid_deaths <- get_draws(gbd_id_type = "cause_id",
                            gbd_id = 341,
                            source = "codcorrect",
                            year_id = 2020, 
                            measure_id = 1,
                            metric_id = 1,
                            version_id = codcorrect_version,
                            gbd_round_id = gbd_round,
                            decomp_step = fatal_decomp_step)

cf_deaths_long <- melt(nocovid_deaths, 
                       id.vars = setdiff(names(nocovid_deaths), grep("draw_", names(nocovid_deaths), value = T)),
                       value.name = "nocovid_deaths")

# merge, calculate implied age, sex, location specific CFR
implied_cfr <- merge(cf_cases_long, cf_deaths_long, by = c("location_id", "year_id", "age_group_id", "sex_id", "cause_id", "variable")) # fewer rows that cf_cases_long or cf_deaths_long because cases includes new subnats that aren't being reported for GBD20 and deaths includes draws for gbd age grps that we don't model in measles (all 0s for these age grps)
implied_cfr[, implied_cfr := nocovid_deaths/nocovid_cases]


# when have nonzero codcorrected death draws but 0 cases (leads to inf CFR), set to 0 CFR
nrow(implied_cfr[nocovid_cases<nocovid_deaths & nocovid_cases == 0]) 
max(implied_cfr[nocovid_cases<nocovid_deaths & nocovid_cases == 0, nocovid_deaths]) #0.96773
implied_cfr[nocovid_cases<nocovid_deaths & nocovid_cases == 0, implied_cfr := 0]

# implied cfr is NA if cases AND deaths are 0; set to 0 also so that when calculate covid inclusive deaths will get 0
implied_cfr[is.na(implied_cfr) & nocovid_cases==0 & nocovid_deaths == 0, implied_cfr := 0]


# pull in covid inclusive age and sex specific incidence, convert to counts
covid_incl_dir <- (paste0("FILEPATH", custom_version, "/draws/"))
ci_split_cases <- lapply(list.files(covid_incl_dir, full.names = T), fread) %>% rbindlist() %>% .[year_id == 2020 & measure_id == 6]
ci_split_cases <- merge(ci_split_cases, population)
ci_split_cases[, (draw_names) := lapply(.SD, function(x) x*population), .SDcols = draw_names]
ci_cases_long <- melt(ci_split_cases, 
                      id.vars = setdiff(names(ci_split_cases), grep("draw_", names(ci_split_cases), value = T)),
                      value.name = "ci_cases")
ci_cases_long[, cause_id := 341]

# merge on draws of implied CFR and calcuate covid inclusive death counts
dt <- merge(ci_cases_long, implied_cfr, by = c("location_id", "year_id", "age_group_id", "sex_id", "cause_id", "variable"))
dt[, ci_deaths := ci_cases * implied_cfr]

# calculate ratio draw-wise from codcorrected covidfree death counts and the manually caclualted covid inclusive death counts, send this as shocks to CoD
dt[, fatal_adjustment_ratio := ci_deaths/nocovid_deaths]

# when nocovid_deaths = 0 (and thus implied cfr is 0 so ci_deaths is also 0) ratio is NA. Set adjustment ratio to 1 because no change from deaths with and without covid.
dt[is.na(fatal_adjustment_ratio) & nocovid_deaths == 0, fatal_adjustment_ratio := 0]

# check mean of draws of fatal ratio and see where > 1 (expect mean adj ratio > 1 in trusted locs where covid cases > expected)
mean_adj_ratio <- dt[,.(mean_fatal_adj_ratio = mean(fatal_adjustment_ratio)), by = c("location_id", "sex_id", "year_id", "age_group_id")]

# save dt, save covidfree_codcorrect version, save covidfree custom version
fwrite(dt, file.path(j.version.dir, "05b_implied_cfr.csv"), row.names = FALSE)
cat(paste0("COVID free codcorrect version:  ", codcorrect_version),
    file=file.path(j.version.dir.logs, "covid_adjustment_input_model_version_ids.txt"), sep="\n")
cat(paste0("COVID free custom NF version:  ", covidfree_custom_version),
    file=file.path(j.version.dir.logs, "covid_adjustment_input_model_version_ids.txt"), sep="\n", append = TRUE)

# prep to save and pass to CoD -- cast draws wide, add rows for missing age grps for all locs
final <- dcast(dt, location_id + year_id + age_group_id + sex_id + cause_id ~ variable, value.var = "fatal_adjustment_ratio")

# keep only most detailed locations
final <- merge(final, hierarchy[,.(location_id, most_detailed)], by = "location_id")
final_to_write <- final[most_detailed == 1][, most_detailed := NULL]
# pad unmodeled age groups with 0s as CoD requested (draws for these age grps are already zeros and want scalar of 0 so stay 0s)
ages <- get_age_metadata(release_id = release_id)
not_modeled_ages <- expand.grid(location_id = unique(final_to_write$location_id),
                            age_group_id = setdiff(ages$age_group_id, unique(final_to_write$age_group_id)),
                            year_id = 2020,
                            cause_id = 341,
                            sex_id = unique(final_to_write$sex_id)) %>% data.table()
not_modeled_ages[, (draw_cols_gbd) := 0]

final_to_write <- rbind(final_to_write, not_modeled_ages)
fwrite(final_to_write, paste0("/FILEPATH", date, "_measles.csv"), row.names = FALSE)

########################## Save for CoD ####################################
# Save in directory that CoD will read from, by locations
cod_save_dir <- paste0("/FILEPATH/", unique(dt$cause_id))
if(!dir.exists(cod_save_dir)) dir.create(cod_save_dir)

system.time(lapply(unique(final_to_write$location_id), function(x) fwrite(final_to_write[location_id==x, ],
                                                                      file.path(cod_save_dir, paste0(x, ".csv")), row.names=FALSE)))

