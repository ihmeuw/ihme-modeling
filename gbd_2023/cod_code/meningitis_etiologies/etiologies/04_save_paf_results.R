#' @author USERNAME
#' @date 2022/07/14
#' @description upload YLL/YLD PAFs for LRI & meningitis

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- paste0("FILEPATH")
} else { 
  j <- "FILEPATH"
  h <- "FILEPATH"
}

# Source all GBD shared functions at once
invisible(sapply(list.files("FILEPATH", full.names = T), source))

## Load packages
pacman::p_load(data.table, ggplot2, dplyr, parallel, argparse, pbapply)


# Get arguments from parser
parser <- ArgumentParser()
parser$add_argument("--meid", help = "modelable entity id for upload", default = NULL, type = "integer")
parser$add_argument("--etiology", help = "pathogen for upload", default = NULL, type = "character")
parser$add_argument("--cause_id", help = "cause_id of results being read from AMR", default = NULL, type = "integer")
parser$add_argument("--in_dir", help = "in directory for AMR saved results", default = NULL, type = "character")
parser$add_argument("--out_dir", help = "out directory for formatted results", default = NULL, type = "character")
parser$add_argument("--code_dir", help = "repository directory, has a csv with dimensions/IDs", default = NULL, type = "character")
parser$add_argument("--ds", help = "specify decomp step", default = 'step4', type = "character")
parser$add_argument("--gbd_round_id", help = "specify gbd round", default = 7L, type = "integer")
parser$add_argument("--desc", help = "upload description", default = as.character(gsub("-", "_", Sys.Date())), type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)

# SAVE RESULTS ------------------------------------------------------------
# Check that all expected causes are present
dim.dt <- fread(file.path(code_dir, paste0("etio_dimension.csv")))
info <- subset(dim.dt, me_id == meid)
causes <- unlist(strsplit(info$cause_id, split=","))
# Naming exception for viral
etiology <- info$pathogen
if(etiology == "virus") etiology <- paste0("virus_", cause_id)
print(etiology)

# Set the out directory based on the cause
# if it is an etiology that includes meningitis: needs adjusted w/ shocks & YLD scalar. if LRI only, needs original
if(!332 %in% causes) out_eti <- file.path(out_dir, "original", etiology)
if(332 %in% causes) out_eti <- file.path(out_dir, "add_shock", etiology)

save_results_risk(input_dir=out_eti,
                  input_file_pattern = "{location_id}.csv",
                  modelable_entity_id=meid,
                  risk_type = "paf",
                  n_draws = 1000,
                  description=desc,
                  mark_best=TRUE,
                  gbd_round_id = gbd_round_id,
                  decomp_step=ds
)
# }



# # Post upload - check that uploaded results match original file
# # Vetting to compare to AMR results
# version <- 722397
# dt <- fread(paste0("FILEPATH"))
# amr_results_summary <- confidence_intervals(amr_results_expanded)
# for(l in unique(dt$location_id)){
#   for(m in unique(dt$measure_id)){
#     for(y in unique(dt$year_id)){
#       for(s in unique(dt$sex_id)){
#         dt_tmp <- dt[location_id == l & measure_id == m & year_id == y & sex_id == s]
#         orig_tmp <- amr_results_summary[location_id == l & year_id == y & sex_id == s & pathogen == "flu"]
#         if(m == 3) orig_tmp <- orig_tmp[measure_id == 6] else if(m == 4)orig_tmp <- orig_tmp[measure_id == 1]
#         test <- all.equal(sort(dt_tmp$mean), sort(orig_tmp$mean))
#         if(test == FALSE) stop("uploads not equal to original")
#       }
#     }
#   }
# }
# 
# age_meta <- get_age_metadata(age_group_set_id = 19, gbd_round_id = 7)
# dt <- merge(dt, age_meta, by = "age_group_id")
# dt$measure <- ifelse(dt$measure_id == 3, "yld", "yll")
# dt$sex <- ifelse(dt$sex_id == 1, "male", "female")
# 
# # Plot the age pattern
# g <- ggplot(data = dt[location_id == 33 & year_id == 2020]) + 
#   geom_line(aes(x = age_group_years_start, y = mean)) +
#   facet_grid(rows = vars(measure), cols = vars(sex))