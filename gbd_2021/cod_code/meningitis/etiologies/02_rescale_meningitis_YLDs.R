#' @author USERNAME
#' @date 2022/07/14
#' @description rescale the YLDs for the different etiologies of meningitis to account for lower long term sequela in viral

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- paste0("FILEPATH")
} else { 
  j <- "FILEPATH"
  h <- "FILEPATH"
}

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
parser$add_argument("--location", help = "specify location_id", default = NULL, type = "integer")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)

# SET OBJECTS -----------------------------------------------------------
# Most updated version of meningitis nonfatal sequela
date_nonfatal_in <- "2022_06_27"
# run this for meningitis only
c_id <- 332
tmp_dir <- paste0("FILEPATH")
pull_dir_06a <- file.path("FILEPATH")

# Get the groups as the list of sequela
groups <- unique(list.files(pull_dir_06a))

# Pull the "YLDs" (estimated YLDs, calculated in 06a)
sequela_list <- lapply(groups, function(g){
  ylds <- fread(file.path(pull_dir_06a, g, paste0(location, "_yld.csv")))
  ylds <- ylds[measure_id == 5]
  ylds$grouping <- g
  return(ylds)
})
all_seq <- rbindlist(sequela_list, use.names = TRUE, fill = TRUE)

# For each of bacterial and viral
# Sum all of the sequela YLDs...
id_cols <- c("measure_id", "location_id", "year_id", "age_group_id", "sex_id")
all_seq_sum <- all_seq[, lapply(.SD, sum, na.rm = TRUE), by = c(id_cols, "etiology"), .SDcols = paste0("draw_", 0:999)]
all_seq_vir_plus_bacterial <- all_seq[, lapply(.SD, sum, na.rm = TRUE), by = id_cols, .SDcols = paste0("draw_", 0:999)]

# Take the % that is viral: viral / (viral + bacterial)
etiology_pct <- merge(all_seq_sum, all_seq_vir_plus_bacterial, by = id_cols)
viral_pct <- etiology_pct[etiology == "viral"]
viral_pct[, paste0("draw_",0:999) := lapply(0:999, function(x) {get(paste0("draw_", x, ".x")) / get(paste0("draw_",x, ".y"))})]
# Woohoo! That's our viral YLD PAF! 
viral_pct[,c(paste0("draw_",0:999, ".x"), paste0("draw_",0:999, ".y"), "measure_id", "etiology") := NULL]

# Do the same for bacterial
bacterial_pct <- etiology_pct[etiology == "bacterial"]
bacterial_pct[, paste0("draw_",0:999) := lapply(0:999, function(x) {get(paste0("draw_", x, ".x")) / get(paste0("draw_",x, ".y"))})]
bacterial_pct[,c(paste0("draw_",0:999, ".x"), paste0("draw_",0:999, ".y"), "measure_id") := NULL]

# Read in the etiology fractions
eti_dir <- file.path(out_dir)
# Only meningitis
dim.dt <- fread(file.path(code_dir, paste0("etio_dimension.csv")))
info <- subset(dim.dt, cause_id %like% c_id)

etio_list <- lapply(unique(info$pathogen), function(p){
  # special way viral is named
  if(p == "virus") p <- paste0(p, "_", c_id)
  # read result for each etiology created in amr_result_upload
  etio <- fread(file.path(eti_dir, "original", p, paste0(location, ".csv")))
  etio <- etio[cause_id == c_id]
  # ylds only for the rescale
  etio <- etio[measure_id == 3]
  return(etio)
})
etio <- rbindlist(etio_list, use.names = TRUE, fill = TRUE)

viral_etio <- etio[pathogen == "virus"]
bacterial_etio <- etio[pathogen != "virus", lapply(.SD, sum, na.rm = TRUE), by = id_cols, .SDcols = paste0("draw_", 0:999)]

# Rescale the etiology fractions to the bacterial envelope
# Etio frac * (new envelope / original bacterial etiology sum envelope) = rescaled fraction
# New envelope, i.e., YLD envelope = bacterial_pct
# Old envelope, i.e., case envelope = bacterial_etio

# Read in the etiology fractions
etio_scale <- etio[pathogen != "virus"]
setnames(bacterial_pct, paste0("draw_", 0:999), paste0("new_yld_envelope_", 0:999))
setnames(bacterial_etio, paste0("draw_", 0:999), paste0("old_case_envelope_", 0:999))
id_cols <- c("location_id", "year_id", "age_group_id", "sex_id")
oldnew <- merge(bacterial_pct, bacterial_etio, by = id_cols)
etio_scale <- merge(etio_scale, oldnew, by = c(id_cols, "measure_id"))
# Do the calc
etio_scale[, paste0("draw_",0:999) := lapply(0:999, function(x) {
  get(paste0("draw_", x)) * get(paste0("new_yld_envelope_", x)) / get(paste0("old_case_envelope_",x))})]

# Fix formatting
etio_scale[,c("etiology", paste0("new_yld_envelope_", 0:999), paste0("old_case_envelope_", 0:999)) := NULL]

# Now for viral
viral_pct <- merge(viral_pct, viral_etio[,c(id_cols, setdiff(names(viral_etio), names(viral_pct))), with = FALSE], by = id_cols)

# Write them out. Only replace with scaled for the YLDs for meningitis (keep YLLs & LRI)
etio_scale <- rbind(etio_scale, viral_pct, fill = TRUE)
lapply(unique(info$pathogen), function(p){
  # replace YLD meningitis w new rescaled
  etio_new <- etio_scale[pathogen == p]
  # rename viral
  if(p == "virus") p <- paste0(p, "_", c_id)
  # read result for each etiology created in amr_result_upload
  etio <- fread(file.path(eti_dir, "original", p, paste0(location, ".csv")))
  # keep YLLs & LRI
  etio <- etio[cause_id != c_id | measure_id == 4]
  # looping through all pathogens
  etio <- rbind(etio, etio_new, fill = TRUE)
  # new out directory
  out_dir <- file.path(eti_dir, "scaled", p)
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  fwrite(etio, file.path(out_dir, paste0(location, ".csv")))
})


