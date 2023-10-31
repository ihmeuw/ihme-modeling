#' @author USERNAME
#' @date 2022/07/14
#' @description compute mortality proportions for all meningitis etiologies accounting for meningococcal shocks
#'              saves them in subdirectories of out_dir for each etiology for upload
rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH" 
  h <- paste0("FILEPATH")
  k <- "FILEPATH"
} else { 
  j <- "FILEPATH"
  h <- "FILEPATH"
  k <- "FILEPATH"
}

# Load packages
pacman::p_load(data.table, boot, ggplot2, argparse)

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


# SOURCE FUNCTIONS --------------------------------------------------------
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# SET OBJECTS -------------------------------------------------------------
meninigitis_shock_version_ids <- c(710480, 710552) 
codcorrect_version <- 311
n_draws <- 1000

men_cause_id <- 332 # used in get_outputs to get CodCorrect deaths

demographics <- get_demographics(gbd_team="epi", 
                                 gbd_round_id=gbd_round_id)
years <- demographics$year_id
# Pull for ALL years: needed to deal with shocks
years <- years[1]:years[length(years)]
sexes <- demographics$sex_id
age_groups <- demographics$age_group_id

# PULL CODCORRECT DEATHS AND SHOCKS ---------------------------------------
men_draws_dt <- get_draws(
                   "cause_id",
                   men_cause_id,
                   source = "codcorrect",
                   measure_id = 1, # deaths
                   metric_id = 1,  # counts
                   location_id = location,
                   age_group_id = age_groups,
                   year_id = years,
                   gbd_round_id = gbd_round_id,
                   decomp_step = "step3", # UPDATE THIS
                   version = codcorrect_version
                )

#replace codcorrect results that are 0 with a tiny value 
men_draws_dt[men_draws_dt == 0] <- .00001



# get shock counts
men_shock_draws_list <- lapply(meninigitis_shock_version_ids, function(mvid) {
  nm_shock_draws <- get_draws(
    gbd_id_type = 'cause_id',
    source = 'codem',
    gbd_id = men_cause_id,
    location_id = location,
    age_group_id = age_groups, 
    year_id = years,
    version_id = mvid,
    gbd_round_id = gbd_round_id,
    decomp_step = ds,
    measure_id = 1, # deaths
    metric_id = 1,  # counts
    downsample = !is.null(n_draws),
    n_draws = n_draws
  )
})
men_shock_draws_dt <- rbindlist(men_shock_draws_list)
cols.remove <- c("envelope", "measure_id", "metric_id", "cause_id", "sex_name")
men_draws_dt[, (cols.remove) := NULL]
men_shock_draws_dt[, (cols.remove) := NULL]

# Read in the etiology fractions
eti_dir <- copy(out_dir)
# Only meningitis
dim.dt <- fread(file.path(code_dir, paste0("etio_dimension.csv")))
info <- subset(dim.dt, cause_id %like% men_cause_id)

etio_list <- lapply(unique(info$pathogen), function(p){
  # special way viral is named
  if(p == "virus") p <- paste0(p, "_", men_cause_id)
  # read result for each etiology created in amr_result_upload
  etio <- fread(file.path(eti_dir, "scaled", p , paste0(location, ".csv")))
  etio <- etio[cause_id == men_cause_id]
  # YLLs only for this process
  etio <- etio[measure_id == 4]
  return(etio)
})
etio <- rbindlist(etio_list, use.names = TRUE, fill = TRUE)

# add shocks to parent meningitis
merge_cols <- c("location_id", "year_id", "sex_id", "age_group_id")
men_draws_dt <- merge(men_draws_dt, men_shock_draws_dt, by = merge_cols)
# Set names to be more informative
setnames(men_draws_dt, c(paste0("draw_", 0:(n_draws-1), ".x"), paste0("draw_", 0:(n_draws-1), ".y")),
                       c(paste0("total_mort_draw_", 0:(n_draws-1)), paste0("shock_mort_draw_", 0:(n_draws-1))))

# squeeze death rates to sum to CODEm deaths + shocks
men_draws_dt[, paste0("no_shock_draw_",0:(n_draws-1)) := lapply(0:(n_draws-1), function(x) {
  get(paste0("total_mort_draw_", x)) - get(paste0("shock_mort_draw_", x))})]
# total_mort_draw_ is the envelope WITH shocks.

# get counts by etiology
setnames(etio, paste0("draw_", 0:(n_draws-1)), paste0("etio_prop_draw_", 0:(n_draws-1)))
etio <- merge(men_draws_dt, etio, by = merge_cols)
# for non-meningococcal is just the proportion (draw_) * the no_shock_draw count
etio[pathogen != "neisseria_meningitidis", 
     paste0("etio_count_draw_",0:(n_draws-1)) := lapply(0:(n_draws-1), function(x) {
     get(paste0("etio_prop_draw_", x)) * get(paste0("no_shock_draw_", x))})]
# for meningococal is the proportion (draw_) * the no_shock_draw count PLUS the shock_draw count
etio[pathogen == "neisseria_meningitidis", 
     paste0("etio_count_draw_",0:(n_draws-1)) := lapply(0:(n_draws-1), function(x) {
     (get(paste0("etio_prop_draw_", x)) * get(paste0("no_shock_draw_", x))) + get(paste0("shock_mort_draw_", x))})]

# re-compute mortality proportions from these new counts: the etio-specific count divided by the total death count (with shock)
etio[, paste0("etio_prop_squeeze_draw_",0:(n_draws-1)) := lapply(0:(n_draws-1), function(x) {
       get(paste0("etio_count_draw_", x)) / get(paste0("total_mort_draw_", x))})]

print("mortality squeeze complete!")

# Fix formatting
etio <- etio[,c(paste0("etio_prop_squeeze_draw_",0:(n_draws-1)), merge_cols, "measure_id", "pathogen", "cause_id"), with = FALSE]
setnames(etio, paste0("etio_prop_squeeze_draw_",0:(n_draws-1)), paste0("draw_",0:(n_draws-1)))

# Write them out. Only replace with scaled for the YLDs for meningitis (keep YLLs & LRI)
lapply(unique(info$pathogen), function(p){
  # replace YLD meningitis w new rescaled
  etio_new <- etio[pathogen == p]
  # rename viral
  if(p == "virus") p <- paste0(p, "_", men_cause_id)
  # read result for each etiology created in rescale_meningitis_ylds
  etio <- fread(file.path(eti_dir, "scaled", p, paste0(location, ".csv")))
  # keep YLDs & LRI
  etio <- etio[cause_id != men_cause_id | measure_id == 3]
  # looping through all pathogens
  if(nrow(etio_new) > 0) {
    etio <- rbind(etio, etio_new, fill = TRUE)
    # new out directory
    out_dir <- file.path(eti_dir, "add_shock", p)
    dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
    fwrite(etio, file.path(out_dir, paste0(location, ".csv")))
  } else return()
})


print("results written out.")
