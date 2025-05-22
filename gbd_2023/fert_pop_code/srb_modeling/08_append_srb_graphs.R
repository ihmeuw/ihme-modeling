################################################################################
## DESCRIPTION Append graphs into singular pdfs (non-standardized and fixed)
## INPUTS pdf graphs for individual locations
## OUTPUTS appended graphs (non-standardized y-axis and fixed)
## Steps
## - Read in configuration files
## - Organize locations per location_set_id 35
## - Develop function to group pdfs (normal and fixed)
##   - Append pdfs
## - Run functions
################################################################################

rm(list = ls())

library(argparse)
library(data.table)
library(mortdb)
library(stringr)
library(qpdf)

## A. Read in configuration files ----------------------------------------------

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--main_dir", 
  type = "character", 
  required = !interactive(),
  help = "base directory for this version run"
)
args <- parser$parse_args()
list2env(args, .GlobalEnv)

# initial inputs
if(interactive()){
  version_id <- "Run id"
  main_dir <- "FILEPATH"
  if (!fs::dir_exists(main_dir)) stop("specify valid 'main_dir'")
}
config <- config::get(
  file = fs::path(main_dir, "/srb_detailed.yml"),
  use_parent = FALSE
)
list2env(config, .GlobalEnv)

missing_locs <- function(nat_id){
  ihme_locs_nat <- ihme_locs[
    grepl(nat_id, ihme_locs) & !ihme_locs %in% loc_metadata$ihme_loc_id
  ]
  return(rbind(
      loc_metadata[grepl(nat_id, ihme_loc_id)], 
      data.table(ihme_loc_id = ihme_locs_nat),
      fill = TRUE
  ) |> _[, location_id := tstrsplit(ihme_loc_id, "_", keep = 2)])
}
fill_missing_locs <- function(dt){
  setorder(dt, location_id)
  dt[, sort_order := nafill(sort_order, type = "locf")]
}

ihme_locs <- fread(fs::path(output_dir, "graphing_data.csv")) |> 
  _[, unique(ihme_loc_id)]

# B. Organize locations per location_set_id 35
Sys.unsetenv("PYTHONPATH")
source("FILEPATH")
loc_metadata <- get_location_metadata(
  location_set_id = 35, release_id = release_id
) |> _[, .(ihme_loc_id, sort_order)] 

list_locs <- setdiff(ihme_locs[!is.na(ihme_locs)], loc_metadata$ihme_loc_id)
missing_nats <- gsub("_\\d+", "", list_locs) |> unique()

adjust_order <- purrr::map(as.list(missing_nats), missing_locs) |> 
  setattr("names", tolower(missing_nats))

adjust_order$chn[
  ihme_loc_id == "CHN_44533",
  sort_order := loc_metadata[ihme_loc_id == "CHN", sort_order]
]

# other locations
adjust_order <- purrr::map(adjust_order, fill_missing_locs) |> rbindlist()

loc_metadata <- unique(rbind(loc_metadata, adjust_order[, - c("location_id")]))

if(any(!ihme_locs[!is.na(ihme_locs)] %in% loc_metadata$ihme_loc_id)){
  list_locs <- setdiff(ihme_locs, loc_metadata$ihme_loc_id)
  stop(print(paste(list_locs, " ")))
}

setorder(loc_metadata, sort_order)

# C. Group pdf files and append pdfs --------------------------------------

pdf_group <- purrr::map(loc_metadata$ihme_loc_id, \(i_loc){
  return(list(
    free = fs::path(graphs_dir, paste0(i_loc, version_id, ".pdf")),
    fixed = fs::path(graphs_dir, paste0(i_loc, version_id, "_fixed.pdf"))
  ))
}) |> purrr::list_transpose()

append_names <- sapply(
  c("", "_fixed"), \(x) 
  fs::path(graphs_dir, paste0(version_id, "_appended_graphs", x, ".pdf"))
) |> setattr("names", c("free", "fixed"))

# location specific with fixed y-axis compiled
qpdf::pdf_combine(pdf_group[["fixed"]], appended_graphs[["fixed"]])

# location specific compiled
qpdf::pdf_combine(pdf_group[["free"]], appended_graphs[["free"]])
