# Replace completeness estimates in the no shock

rm(list=ls())
library(data.table)
library(argparse)

parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version id for this run of DDM')
args <- parser$parse_args()

version_id <- args$version_id

# Replace Adult DDM completeness estimates with the with shock results

all_final_comp <- readstata13::read.dta13(paste0(
  "FILEPATH"
))
setDT(all_final_comp)
# Drop adult estimates
ind_locs <- unique(all_final_comp[grepl("IND", ihme_loc_id), ihme_loc_id])
all_final_comp[
  !(ihme_loc_id %in% ind_locs & source == "SRS"),
  c("final_comp", "sd", "adjust") := NA_real_
]

# Read in DDM estimates that used w/ shock VRP
wshock_ddm <- haven::read_dta(paste0("FILEPATH"))
wshock_ddm <- as.data.table(wshock_ddm)

wshock_ddm <- wshock_ddm[
  ,
  .(source, ihme_loc_id, year, sex, ws_final_comp = final_comp, ws_sd = sd, ws_adjust = adjust)
]

# replace adult completeness
all_final_comp <- merge(
  all_final_comp,
  wshock_ddm,
  by = c("source", "ihme_loc_id", "year", "sex")
)

all_final_comp[is.na(final_comp), final_comp := ws_final_comp]
all_final_comp[is.na(sd), sd := ws_sd]
all_final_comp[is.na(adjust), adjust := ws_adjust]

all_final_comp[, c("ws_final_comp", "ws_sd", "ws_adjust") := NULL]

haven::write_dta(all_final_comp, paste0("FILEPATH"))
