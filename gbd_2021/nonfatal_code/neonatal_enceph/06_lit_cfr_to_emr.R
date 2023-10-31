## ******************************************************************************
##
## Purpose: -Transforms raw CFR lit data from Bundle 337 to EMR
##          -Saves as new rows in the bundle with measure "mtexcess"
## Input:   bundle ID
## Output:  
## Created: 2017-12-29
## Last updated: 2020-06-29
##
## ******************************************************************************


rm(list=ls())

me <- 25306
acause <- 'neonatal_enceph'
bun_id <- 337

os <- .Platform$OS.type
if (os=="windows") {
        j<- "PATHNAME"
        h <-"PATHNAME"
        my_libs <- "PATHNAME"
} else {
        j<- "PATHNAME"
        h<-"PATHNAME"
        my_libs <- "PATHNAME"
}

pacman::p_load(data.table, openxlsx, ggplot2, plotly)

source("PATHNAME/get_bundle_data.R")
source("PATHNAME/get_bundle_version.R")
source("PATHNAME/upload_bundle_data.R")

key <- fread("PATHNAME/cfr_to_emr_key.csv")

# acause <- key[modelable_entity_id == me, acause]
# cfr_bundle <- key[modelable_entity_id == me, cfr_bundle]
# emr_bundle <- key[modelable_entity_id == me, emr_bundle]
# 
# emr_data <- get_bundle_data(bundle_id <- emr_bundle, decomp_step = 'iterative', gbd_round_id = 7)
# emr_data <- emr_data[measure == 'mtexcess']

cfr_data <- get_bundle_version(bundle_version_id = 28721, fetch = 'all')
#only update the newly added Nigeria data
cfr_data <- cfr_data[seq > 130]

cfr_data <- cfr_data[!(mean == 1 | sample_size == cases), ]

cfr_data <- cfr_data[age_start < 28/365 & age_end <= 28/365]
cfr_data[age_end > 7/365 & age_end < 10/365, age_end := 7/365]

#flag the lit data that has both ENN and LNN data because it can be used to estimate the age ratio
cfr_data[, full_age_pattern := 0]
cfr_data[nid %in% c(127936,221564), full_age_pattern := 1]

cfr_data[ , mean := -log(1-mean)/(age_end - age_start)
        ][, measure := "mtexcess"
        ][, lower := ""
        ][, upper := ""
        ][, uncertainty_type_value := ""
        ][, standard_error := "",
        ][, note_modeler := "Transformed from raw cfr data by -ln(1-mean)/(age_end - age_start)"
        ][, bundle_id := bun_id,
        ][, seq := NA
        ][, underlying_nid := ""
        ][, sampling_type := ""
        ][, recall_type_value := ""
        ][, design_effect := ""
        ][, response_rate := "",
        ][, modelable_entity_id := me]

filename <- paste0("emr_transformed_from_cfr_", Sys.Date(), ".xlsx")
filepath <- paste0("PATHNAME", filename)

write.xlsx(cfr_data, filepath, row.names = F, na = "", sheetName = "extraction")

upload_bundle_data(bun_id, filepath = filepath, decomp_step = 'iterative', gbd_round_id = 7)

bv <- save_bundle_version(bundle_id = bun_id, decomp_step = 'iterative', gbd_round_id = 7)
