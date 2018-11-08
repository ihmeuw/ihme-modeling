# --------------

args <- commandArgs(trailingOnly = TRUE)
me <- args[1]

os <- .Platform$OS.type
if (os=="windows") {
  j<- FILEPATH
  h <-FILEPATH
  my_libs <- paste0(h, "/local_packages")
} else {
  j<- FILEPATH
  h<-FILEPATH
  my_libs <- paste0(h, "/cluster_packages")
}

library(data.table)
library(magrittr)
library(openxlsx, lib.loc = my_libs)

source(FILEPATH)

source(FILEPATH)

key <- fread(FILEPATH)

acause <- key[modelable_entity_id == me, acause]
cfr_bundle <- key[modelable_entity_id == me, cfr_bundle]
emr_bundle <- key[modelable_entity_id == me, emr_bundle]

cfr_data <- get_epi_data(cfr_bundle)

emr_data <- get_epi_data(emr_bundle)
emr_data <- emr_data[measure == "mtexcess", ]

print(paste("EMR data points:", emr_data[, .N]))
print(paste("CFR data points:", cfr_data[, .N]))

cfr_data <- cfr_data[!(mean == 1 | sample_size == cases), ]


cfr_data[age_end == 0, age_end := 28/365]
cfr_data[age_end == 28/365.25, age_end := 28/365]


cfr_data[ , mean := -log(1-mean)/(age_end - age_start)
        ][, measure := "mtexcess"
        ][, lower := ""
        ][, upper := ""
        ][, uncertainty_type_value := ""
        ][, standard_error := "",
        ][, note_modeler := "Transformed from raw cfr data by -ln(1-mean)/(age_end - age_start)"
        ][, bundle_id := emr_bundle,
        ][, seq := ""
        ][, underlying_nid := ""
        ][, sampling_type := ""
        ][, recall_type_value := ""
        ][, design_effect := ""
        ][, response_rate := "",
        ][, modelable_entity_id := me]


combined <- rbindlist(list(emr_data[, list(seq)], cfr_data), fill = T, use.names = T)

filename <- paste0("emr_transformed_from_cfr_", Sys.Date(), ".xlsx")
filepath <- FILEPATH

write.xlsx(combined, filepath, row.names = F, na = "", sheetName = "extraction")

upload_epi_data(emr_bundle, filepath)