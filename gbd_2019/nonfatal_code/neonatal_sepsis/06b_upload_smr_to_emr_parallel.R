
print(paste("Working dir:", getwd()))

args <- commandArgs(trailingOnly = TRUE)
acause <- args[1]
bundle_id <- args[2]
me_id <- args[3]

j <- "FILEPATH"
my_libs <- "FILEPATH"

library(data.table)
library(magrittr)
library(readxl, lib.loc = my_libs)
library(openxlsx, lib.loc = my_libs)

print(paste(acause, bundle_id, me_id))

source(paste0(
    j, "FILEPATH/upload_bundle_data.R"  ))

source(paste0(
  j, "FILEPATH/get_bundle_data.R"  ))

data_dir <- paste0("FILEPATH")

orig_filename <- paste0(me_id, "_mtexcess_2019.xlsx")
orig_filepath <- paste0(data_dir, orig_filename)
 
new_data <- as.data.table(read_excel(orig_filepath))
new_data[, bundle_id := bundle_id]

prev_seqs_to_delete <- get_bundle_data(bundle_id, decomp_step = "step4")
prev_seqs_to_delete <- prev_seqs_to_delete[measure == "mtexcess", list(seq)]

combined <- rbindlist(list(prev_seqs_to_delete, new_data), use.names = T, fill = T)
combined_filename <- paste0(me_id, "_mtexcess_for_upload_2019_step4.xlsx")
combined_filepath <- paste0(data_dir, combined_filename)

write.xlsx(combined, combined_filepath, sheetName = "extraction", showNA = F, row.names=F )

upload_bundle_data(bundle_id, decomp_step = "step4", combined_filepath)

