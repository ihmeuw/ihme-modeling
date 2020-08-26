##########################################################################################
### Project: Neonatal Sepsis and Other Neonatal Infections
### Purpose: Convert CFR to EMR 
###########################################################################################

prep_emr <- function(bundle, decomp_step){
  
  #pull bundle ids
  cfr_bundle <- fread("FILEPATH/cfr_to_emr_key.csv")[acause==bundle]$cfr_bundle
  emr_bundle <- fread("FILEPATH/cfr_to_emr_key.csv")[acause==bundle]$emr_bundle
  
  #read in data
  cfr_data <- get_bundle_data(cfr_bundle,decomp_step)
  emr_data <- get_bundle_data(emr_bundle,decomp_step)[measure=="mtexcess",]
  
  print(paste("EMR data points:", emr_data[, .N]))
  print(paste("CFR data points:", cfr_data[, .N]))
  
  #format CFR data to EMR
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
  
  # Sex-split converted data
  cfr_data <- rbind(cfr_data,cfr_data)
  cfr_data$effective_sample_size <- NULL
  cfr_data$sample_size <- cfr_data$sample_size/2
  cfr_data$cases <- ""
  
  # Write out processed data
  combined <- rbindlist(list(emr_data[, list(seq)], cfr_data), fill = T, use.names = T)
  
  filename <- paste0("emr_transformed_from_cfr_", Sys.Date(), ".xlsx")
  filepath <- paste0("FILEPATH", filename)
  
  write.xlsx(combined, filepath, row.names = F, na = "", sheetName = "extraction")
  
  upload_bundle_data(emr_bundle, filepath)
  print(paste0("Success! EMR data successfully uploaded for ",bundle,"!"))
}